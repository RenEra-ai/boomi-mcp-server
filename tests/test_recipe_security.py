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

from collections import abc
from typing import Dict, List, Literal, Tuple

import json
import logging
import pickle
import sys
from pathlib import Path

import pytest
from pydantic import ConfigDict, ValidationError, field_serializer, field_validator, model_validator

_project_root = Path(__file__).resolve().parent.parent
_src = str(_project_root / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)
sys.path.insert(0, str(_project_root / "tests" / "patterns"))
sys.path.insert(0, str(_project_root / "tests"))

from boomi_mcp.categories.integration_authoring import (
    build_from_archetype_action,
    compose_archetypes_action,
    list_integration_archetypes_action,
)
from boomi_mcp.categories.meta_tools import (
    get_schema_template_action,
    plan_integration_design_action,
)
from boomi_mcp.errors import (
    RECIPE_CONSTRAINT_FAILED,
    RECIPE_CONTRIBUTION_INVALID,
    RECIPE_INPUT_INVALID,
)
from boomi_mcp.models.integration_models import IntegrationSpecV1
from boomi_mcp.models.recipe_contributions import (
    ProcessIRPatchV1,
    RecipeComponentKey,
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
    RecipeInputBase,
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
        # Field values are copied by REFERENCE, not through ``model_dump()``. A
        # first version dumped, which turned the nested ``SyncComponentSlotV1``
        # models into plain dicts — so the SUB-TREE check caught this case
        # incidentally and the root check was left unpinned. Copying the values
        # keeps every declared type correct, leaving the root check as the only
        # thing that can reject it.
        return _OpenSubclassInputV1.model_construct(
            **{name: getattr(self, name) for name in type(self).model_fields},
            smuggled="SENTINEL-SQL-SELECT-SECRETS",
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


# ---------------------------------------------------------------------------
# ...and the same trick one level DOWN
#
# A nested model's ``after``, or a ``field_validator``, replaces the value at its
# own position while the registered outer type survives — so a root-only check
# passes and a DECLARED field holds the caller's mapping. Unlike the
# attribute-only channels, this one shows up in ``model_dump()`` and in an
# ordinary ``inp.field`` read (issue #145, Codex review).
# ---------------------------------------------------------------------------


class _NestedSlot(RecipeInputBase):
    """Nested model whose ``after`` hands back the stash instead of itself."""

    label: str = "x"

    @model_validator(mode="after")
    def _return_the_stash(self):
        return dict(_SMUGGLED_RAW) or {"label": self.label}


class _HonestSlot(RecipeInputBase):
    label: str = "x"


def _stash_before(cls, data):
    if isinstance(data, dict):
        _SMUGGLED_RAW.clear()
        _SMUGGLED_RAW.update(data)
        return {k: v for k, v in data.items() if k in cls.model_fields}
    return data


class _NestedBypassInputV1(RecipeInputBase):
    """The nested-model form: the declared field ends up holding a raw mapping."""

    slot: _NestedSlot = _NestedSlot()

    _stash = model_validator(mode="before")(classmethod(_stash_before))


class _NestedTupleBypassInputV1(RecipeInputBase):
    """The sequence form. Production input models declare ``Tuple[Model, ...]``,
    so an element-level replacement is the shape that actually matters here."""

    slots: Tuple[_NestedSlot, ...] = ()

    _stash = model_validator(mode="before")(classmethod(_stash_before))


class _FieldSwapInputV1(RecipeInputBase):
    """The field-validator form: a ``str`` field ends up holding a mapping."""

    label: str = "x"

    _stash = model_validator(mode="before")(classmethod(_stash_before))

    @field_validator("label", mode="after")
    @classmethod
    def _swap(cls, value):
        return dict(_SMUGGLED_RAW) or value


class _HonestNestedInputV1(RecipeInputBase):
    """Firing control: the same shape, honestly implemented."""

    slot: _HonestSlot = _HonestSlot()
    slots: Tuple[_HonestSlot, ...] = ()


_NESTED_CASES = [
    (_NestedBypassInputV1, {"slot": {"label": "y"}}),
    (_NestedTupleBypassInputV1, {"slots": [{"label": "y"}]}),
    (_FieldSwapInputV1, {"label": "y"}),
]


@pytest.mark.parametrize(
    "model,payload",
    _NESTED_CASES,
    ids=["nested_model", "tuple_element", "field_validator"],
)
def test_a_replacement_below_the_root_is_rejected(model, payload):
    _SMUGGLED_RAW.clear()
    _EXECUTOR_CALLS.clear()
    registry = _registry_with_input(model, executor=_recording_executor)

    raw = {**payload, "smuggled": "SENTINEL-SQL-SELECT-SECRETS"}
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


@pytest.mark.parametrize(
    "model,payload",
    _NESTED_CASES,
    ids=["nested_model", "tuple_element", "field_validator"],
)
def test_the_below_root_cases_defeat_a_root_only_check(model, payload):
    """Anti-vacuity control.

    Each case must produce the exact registered outer type — otherwise it is
    caught by the root check and proves nothing about the sub-tree — AND must
    land the caller's key on a surface an ordinary executor reads.
    """
    _SMUGGLED_RAW.clear()
    raw = {**payload, "smuggled": "SENTINEL-SQL-SELECT-SECRETS"}
    validated = model.model_validate(raw)
    assert type(validated) is model, "root check would have caught this"

    import warnings as _warnings

    with _warnings.catch_warnings():
        _warnings.simplefilter("ignore")
        dumped = json.dumps(validated.model_dump(), default=str)
    assert "SENTINEL-SQL-SELECT-SECRETS" in dumped


def test_an_honest_nested_model_is_not_refused():
    """Firing control for the sub-tree check."""
    _SMUGGLED_RAW.clear()
    raw = {"slot": {"label": "y"}, "slots": [{"label": "z"}]}
    validated = _HonestNestedInputV1.model_validate(raw)
    assert type(validated) is _HonestNestedInputV1
    validated.model_dump(warnings="error")  # must not raise

    registry = _registry_with_input(_HonestNestedInputV1)
    try:
        run_recipes(
            [RecipeRequestV1(recipe_id="test.recipe", invocation_id="i1", raw_input=raw)],
            catalog=MaterializationCatalog({}),
            registry=registry,
        )
    except RecipeError as exc:
        assert exc.diagnostics[0].code != RECIPE_INPUT_INVALID


class _MaskedSwapInputV1(RecipeInputBase):
    """A ``field_serializer`` that hides a swapped mapping from the sweep."""

    label: str = "x"

    _stash = model_validator(mode="before")(classmethod(_stash_before))

    @field_validator("label", mode="after")
    @classmethod
    def _swap(cls, value):
        return dict(_SMUGGLED_RAW) or value

    @field_serializer("label")
    def _ser(self, value):
        return "benign"


class _ConstraintLieInputV1(RecipeInputBase):
    """Declares ``Literal["a"]`` and stores something else — same runtime type."""

    label: Literal["a"] = "a"

    _stash = model_validator(mode="before")(classmethod(_stash_before))

    @field_validator("label", mode="after")
    @classmethod
    def _swap(cls, value):
        return json.dumps(_SMUGGLED_RAW) if _SMUGGLED_RAW else value


class _HonestlyPermissiveInputV1(RecipeInputBase):
    """The CONTROL. Declares a plain ``str`` and stores caller JSON in it.

    Nothing is bypassed here — the value matches its annotation exactly. No gate
    can refuse this, and none should: a model author is entitled to declare a
    permissive field. It exists to measure what the constraint lie above actually
    gains over a truthful declaration.
    """

    label: str = "x"

    _stash = model_validator(mode="before")(classmethod(_stash_before))

    @field_validator("label", mode="after")
    @classmethod
    def _swap(cls, value):
        return json.dumps(_SMUGGLED_RAW) if _SMUGGLED_RAW else value


def _leak_surfaces(model):
    """Where does the caller's key end up, for one model?"""
    _SMUGGLED_RAW.clear()
    raw = {"label": "a", "smuggled": "SENTINEL-SQL-SELECT-SECRETS"}
    validated = model.model_validate(raw)
    try:
        validated.model_dump(warnings="error")
        caught = False
    except Exception:
        caught = True
    import warnings as _warnings

    with _warnings.catch_warnings():
        _warnings.simplefilter("ignore")
        dumped = json.dumps(validated.model_dump(), default=str)
    return {
        "caught": caught,
        "attribute": "SENTINEL-SQL-SELECT-SECRETS" in str(validated.label),
        "model_dump": "SENTINEL-SQL-SELECT-SECRETS" in dumped,
    }


def test_a_masking_serializer_no_longer_helps():
    """The serializer BAN was removed, and this is why that is safe.

    A masking ``field_serializer`` still suppresses ``model_dump()`` — but the
    engine no longer asks the model's serializer anything. It validates each
    stored value with an adapter it built itself, so what the author's serializer
    would have said is irrelevant.

    Removing the ban also removed six false rejections. ``SecretStr``,
    ``AnyUrl``, ``IPv4Address``, ``IPv4Network``, ``re.Pattern``, ``deque`` and
    ``Path`` all carry built-in ser schemas and were refused by it — and
    ``SecretStr`` is the type an author *should* reach for, so refusing it pushed
    them toward plain ``str`` (issue #145, live QA).
    """
    _SMUGGLED_RAW.clear()
    _EXECUTOR_CALLS.clear()
    registry = _registry_with_input(_MaskedSwapInputV1, executor=_recording_executor)

    # It registers now — the ban is gone.
    raw = {"label": "a", "smuggled": "SENTINEL-SQL-SELECT-SECRETS"}
    with pytest.raises(RecipeError) as exc:
        run_recipes(
            [RecipeRequestV1(recipe_id="test.recipe", invocation_id="i1", raw_input=raw)],
            catalog=MaterializationCatalog({}),
            registry=registry,
        )
    assert exc.value.diagnostics[0].code == RECIPE_INPUT_INVALID
    assert _EXECUTOR_CALLS == []

    # ...and the masking is real, which is what makes the catch meaningful.
    surfaces = _leak_surfaces(_MaskedSwapInputV1)
    assert surfaces["attribute"] is True
    assert surfaces["model_dump"] is False


@pytest.mark.parametrize("name", ["SecretStr", "Path"])
def test_the_types_the_serializer_ban_refused_now_register(name):
    """Removing the ban restored these two.

    ``SecretStr`` is the one that mattered: it is the type an author *should*
    reach for on a sensitive input, so refusing it pushed them toward plain
    ``str`` — a security-negative outcome from a security check.

    SEPARATE, MEASURED, AND STILL OPEN: ``AnyUrl``, ``IPv4Address``,
    ``IPv4Network``, ``re.Pattern`` and ``deque`` are still refused — by the
    wrap/plain VALIDATOR ban, not this one, since each compiles to a
    ``function-plain``/``function-wrap`` validator node. That over-fire predates
    this change and is not fixed by it (issue #145, live QA).
    """
    from pathlib import PurePosixPath

    from pydantic import SecretStr

    annotation, default = {
        "SecretStr": (SecretStr, SecretStr("s")),
        "Path": (PurePosixPath, PurePosixPath("/tmp")),
    }[name]
    model = type(
        f"Honest{name}InputV1",
        (RecipeInputBase,),
        {
            "model_config": ConfigDict(extra="forbid", frozen=True),
            "__annotations__": {"field": annotation},
            "field": default,
        },
    )
    _registry_with_input(model)  # must not raise


@pytest.mark.parametrize("name", ["AnyUrl", "IPv4Address", "IPv4Network", "Pattern"])
def test_the_wrap_ban_still_over_fires_on_these(name):
    """Records a defect rather than hiding it.

    These are honest stdlib/pydantic types that a recipe input might reasonably
    declare, and the wrap/plain validator ban refuses all four because they
    compile to ``function-plain``/``function-wrap`` VALIDATOR nodes. Asserting the
    current behaviour keeps the cost visible and makes this test fail — loudly,
    and in the right place — the moment the ban is narrowed or removed.

    ``deque`` was listed here once and does NOT belong: it is refused by the
    CLOSEDNESS gate, a different check, so crediting it to the wrap ban would have
    left this test passing by accident after the ban was removed.
    """
    import ipaddress
    import re
    from typing import Optional

    from pydantic import AnyUrl

    annotation, default = {
        "AnyUrl": (Optional[AnyUrl], None),
        "IPv4Address": (ipaddress.IPv4Address, ipaddress.IPv4Address("1.2.3.4")),
        "IPv4Network": (ipaddress.IPv4Network, ipaddress.IPv4Network("10.0.0.0/8")),
        "Pattern": (Optional[re.Pattern], None),
    }[name]
    model = type(
        f"Refused{name}InputV1",
        (RecipeInputBase,),
        {
            "model_config": ConfigDict(extra="forbid", frozen=True),
            "__annotations__": {"field": annotation},
            "field": default,
        },
    )
    with pytest.raises(ValueError, match="validator"):
        _registry_with_input(model)


class _DeepSlot(RecipeInputBase):
    """A VALID slot instance whose own field was swapped.

    The outer ``Tuple[_DeepSlot, ...]`` annotation cannot catch this — every
    element really is a ``_DeepSlot``. Only walking into the elements does.
    """

    label: str = "x"

    @field_validator("label", mode="after")
    @classmethod
    def _swap(cls, value):
        return dict(_SMUGGLED_RAW) or value


class _DeepTupleInputV1(RecipeInputBase):
    slots: Tuple[_DeepSlot, ...] = ()

    _stash = model_validator(mode="before")(classmethod(_stash_before))


def test_a_swap_inside_a_valid_sequence_element_is_caught():
    """Pins the element walk specifically.

    Removing it leaves every other test green — the tuple case elsewhere is
    caught one level up, by the element's own type. This is the shape that needs
    the walk, and it is the production shape: ``component_slots`` is a tuple of
    models (issue #145).
    """
    _SMUGGLED_RAW.clear()
    _EXECUTOR_CALLS.clear()
    registry = _registry_with_input(_DeepTupleInputV1, executor=_recording_executor)

    raw = {"slots": [{"label": "y"}], "smuggled": "SENTINEL-SQL-SELECT-SECRETS"}

    # Anti-vacuity: the elements really are valid instances of the declared type,
    # so nothing above the element level can object.
    validated = _DeepTupleInputV1.model_validate(dict(raw))
    assert all(type(s) is _DeepSlot for s in validated.slots)
    assert "SENTINEL-SQL-SELECT-SECRETS" in str(validated.slots[0].label)

    with pytest.raises(RecipeError) as exc:
        run_recipes(
            [RecipeRequestV1(recipe_id="test.recipe", invocation_id="i1", raw_input=raw)],
            catalog=MaterializationCatalog({}),
            registry=registry,
        )
    assert exc.value.diagnostics[0].code == RECIPE_INPUT_INVALID
    assert _EXECUTOR_CALLS == []


class _DeepDictInputV1(RecipeInputBase):
    """A model behind a MAPPING value. The mapping branch had no fixture at all,
    so deleting it whole changed nothing (issue #145, live QA)."""

    slots: Dict[str, _DeepSlot] = {}

    _stash = model_validator(mode="before")(classmethod(_stash_before))


class _NestedContainerInputV1(RecipeInputBase):
    """A model at container depth TWO — the shape the one-level walk missed."""

    slots: Tuple[Tuple[_DeepSlot, ...], ...] = ()

    _stash = model_validator(mode="before")(classmethod(_stash_before))


class _DictOfListInputV1(RecipeInputBase):
    """``Dict[str, List[Model]]`` — an ordinary shape, and it walked straight
    through the one-level version."""

    slots: Dict[str, List[_DeepSlot]] = {}

    _stash = model_validator(mode="before")(classmethod(_stash_before))


_DEEP_CASES = [
    (_DeepDictInputV1, {"slots": {"a": {"label": "y"}}}),
    (_NestedContainerInputV1, {"slots": [[{"label": "y"}]]}),
    (_DictOfListInputV1, {"slots": {"a": [{"label": "y"}]}}),
]


@pytest.mark.parametrize(
    "model,payload",
    _DEEP_CASES,
    ids=["dict_value", "tuple_of_tuple", "dict_of_list"],
)
def test_a_swap_below_one_container_level_is_caught(model, payload):
    """The elements of a container are not the leaves of the walk.

    Each element here is a genuinely valid model, so nothing above it can object —
    only recursing through the container into the element, and then into the
    element's own fields, sees the swap.
    """
    _SMUGGLED_RAW.clear()
    _EXECUTOR_CALLS.clear()
    registry = _registry_with_input(model, executor=_recording_executor)
    raw = {**payload, "smuggled": "SENTINEL-SQL-SELECT-SECRETS"}

    # Anti-vacuity: the swap really happened, and really is behind valid models.
    validated = model.model_validate(dict(raw))
    assert "SENTINEL-SQL-SELECT-SECRETS" in str(validated.model_dump())

    with pytest.raises(RecipeError) as exc:
        run_recipes(
            [RecipeRequestV1(recipe_id="test.recipe", invocation_id="i1", raw_input=raw)],
            catalog=MaterializationCatalog({}),
            registry=registry,
        )
    assert exc.value.diagnostics[0].code == RECIPE_INPUT_INVALID
    assert _EXECUTOR_CALLS == []


class _HashableSlot(RecipeInputBase):
    """Usable as a mapping KEY, and lies with a value that stays hashable.

    A dict swap is unreachable here — it would make the frozen model unhashable —
    so the lie is a tuple in a ``str`` field, which is both a type lie and
    hashable (issue #145, live QA).
    """

    label: str = "x"

    @field_validator("label", mode="after")
    @classmethod
    def _swap(cls, value):
        return tuple(_SMUGGLED_RAW.items()) or value


class _ModelKeyInputV1(RecipeInputBase):
    slots: Dict[_HashableSlot, str] = {}

    _stash = model_validator(mode="before")(classmethod(_stash_before))


def test_a_swap_in_a_mapping_KEY_is_caught():
    """Mapping keys are walked, and this is the only thing that says so.

    The key branch was added from a report without a fixture — the third time a
    branch in this walk arrived untested — so deleting it changed nothing.

    Built with ``model_construct`` rather than through ``run_recipes``: a raw
    payload cannot express a model-keyed mapping at all, because the key would
    have to be an unhashable dict. That is also why this shape is exotic — but
    the branch exists, so it is pinned.
    """
    from boomi_mcp.recipes.engine import _assert_declared_shape

    _SMUGGLED_RAW.clear()
    _SMUGGLED_RAW.update({"smuggled": "SENTINEL-SQL-SELECT-SECRETS"})

    honest_key = _HashableSlot.model_construct(label="y")
    lying_key = _HashableSlot.model_validate({"label": "y"})

    # Anti-vacuity: the key is a valid model, and the lie is on the KEY only.
    assert type(lying_key) is _HashableSlot
    assert "SENTINEL-SQL-SELECT-SECRETS" in str(lying_key.label)

    # Control: the honest key passes.
    _assert_declared_shape(_ModelKeyInputV1.model_construct(slots={honest_key: "v"}))

    with pytest.raises(Exception):
        _assert_declared_shape(_ModelKeyInputV1.model_construct(slots={lying_key: "v"}))


def test_a_generator_in_a_sequence_field_is_refused_and_not_consumed():
    """Strict mode, and why it is strict.

    Lax coercion turned a stored generator into a tuple, reported no mismatch,
    and left the field holding an exhausted generator — a miss and a destructive
    side effect at once. The second assertion is the one that matters: the check
    must not damage what it inspects.
    """
    from boomi_mcp.recipes.engine import _assert_declared_shape

    def _gen():
        yield from ("a", "b", "c")

    class SeqInputV1(RecipeInputBase):
        seq: Tuple[str, ...] = ()

    instance = SeqInputV1.model_construct(seq=_gen())
    with pytest.raises(Exception):
        _assert_declared_shape(instance)
    assert list(instance.seq) == ["a", "b", "c"], "the check consumed the value"


@pytest.mark.parametrize(
    "annotation,stored",
    [
        (int, "5"),
        (Tuple[str, ...], ["a"]),
        (bool, "true"),
    ],
    # ``float`` from ``int`` is deliberately absent: strict mode permits that
    # widening, and it carries no payload capacity.
    ids=["int_from_str", "tuple_from_list", "bool_from_str"],
)
def test_a_coercible_type_lie_is_still_a_lie(annotation, stored):
    """After validation a stored value should already BE its declared type.

    Lax mode re-permits the coercion pydantic already did at ``model_validate``
    time, which is exactly how these passed before.
    """
    from boomi_mcp.recipes.engine import _assert_declared_shape

    model = type(
        "CoercibleInputV1",
        (RecipeInputBase,),
        {
            "model_config": ConfigDict(extra="forbid", frozen=True),
            "__annotations__": {"field": annotation},
            "field": stored,
        },
    )
    with pytest.raises(Exception):
        _assert_declared_shape(model.model_construct(field=stored))


def test_a_swap_behind_a_pydantic_dataclass_is_caught():
    """A dataclass is neither a ``BaseModel`` nor a container.

    An earlier walk had exactly two branches — model and container — so a model
    reached through a dataclass field was never entered (issue #145, live QA).
    """
    from pydantic.dataclasses import dataclass as pydantic_dataclass

    @pydantic_dataclass(frozen=True, config=ConfigDict(extra="forbid"))
    class Holder:
        slot: _DeepSlot = _DeepSlot()

    class DataclassInputV1(RecipeInputBase):
        holder: Holder = Holder()

        _stash = model_validator(mode="before")(classmethod(_stash_before))

    _SMUGGLED_RAW.clear()
    _EXECUTOR_CALLS.clear()
    registry = _registry_with_input(DataclassInputV1, executor=_recording_executor)
    raw = {"holder": {"slot": {"label": "y"}}, "smuggled": "SENTINEL-SQL-SELECT-SECRETS"}

    # Anti-vacuity: the dataclass really is in the way, and the swap really happened.
    validated = DataclassInputV1.model_validate(dict(raw))
    assert not isinstance(validated.holder, RecipeInputBase)
    assert "SENTINEL-SQL-SELECT-SECRETS" in str(validated.holder.slot.label)

    with pytest.raises(RecipeError) as exc:
        run_recipes(
            [RecipeRequestV1(recipe_id="test.recipe", invocation_id="i1", raw_input=raw)],
            catalog=MaterializationCatalog({}),
            registry=registry,
        )
    assert exc.value.diagnostics[0].code == RECIPE_INPUT_INVALID
    assert _EXECUTOR_CALLS == []


def test_a_swapped_core_schema_does_not_reopen_the_extras_gate():
    """The bypass that retired every registration-time attribute read.

    ``__pydantic_core_schema__`` is a plain writable class attribute, and
    registration runs after class creation — so assigning a closed twin's schema
    makes an ``extra="allow"`` model pass all three registration gates while its
    compiled validator goes on accepting undeclared keys. The instance it
    produces cannot lie about what it is carrying (issue #145, live QA).
    """

    class ClosedTwinInputV1(RecipeInputBase):
        model_config = ConfigDict(extra="forbid", frozen=True)
        label: str = "x"

    class OpenInputV1(RecipeInputBase):
        model_config = ConfigDict(extra="allow", frozen=True)
        label: str = "x"

    OpenInputV1.__pydantic_core_schema__ = ClosedTwinInputV1.__pydantic_core_schema__

    # The model really does still accept undeclared keys: the swap changes what the
    # gates READ, never what the compiled validator DOES.
    smuggled = OpenInputV1.model_validate({"label": "a", "smuggled": "S"})
    assert getattr(smuggled, "smuggled", None) == "S"

    # REGISTRATION NOW REFUSES IT. The schema gate rebuilds before reading, so the
    # swapped attribute is regenerated from the live annotations and the model's
    # real, open shape is what the gate sees. Before that rebuild the swap fooled
    # every registration gate and only the value walk caught it
    # (issue #145, live QA #390).
    with pytest.raises(ValueError, match="not closed"):
        _registry_with_input(OpenInputV1, executor=_recording_executor)

    # The value walk is still the backstop for a model that registers cleanly and
    # is opened afterwards, which no registration-time gate can see at all.
    class LateOpenInputV1(RecipeInputBase):
        model_config = ConfigDict(extra="forbid", frozen=True)
        label: str = "x"

    registry = _registry_with_input(LateOpenInputV1, executor=_recording_executor)
    LateOpenInputV1.model_config = ConfigDict(extra="allow", frozen=True)
    LateOpenInputV1.__pydantic_core_schema__ = OpenInputV1.__pydantic_core_schema__
    LateOpenInputV1.model_rebuild(force=True)
    OpenInputV1 = LateOpenInputV1  # the rest of this test drives the late-opened one

    # The engine refuses it anyway, at the value.
    _EXECUTOR_CALLS.clear()
    with pytest.raises(RecipeError) as exc:
        run_recipes(
            [
                RecipeRequestV1(
                    recipe_id="test.recipe",
                    invocation_id="i1",
                    raw_input={"label": "a", "smuggled": "SENTINEL-SQL-SELECT-SECRETS"},
                )
            ],
            catalog=MaterializationCatalog({}),
            registry=registry,
        )
    assert exc.value.diagnostics[0].code == RECIPE_INPUT_INVALID
    assert _EXECUTOR_CALLS == []


def test_a_swapped_serializer_does_not_hide_a_declared_field_swap():
    """The same class, aimed at the sweep instead of the extras gate."""
    from pydantic_core import SchemaSerializer, core_schema

    class SwappedSerInputV1(RecipeInputBase):
        model_config = ConfigDict(extra="forbid", frozen=True)
        label: str = "x"

        _stash = model_validator(mode="before")(classmethod(_stash_before))

        @field_validator("label", mode="after")
        @classmethod
        def _swap(cls, value):
            return dict(_SMUGGLED_RAW) or value

    SwappedSerInputV1.__pydantic_serializer__ = SchemaSerializer(
        core_schema.any_schema()
    )

    _SMUGGLED_RAW.clear()
    _EXECUTOR_CALLS.clear()
    registry = _registry_with_input(SwappedSerInputV1, executor=_recording_executor)
    with pytest.raises(RecipeError) as exc:
        run_recipes(
            [
                RecipeRequestV1(
                    recipe_id="test.recipe",
                    invocation_id="i1",
                    raw_input={"label": "a", "smuggled": "SENTINEL-SQL-SELECT-SECRETS"},
                )
            ],
            catalog=MaterializationCatalog({}),
            registry=registry,
        )
    assert exc.value.diagnostics[0].code == RECIPE_INPUT_INVALID
    assert _EXECUTOR_CALLS == []


def test_a_constraint_lie_gains_nothing_over_an_honest_permissive_field():
    """The evidence for NOT re-checking constraints, kept executable.

    A ``field_validator`` returning a different ``str`` for a ``Literal["a"]``
    field is not caught — serialization compares types, not constraints. The
    reason that is accepted rather than fixed is the control: a model that simply
    declares ``label: str`` reaches the identical surfaces, and no gate can refuse
    a truthful declaration of a permissive field.

    If these two ever diverge, the trade-off argued in §12 no longer holds and the
    decision has to be re-made rather than inherited.
    """
    lie = _leak_surfaces(_ConstraintLieInputV1)
    control = _leak_surfaces(_HonestlyPermissiveInputV1)

    assert lie == control, (lie, control)
    assert control["model_dump"] is True  # the control genuinely leaks...
    assert control["caught"] is False  # ...and is genuinely not catchable


class _RefLieInputV1(SyncRecipeInputV1):
    """Lies about a constrained value that DOES flow onward into a contribution."""

    @field_validator("process_key", mode="after")
    @classmethod
    def _lie(cls, value):
        return "  padded key  "


class _InertLieInputV1(SyncRecipeInputV1):
    """Lies about a constrained value that nothing downstream reads."""

    @field_validator("version", mode="after")
    @classmethod
    def _lie(cls, value):
        return "2"


def _sync_executor(inp):
    from boomi_mcp.recipes.builtins.sync import _contributions

    return _contributions(inp)


def _lying_registry(model):
    return build_test_registry(
        (
            RecipeRegistrationV1(
                recipe_id="test.recipe",
                recipe_version="1.0.0",
                entry_kind="executable_recipe",
                is_default=True,
                input_model=model,
                executor=_sync_executor,
                output_types=(
                    "component_contribution",
                    "constraint_requirement",
                    "process_ir_patch",
                ),
                conflict_policy=RecipeConflictPolicyV1(),
            ),
        )
    )


def test_a_constraint_lie_that_flows_onward_is_caught_by_the_contribution_layer():
    """The CONTAINMENT half of the §12 argument, kept executable.

    The input gate deliberately does not re-check constraints. It does not need
    to for any value that matters: the contribution models re-validate every
    constrained value that crosses into composition, independently of the input
    model, so the lie dies at ``_run_executor``.

    If this ever stops holding, the §12 decision loses its primary justification
    and has to be re-made — which is the point of asserting it here rather than
    describing it in prose.
    """
    from boomi_mcp.recipes.engine import _run_executor

    # Drive the contribution layer DIRECTLY. A first version used ``run_recipes``
    # with an empty ``MaterializationCatalog``, which raises
    # ``RECIPE_CONTRIBUTION_INVALID`` on its own for a missing slot — so the test
    # passed with the lie removed and proved nothing. The control below is what
    # makes this one mean something.
    registry = _lying_registry(_RefLieInputV1)
    descriptor = registry.resolve("test.recipe", "1.0.0")

    # CONTROL: the honest input passes this layer cleanly.
    honest = SyncRecipeInputV1.model_validate(_VALID_INPUT)
    _run_executor(descriptor, registry, honest)

    # The lie survives the INPUT gate...
    lying = _RefLieInputV1.model_validate(_VALID_INPUT)
    assert lying.process_key == "  padded key  "
    lying.model_dump(warnings="error")  # the sub-tree sweep does not object

    # ...and dies at the independent contribution gate.
    #
    # WHICH gate, precisely: the contribution model refuses the value at
    # CONSTRUCTION, inside the executor — not the post-return
    # ``validate_contribution_object`` sweep, which is never reached. That matters
    # for the next reader, and it matters for this test: ``_run_executor`` emits
    # ``RECIPE_CONTRIBUTION_INVALID`` from a broad ``except Exception``, so the
    # assertion below is satisfied by ANY executor crash. The direct construction
    # assertion is what makes this discriminate — it names the model and the
    # field, and no unrelated failure can satisfy it (issue #145, live QA).
    from pydantic import TypeAdapter

    assert ProcessIRPatchV1.model_fields["process_key"].annotation is not None
    key_adapter = TypeAdapter(RecipeComponentKey)
    with pytest.raises(ValidationError):
        key_adapter.validate_python("  padded key  ")
    # Isolation control: same characters, no PADDING — accepted. So the refusal
    # is the surrounding whitespace, not the space and not the content.
    assert key_adapter.validate_python("padded key") == "padded key"

    with pytest.raises(RecipeError) as exc:
        _run_executor(descriptor, registry, lying)
    assert exc.value.diagnostics[0].code == RECIPE_CONTRIBUTION_INVALID


def test_a_constraint_lie_nothing_reads_is_inert_rather_than_corrupt():
    """The other half: the one survivor changes nothing.

    ``version`` violating its ``^1$`` constraint is not caught anywhere — and
    produces a run identical to the truthful one. A survivor that alters no
    artifact is the reason "not caught" is acceptable here, so it is asserted
    rather than assumed.
    """
    # The lie is real and no gate refuses it...
    lying = _InertLieInputV1.model_validate(_VALID_INPUT)
    assert lying.version == "2", "the lie did not take effect; the test proves nothing"
    lying.model_dump(warnings="error")  # the sub-tree sweep does not object either

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
            registry=_lying_registry(_InertLieInputV1),
        )
    except RecipeError as exc:  # an empty catalog objects later; input must not
        assert exc.diagnostics[0].code != RECIPE_INPUT_INVALID

    # ...and it changes nothing the recipe produces.
    honest = SyncRecipeInputV1.model_validate(_VALID_INPUT)
    assert canonical_recipe_contributions_json(
        list(_sync_executor(lying))
    ) == canonical_recipe_contributions_json(list(_sync_executor(honest)))


class _OmissionLieInputV1(SyncRecipeInputV1):
    """Lies by OMISSION: returns ``()`` for a ``min_length=1`` tuple.

    Nothing is smuggled — this is the case contribution re-validation structurally
    cannot cover, because there is no value crossing into composition to re-check.
    """

    @field_validator("component_slots", mode="after")
    @classmethod
    def _omit(cls, value):
        return ()


def test_an_omission_lie_is_caught_by_the_declared_constraints_not_by_revalidation():
    """The second half of the §12 containment claim.

    Re-validating returned contributions cannot see a contribution that was never
    returned, so an omission needs a different gate — the declared
    ``ConstraintRequirement``s, evaluated after composition. Asserted here because
    the §12 argument named only the first mechanism, which does not reach this
    case (issue #145, Codex review).
    """
    lying = _OmissionLieInputV1.model_validate(_VALID_INPUT)
    assert lying.component_slots == ()  # the lie took effect...
    lying.model_dump(warnings="error")  # ...and the sub-tree sweep cannot see it

    # The executor happily emits FEWER contributions — nothing invalid to re-check.
    #
    # COUNTS, not a set of kinds. A set-of-kinds assertion is blind to a PARTIAL
    # omission (drop some slots, keep one), which is the more likely real defect;
    # it only discriminates here because ``_VALID_INPUT`` happens to carry exactly
    # one slot (issue #145, live QA).
    honest = SyncRecipeInputV1.model_validate(_VALID_INPUT)
    produced = _sync_executor(lying)
    expected = _sync_executor(honest)
    assert len(produced) < len(expected), (len(produced), len(expected))
    assert "component_contribution" not in {c.contribution_kind for c in produced}

    # The full funnel still refuses it, at the constraint stage.
    with pytest.raises(RecipeError) as exc:
        run_recipes(
            [
                RecipeRequestV1(
                    recipe_id="test.recipe",
                    invocation_id="i1",
                    raw_input=_VALID_INPUT,
                )
            ],
            catalog=MaterializationCatalog({}),
            registry=_lying_registry(_OmissionLieInputV1),
        )
    assert exc.value.diagnostics[0].code == RECIPE_CONSTRAINT_FAILED

    # EXPLICIT CONTROL. The honest input also fails against this empty catalog —
    # with a DIFFERENT code. That difference is the only thing making the
    # assertion above depend on the lie, and it was previously left implicit,
    # which is how the earlier version of this fixture produced a false pass.
    with pytest.raises(RecipeError) as control:
        run_recipes(
            [
                RecipeRequestV1(
                    recipe_id="test.recipe",
                    invocation_id="i1",
                    raw_input=_VALID_INPUT,
                )
            ],
            catalog=MaterializationCatalog({}),
            registry=_lying_registry(SyncRecipeInputV1),
        )
    assert control.value.diagnostics[0].code != RECIPE_CONSTRAINT_FAILED


def test_every_production_input_model_survives_the_sub_tree_check():
    """The live risk of this guard is a false rejection, not a miss."""
    validated = SyncRecipeInputV1.model_validate(_VALID_INPUT)
    validated.model_dump(warnings="error")


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


# ---------------------------------------------------------------------------
# Three ways a value can be wrong that "the adapter accepted it" does not see
# (issue #145, Codex review)
# ---------------------------------------------------------------------------


class _DeclaredKeysOnlyLeaf(RecipeInputBase):
    """Returns a dict of ONLY declared keys, so the adapter happily accepts it."""

    ok: str = "x"

    @model_validator(mode="after")
    def _to_dict(self):
        return {"ok": self.ok}


class _HasLeafInputV1(RecipeInputBase):
    leaf: _DeclaredKeysOnlyLeaf = _DeclaredKeysOnlyLeaf()


class _HasLeafTupleInputV1(RecipeInputBase):
    leaves: Tuple[_DeclaredKeysOnlyLeaf, ...] = ()


class _HasLeafDictInputV1(RecipeInputBase):
    leaves: Dict[str, _DeclaredKeysOnlyLeaf] = {}


@pytest.mark.parametrize(
    "model,payload",
    [
        (_HasLeafInputV1, {"leaf": {"ok": "y"}}),
        (_HasLeafTupleInputV1, {"leaves": [{"ok": "y"}]}),
        (_HasLeafDictInputV1, {"leaves": {"k": {"ok": "y"}}}),
    ],
    ids=["field", "inside_a_tuple", "as_a_mapping_value"],
)
def test_a_mapping_standing_in_for_a_model_is_refused(model, payload):
    """Successful conversion is not proof.

    Strict mode still admits a mapping where a model is declared, and then runs
    that model's validators — which return the dict again. The adapter therefore
    "succeeds", its result is discarded, and the stored value was never a model.
    The tuple case additionally needs the element annotation to be carried down.
    """
    from boomi_mcp.recipes.engine import _assert_declared_shape

    instance = model.model_validate(dict(payload))
    if hasattr(instance, "leaf"):
        stored = instance.leaf
    elif isinstance(instance.leaves, dict):
        stored = next(iter(instance.leaves.values()))
    else:
        stored = instance.leaves[0]
    assert isinstance(stored, dict), "fixture drifted; nothing is being smuggled"

    with pytest.raises(Exception):
        _assert_declared_shape(instance)


def test_a_dataclass_field_is_checked_against_its_declared_type():
    """The dataclass branch recursed with ``annotation=None``, so its fields were
    visited and never judged — a mapping sat in a ``str`` field, readable at
    ``inp.holder.label``."""
    from pydantic.dataclasses import dataclass as pydantic_dataclass

    from boomi_mcp.recipes.engine import _assert_declared_shape

    @pydantic_dataclass(frozen=True, config=ConfigDict(extra="forbid"))
    class Holder:
        label: str = "x"

        @field_validator("label", mode="after")
        @classmethod
        def _swap(cls, value):
            return dict(_SMUGGLED_RAW) or value

    class DataclassFieldInputV1(RecipeInputBase):
        holder: Holder = Holder()

        _stash = model_validator(mode="before")(classmethod(_stash_before))

    _SMUGGLED_RAW.clear()
    raw = {"holder": {"label": "y"}, "smuggled": "SENTINEL-SQL-SELECT-SECRETS"}
    instance = DataclassFieldInputV1.model_validate(dict(raw))

    # Anti-vacuity: the caller's key really is sitting in a declared ``str``.
    assert "SENTINEL-SQL-SELECT-SECRETS" in str(instance.holder.label)

    with pytest.raises(Exception):
        _assert_declared_shape(instance)


def test_a_lazy_iterable_is_refused_rather_than_consumed():
    """``Iterable[str]`` validates LAZILY.

    Pydantic returns a ``ValidatorIterator`` without inspecting an element, so a
    replayable custom iterable yielding caller mappings reached the executor
    untouched — and identically on both determinism runs. Consuming it to look
    would destroy it, so the gate refuses what it cannot inspect.
    """
    from typing import Iterable

    from boomi_mcp.recipes.engine import _assert_declared_shape

    class Replayable:
        def __init__(self, items):
            self._items = list(items)

        def __iter__(self):
            return iter(self._items)

    class LazyInputV1(RecipeInputBase):
        seq: Iterable[str] = ()

    payload = Replayable([{"smuggled": "SENTINEL-SQL-SELECT-SECRETS"}])
    instance = LazyInputV1.model_construct(seq=payload)

    with pytest.raises(Exception):
        _assert_declared_shape(instance)

    # And it was refused WITHOUT being drained.
    assert list(payload) == [{"smuggled": "SENTINEL-SQL-SELECT-SECRETS"}]


def test_honest_container_and_union_shapes_still_pass():
    """Firing control for the annotation carrying, which is the newest machinery
    and the one most likely to over-fire."""
    from typing import Optional, Union

    from boomi_mcp.recipes.engine import _assert_declared_shape

    class Leaf(RecipeInputBase):
        ok: str = "x"

    class HonestInputV1(RecipeInputBase):
        one: Leaf = Leaf()
        many: Tuple[Leaf, ...] = ()
        pair: Tuple[str, int] = ("a", 1)
        mapping: Dict[str, Leaf] = {}
        listed: List[Leaf] = []
        maybe: Optional[Leaf] = None
        either: Union[str, int] = "a"

    instance = HonestInputV1.model_validate(
        {
            "one": {"ok": "a"},
            "many": [{"ok": "b"}],
            "pair": ("a", 1),
            "mapping": {"k": {"ok": "c"}},
            "listed": [{"ok": "d"}],
            "maybe": {"ok": "e"},
            "either": 3,
        }
    )
    _assert_declared_shape(instance)  # must not raise


class _HasLeafKeyInputV1(RecipeInputBase):
    """A model as a mapping KEY. Three of the last four unpinned sub-branches
    have been on the key path, so it gets its own fixture (issue #145, live QA)."""

    leaves: Dict[_HashableSlot, str] = {}


def test_a_mapping_KEY_annotation_is_carried_down():
    """Pins the key annotation specifically.

    The value path is covered by ``[as_a_mapping_value]``; dropping the KEY
    annotation left every test green.
    """
    from boomi_mcp.recipes.engine import _assert_declared_shape

    _SMUGGLED_RAW.clear()

    # A tuple is hashable, so it can sit where a model key is declared — and it
    # is not a model, which only the key annotation can notice.
    instance = _HasLeafKeyInputV1.model_construct(leaves={("not", "a", "model"): "v"})
    with pytest.raises(Exception):
        _assert_declared_shape(instance)

    # Control: a real model key passes.
    honest = _HasLeafKeyInputV1.model_construct(
        leaves={_HashableSlot.model_construct(label="y"): "v"}
    )
    _assert_declared_shape(honest)


@pytest.mark.parametrize(
    "name,value",
    [
        ("range", range(3)),
        ("dict_keys", {"a": 1}.keys()),
        ("dict_items", {"a": 1}.items()),
        ("memoryview", memoryview(b"ab")),
    ],
)
def test_a_re_iterable_collection_is_walked_not_refused(name, value):
    """The refusal is aimed at what cannot be inspected, not at what is unusual.

    A first version listed five concrete container types and refused everything
    else — which closed the lazy-iterable attack and also refused ``range``,
    ``dict_keys``/``values``/``items``, ``array.array``, ``memoryview`` and any
    custom ``abc.Sequence``. An ``abc.Collection`` is sized and re-iterable, so
    walking it is repeatable and non-destructive (issue #145, live QA).
    """
    from typing import Any as AnyType

    from boomi_mcp.recipes.engine import _assert_declared_shape

    model = type(
        "ReIterableInputV1",
        (RecipeInputBase,),
        {
            "model_config": ConfigDict(extra="forbid", frozen=True),
            "__annotations__": {"field": AnyType},
            "field": None,
        },
    )
    _assert_declared_shape(model.model_construct(field=value))  # must not raise


def test_an_any_field_is_never_refused_for_being_any():
    """``typing.Any`` is a CLASS from Python 3.11.

    A blanket ``isinstance(option, type)`` therefore accepted it as a candidate
    class and ``isinstance(value, Any)`` raised ``TypeError`` — refusing every
    ``Any`` field whatever it held, including ``Dict[str, Any]``, which §12's own
    capability argument recommends as the truthful permissive declaration.
    """
    from typing import Any as AnyType, Dict as DictType, Optional as OptionalType

    from boomi_mcp.recipes.engine import _assert_declared_shape

    class AnyFieldInputV1(RecipeInputBase):
        loose: AnyType = None
        maybe: OptionalType[AnyType] = None
        bag: DictType[str, AnyType] = {}

    _assert_declared_shape(
        AnyFieldInputV1.model_validate({"loose": {"a": [1]}, "bag": {"k": {"deep": 1}}})
    )


def test_the_class_check_does_not_disagree_with_the_adapter():
    """Pydantic's ``strict=True`` accepts an ``int`` for a ``float``.

    A wider class check refused an ``int`` literal default in a ``float`` field
    while the same field passed whenever a caller supplied the value — two halves
    of one gate with different notions of "declared type", presenting as an
    intermittent failure.
    """
    from boomi_mcp.recipes.engine import _assert_declared_shape

    class RatioInputV1(RecipeInputBase):
        ratio: float = 0  # an int literal default; pydantic does not validate it

    _assert_declared_shape(RatioInputV1())  # default path
    _assert_declared_shape(RatioInputV1.model_validate({"ratio": 1}))  # supplied path


def test_a_one_shot_iterator_is_refused_without_being_drained():
    """Isolates the ``iter(v) is v`` rule.

    A re-iterable collection is walked; a ONE-SHOT iterator cannot be, because
    looking at it consumes the value the executor is about to receive. Reached
    through an ``Any`` annotation so no element annotation can catch it first —
    the refusal itself has to be what fires.
    """
    from typing import Any as AnyType

    from boomi_mcp.recipes.engine import _assert_declared_shape

    def _gen():
        yield {"smuggled": "SENTINEL-SQL-SELECT-SECRETS"}

    payload = _gen()
    model = type(
        "OneShotInputV1",
        (RecipeInputBase,),
        {
            "model_config": ConfigDict(extra="forbid", frozen=True),
            "__annotations__": {"field": AnyType},
            "field": None,
        },
    )
    with pytest.raises(Exception):
        _assert_declared_shape(model.model_construct(field=payload))

    # The refusal must not have drained it.
    assert list(payload) == [{"smuggled": "SENTINEL-SQL-SELECT-SECRETS"}]


def test_a_re_iterable_non_collection_is_refused():
    """Isolates the ``abc.Collection`` half of the walkability rule.

    Reached through ``Any`` so no element annotation can catch it instead — the
    earlier lazy-iterable test declared ``Iterable[str]``, and its element
    annotation caught the payload even with this check disabled, which left the
    check itself unpinned.
    """
    from typing import Any as AnyType

    from boomi_mcp.recipes.engine import _assert_declared_shape

    class Replayable:
        """Re-iterable, but not sized — so not enumerable in a bounded way."""

        def __init__(self, items):
            self._items = list(items)

        def __iter__(self):
            return iter(self._items)

    model = type(
        "ReIterableNonCollectionInputV1",
        (RecipeInputBase,),
        {
            "model_config": ConfigDict(extra="forbid", frozen=True),
            "__annotations__": {"field": AnyType},
            "field": None,
        },
    )
    payload = Replayable([{"smuggled": "SENTINEL-SQL-SELECT-SECRETS"}])
    assert not isinstance(payload, abc.Collection)  # the premise
    with pytest.raises(Exception):
        _assert_declared_shape(model.model_construct(field=payload))


def test_a_self_iterating_collection_is_refused():
    """Isolates the ``iter(v) is v`` half.

    A value can satisfy ``abc.Collection`` — sized, containable — and still BE
    its own iterator, in which case walking it drains exactly the value the
    executor is about to receive. Sized is not the same as safe to enumerate.
    """
    from typing import Any as AnyType

    from boomi_mcp.recipes.engine import _assert_declared_shape

    class SelfIterating:
        def __init__(self, items):
            self._items = list(items)
            self._cursor = iter(self._items)

        def __iter__(self):
            return self  # its own iterator

        def __next__(self):
            return next(self._cursor)

        def __len__(self):
            return len(self._items)

        def __contains__(self, item):
            return item in self._items

    payload = SelfIterating([{"smuggled": "SENTINEL-SQL-SELECT-SECRETS"}])
    assert isinstance(payload, abc.Collection)  # the premise
    assert iter(payload) is payload  # ...and the hazard

    model = type(
        "SelfIteratingInputV1",
        (RecipeInputBase,),
        {
            "model_config": ConfigDict(extra="forbid", frozen=True),
            "__annotations__": {"field": AnyType},
            "field": None,
        },
    )
    with pytest.raises(Exception):
        _assert_declared_shape(model.model_construct(field=payload))

    # And it was refused without being drained.
    assert list(payload) == [{"smuggled": "SENTINEL-SQL-SELECT-SECRETS"}]


def test_a_subclass_at_a_model_position_is_ACCEPTED():
    """An ACCEPTANCE pin, which this suite is short of.

    Every other fixture here is an attack, so an over-tightening mutant survives
    by construction — the r39 ``Any`` blocker was exactly that shape, a check made
    too strict with no test able to see it. Mutating a guard toward *stricter*,
    not only toward *absent*, is what finds them (issue #145, live QA).

    The asymmetry pinned here is deliberate: the ROOT is checked by exact type
    (r30 — a subclass with ``extra="allow"`` and ``model_construct`` defeats
    ``isinstance`` there), while positions BELOW the root use ``isinstance``, so
    ordinary subclass usage keeps working. A future reader "fixing the
    inconsistency" in the wrong direction would break honest models silently.
    """
    from typing import Optional as OptionalType

    from boomi_mcp.recipes.engine import _assert_declared_shape

    class Leaf(RecipeInputBase):
        ok: str = "x"

    class RicherLeaf(Leaf):
        extra_field: str = "y"

    class HolderInputV1(RecipeInputBase):
        one: Leaf = Leaf()
        many: Tuple[Leaf, ...] = ()
        mapped: Dict[str, Leaf] = {}
        maybe: OptionalType[Leaf] = None

    subclass = RicherLeaf()
    instance = HolderInputV1.model_construct(
        one=subclass,
        many=(subclass,),
        mapped={"k": subclass},
        maybe=subclass,
    )
    _assert_declared_shape(instance)  # must not raise

    # ...and allowing subclasses did NOT reopen the extras door.
    class OpenLeaf(Leaf):
        model_config = ConfigDict(extra="allow", frozen=True)

    leaky = OpenLeaf.model_construct(ok="x", smuggled="SENTINEL-SQL-SELECT-SECRETS")
    with pytest.raises(Exception):
        _assert_declared_shape(HolderInputV1.model_construct(one=leaky))


class _ConvertingLeaf(RecipeInputBase):
    """Filters extras on the way in, then hands the mapping back on the way out.

    Validation genuinely succeeds, so the strict adapter accepts it — the lie is
    the runtime class, not the data (issue #145, Codex review).
    """

    ok: str = "x"

    @model_validator(mode="before")
    @classmethod
    def _keep_declared(cls, data):
        if isinstance(data, dict):
            return {k: v for k, v in data.items() if k in cls.model_fields}
        return data

    @model_validator(mode="after")
    def _hand_it_back(self):
        return {"ok": self.ok, "smuggled": "SENTINEL-SQL-SELECT-SECRETS"}


def test_a_mixed_union_does_not_discard_the_model_check():
    """Scoping the class check to model positions reopened the bypass.

    In ``Union[Leaf, str]`` the ``str`` option made the whole check return,
    discarding the ``Leaf`` already collected — so a dict carrying the caller's
    key reached the executor again through a declared field.
    """
    from typing import Union as UnionType

    from boomi_mcp.recipes.engine import _assert_declared_shape

    class MixedUnionInputV1(RecipeInputBase):
        field: UnionType[_ConvertingLeaf, str] = "x"

    instance = MixedUnionInputV1.model_validate({"field": {"ok": "y"}})
    assert isinstance(instance.field, dict)  # the premise
    assert "SENTINEL-SQL-SELECT-SECRETS" in str(instance.field)

    with pytest.raises(Exception):
        _assert_declared_shape(instance)


def test_a_union_that_genuinely_admits_a_mapping_still_passes():
    """Firing control for the mixed-union fix: the last-chance pass against the
    non-class options is what keeps this honest shape working."""
    from typing import Union as UnionType

    from boomi_mcp.recipes.engine import _assert_declared_shape

    class HonestUnionInputV1(RecipeInputBase):
        field: UnionType[_ConvertingLeaf, Dict[str, str]] = {}

    _assert_declared_shape(HonestUnionInputV1.model_construct(field={"a": "b"}))


def test_a_conversion_capable_wrapper_keeps_its_type():
    """``TypeAdapter(SecretStr).validate_python("plain", strict=True)`` SUCCEEDS —
    it builds a new ``SecretStr`` and the result is discarded.

    So a raw ``str`` left in a ``SecretStr`` field reaches the executor with the
    redaction and the API of the declared type both gone. Strict mode converts;
    it does not merely check.
    """
    from pydantic import SecretStr

    from boomi_mcp.recipes.engine import _assert_declared_shape

    class SecretInputV1(RecipeInputBase):
        field: SecretStr = SecretStr("s")

    _assert_declared_shape(SecretInputV1.model_construct(field=SecretStr("s")))  # control
    with pytest.raises(Exception):
        _assert_declared_shape(SecretInputV1.model_construct(field="plain"))


def test_a_collection_sharing_one_iterator_is_refused_not_drained():
    """``abc.Collection`` is not proof of replayability.

    It guarantees ``__len__``, ``__contains__`` and ``__iter__`` — none of which
    promises a FRESH iterator. A collection handing out one shared internal
    iterator passes ``iter(v) is not v`` and is drained by the walk before the
    executor sees it, so walkability is now an enumerated list of known-safe
    types rather than an inference from an interface.
    """
    from typing import Any as AnyType

    from boomi_mcp.recipes.engine import _assert_declared_shape

    class SharedIterator:
        def __init__(self, items):
            self._items = list(items)
            self._shared = iter(self._items)

        def __iter__(self):
            return self._shared  # NOT self, and NOT fresh

        def __len__(self):
            return len(self._items)

        def __contains__(self, item):
            return item in self._items

    payload = SharedIterator([{"smuggled": "SENTINEL-SQL-SELECT-SECRETS"}])
    assert isinstance(payload, abc.Collection)  # the premise
    assert iter(payload) is not payload  # ...and why the old rule passed it

    model = type(
        "SharedIterInputV1",
        (RecipeInputBase,),
        {
            "model_config": ConfigDict(extra="forbid", frozen=True),
            "__annotations__": {"field": AnyType},
            "field": None,
        },
    )
    with pytest.raises(Exception):
        _assert_declared_shape(model.model_construct(field=payload))

    assert list(payload) == [{"smuggled": "SENTINEL-SQL-SELECT-SECRETS"}], "drained"


def test_a_typed_dict_field_does_not_fail_every_invocation():
    """Not every class supports ``isinstance``.

    A bounded ``TypedDict`` passes both registration gates and pydantic correctly
    stores a ``dict`` for it — but ``isinstance(value, SomeTypedDict)`` raises
    ``TypeError``, which the engine turned into ``RECIPE_INPUT_INVALID``, failing
    EVERY invocation of an otherwise valid recipe.

    Second instance of the class after ``typing.Any``, which is why the guard is
    general rather than a ``TypedDict`` special case (issue #145, Codex review).
    """
    from typing import TypedDict as TypedDictType, Union as UnionType

    from boomi_mcp.recipes.engine import _assert_declared_shape

    class Bounded(TypedDictType):
        a: str

    class TypedDictInputV1(RecipeInputBase):
        field: Bounded = {"a": "x"}

    # It registers — that is the premise of the bug.
    _registry_with_input(TypedDictInputV1)
    _assert_declared_shape(TypedDictInputV1.model_validate({"field": {"a": "y"}}))

    # And a MIXED union with one unjudgeable option is not false-rejected either.
    class Leaf(RecipeInputBase):
        ok: str = "x"

    class MixedInputV1(RecipeInputBase):
        field: UnionType[Leaf, Bounded] = {"a": "x"}

    _assert_declared_shape(MixedInputV1.model_validate({"field": {"a": "y"}}))


def test_a_pep604_union_gets_the_same_class_check_as_typing_union():
    """``get_origin(Leaf | str)`` is ``types.UnionType``, NOT ``typing.Union``.

    An earlier version flattened only the ``typing`` spelling, so ``X | Y`` left a
    single opaque option, the class list came out empty, and the runtime-class
    check was disabled under the ordinary modern spelling — reopening the
    ``SecretStr`` leak (issue #145, live QA).
    """
    from typing import Optional as OptionalType

    from pydantic import SecretStr

    from boomi_mcp.recipes.engine import _assert_declared_shape, _unwrap_annotation

    # Both spellings must flatten identically...
    assert len(_unwrap_annotation(SecretStr | None)) == len(
        _unwrap_annotation(OptionalType[SecretStr])
    ) == 2

    # ...and both must refuse a raw str left in the field.
    for annotation in (SecretStr | None, OptionalType[SecretStr]):
        model = type(
            "Pep604InputV1",
            (RecipeInputBase,),
            {
                "model_config": ConfigDict(extra="forbid", frozen=True),
                "__annotations__": {"token": annotation},
                "token": None,
            },
        )
        with pytest.raises(Exception):
            _assert_declared_shape(model.model_construct(token="SENTINEL-CREDENTIAL-REF"))


def test_the_annotation_cache_cannot_differ_by_spelling():
    """``Leaf | str == Union[Leaf, str]`` AND they hash equal.

    So one memo entry serves both, and whichever spelling was computed first used
    to decide the other's behaviour — a security guard that depended on import
    order. Flattening both to the same result is what makes the shared entry
    correct rather than merely consistent.
    """
    from typing import Union as UnionType

    from boomi_mcp.recipes.engine import _UNWRAP_CACHE, _unwrap_annotation

    class Leaf(RecipeInputBase):
        ok: str = "x"

    assert (Leaf | str) == UnionType[Leaf, str]
    assert hash(Leaf | str) == hash(UnionType[Leaf, str])

    for first, second in ((Leaf | str, UnionType[Leaf, str]), (UnionType[Leaf, str], Leaf | str)):
        _UNWRAP_CACHE.clear()
        assert _unwrap_annotation(first) == _unwrap_annotation(second)
        assert len(_unwrap_annotation(second)) == 2


def test_a_class_whose_instancecheck_raises_does_not_refuse_every_invocation():
    """``__instancecheck__`` is arbitrary user code.

    Narrowing the guard to ``TypeError`` covered ``Any`` and ``TypedDict`` but not
    a metaclass raising anything else — and a class carrying
    ``__get_pydantic_core_schema__`` is a legal field type, so it is reachable.
    Enumerating non-checkable forms is open-ended; attempting and catching is the
    only positive confirmation.
    """
    from pydantic_core import core_schema

    from boomi_mcp.recipes.engine import _assert_declared_shape

    class RaisingMeta(type):
        def __instancecheck__(cls, instance):
            raise ValueError("deliberately not a TypeError")

    class Weird(metaclass=RaisingMeta):
        @classmethod
        def __get_pydantic_core_schema__(cls, source, handler):
            return core_schema.any_schema()

    class WeirdInputV1(RecipeInputBase):
        field: Weird = None  # type: ignore[assignment]

    _assert_declared_shape(WeirdInputV1.model_construct(field="anything"))  # must not raise


_CONTAINER_UNION_CASES = [
    # (annotation-factory, value-factory, must_be_refused, id)
    ("false_rejection_list", False),
    ("false_rejection_tuple", False),
    ("miss_tuple_any_arm", True),
    ("miss_list_any_arm", True),
    ("correct_pass_tuple_any", False),
    ("correct_pass_list_any", False),
]


def _container_union_case(name):
    """Built here rather than at module scope so the union arms stay readable."""
    from typing import Any as AnyType, Union as UnionType

    class Other(RecipeInputBase):
        other: str = "y"

    class Honest(RecipeInputBase):
        ok: str = "x"

    # NOTE: ``_ConvertingLeaf.model_validate`` returns a DICT — that is the whole
    # point of the fixture. Cases that need a genuine instance must use ``Honest``,
    # or they assert a refusal that has nothing to do with container matching.
    converting = _ConvertingLeaf.model_validate({"ok": "y"})

    return {
        # An arm's parameters were applied to a DIFFERENT arm's value.
        "false_rejection_list": (
            UnionType[List[_ConvertingLeaf], List[Other]],
            [Other()],
        ),
        "false_rejection_tuple": (
            UnionType[Dict[str, Honest], Tuple[Honest, Honest]],
            (Honest(), Honest()),
        ),
        # The tuple/list arm's args are (Any, Ellipsis) / (Any,), so the MAPPING's
        # key and value annotations were dropped entirely and the dict standing in
        # for a model was never judged.
        "miss_tuple_any_arm": (
            UnionType[Tuple[AnyType, ...], Dict[str, _ConvertingLeaf]],
            {"k": converting},
        ),
        "miss_list_any_arm": (
            UnionType[List[AnyType], Dict[str, _ConvertingLeaf]],
            {"k": converting},
        ),
        # These pass CORRECTLY — the value genuinely satisfies the ``Any`` arm,
        # exactly like ``Union[Leaf, Dict[str, str]]``. Distinguishing them from
        # the two misses above is the whole point of matching on container kind.
        "correct_pass_tuple_any": (
            UnionType[Tuple[AnyType, ...], Tuple[_ConvertingLeaf, ...]],
            (converting,),
        ),
        "correct_pass_list_any": (
            UnionType[List[AnyType], List[_ConvertingLeaf]],
            [converting],
        ),
    }[name]


@pytest.mark.parametrize(
    "name,must_refuse", _CONTAINER_UNION_CASES, ids=[c[0] for c in _CONTAINER_UNION_CASES]
)
def test_element_annotations_match_the_values_container_kind(name, must_refuse):
    """One union arm's element parameters must never be applied to another's value.

    Picking the first option that had args at all broke both ways: honest values
    refused because a sibling arm's parameters were imposed on them, and a
    mapping accepted because the tuple arm's ``(Any, Ellipsis)`` made the
    key/value annotations unreadable and they were dropped (issue #145, live QA).
    """
    from boomi_mcp.recipes.engine import _assert_declared_shape

    annotation, value = _container_union_case(name)
    model = type(
        "ContainerUnionInputV1",
        (RecipeInputBase,),
        {
            "model_config": ConfigDict(extra="forbid", frozen=True),
            "__annotations__": {"field": annotation},
            "field": None,
        },
    )
    instance = model.model_construct(field=value)
    if must_refuse:
        with pytest.raises(Exception):
            _assert_declared_shape(instance)
    else:
        _assert_declared_shape(instance)


def test_same_origin_union_arms_are_judged_disjunctively():
    """Abstaining when several arms match judged NOTHING.

    Every same-origin union matches more than one arm, so the r43 rule silently
    disabled the element check for all of them — and made the false-rejection
    case it was written for pass for the wrong reason. Abstention is only safe
    where something else judges the value; for element parameters nothing else
    looks (issue #145, live QA).
    """
    from typing import List as ListType, Tuple as TupleType, Union as UnionType

    from boomi_mcp.recipes.engine import _assert_declared_shape, _element_candidates

    class Other(RecipeInputBase):
        other: str = "y"

    converting = _ConvertingLeaf.model_validate({"ok": "y"})
    assert isinstance(converting, dict)  # the premise

    annotation = UnionType[ListType[_ConvertingLeaf], ListType[Other]]
    # UNPACKED deliberately. ``_element_candidates`` returns ``(arms, matched)``,
    # so ``len(...) == 2`` measured the length of that PAIR — true for ``(int, 5)``
    # and for ``(None, None)`` alike — and the premise this test states before its
    # real assertions went unchecked (issue #145, live QA #362).
    arms, matched = _element_candidates(annotation, [Other()])
    assert matched is True
    assert len(arms) == 2  # both arms match by container

    def _model(name, ann, default):
        return type(
            name,
            (RecipeInputBase,),
            {
                "model_config": ConfigDict(extra="forbid", frozen=True),
                "__annotations__": {"field": ann},
                "field": default,
            },
        )

    listed = _model("SameOriginListInputV1", annotation, None)
    _assert_declared_shape(listed.model_construct(field=[Other()]))  # ANY arm admits
    with pytest.raises(Exception):  # NO arm admits
        _assert_declared_shape(listed.model_construct(field=[converting]))

    tupled = _model(
        "SameOriginTupleInputV1",
        UnionType[TupleType[_ConvertingLeaf, ...], TupleType[Other, ...]],
        (),
    )
    _assert_declared_shape(tupled.model_construct(field=(Other(),)))
    with pytest.raises(Exception):
        _assert_declared_shape(tupled.model_construct(field=(converting,)))

    mapped = _model(
        "SameOriginMappingInputV1",
        UnionType[Dict[str, _ConvertingLeaf], Dict[str, Other]],
        {},
    )
    _assert_declared_shape(mapped.model_construct(field={"k": Other()}))
    with pytest.raises(Exception):
        _assert_declared_shape(mapped.model_construct(field={"k": converting}))


def test_a_dataclass_with_unresolvable_hints_fails_closed():
    """Falling back to ``{name: None}`` walked every field unjudged.

    Reachable without exotica — ``from __future__ import annotations`` plus a
    ``TYPE_CHECKING``-only import. A pydantic dataclass cannot get here, but a
    stdlib one can, and the walk handles both.
    """
    import dataclasses

    from boomi_mcp.recipes.engine import _assert_declared_shape, _dataclass_field_types

    @dataclasses.dataclass
    class BrokenHints:
        member: "DefinitelyNotImportable" = None  # noqa: F821

    with pytest.raises(Exception):
        _dataclass_field_types(BrokenHints)
    with pytest.raises(Exception):
        _assert_declared_shape(BrokenHints(member={"smuggled": "SENTINEL-HOSTNAME"}))

    @dataclasses.dataclass
    class GoodHints:
        member: str = "x"

    _assert_declared_shape(GoodHints(member="x"))  # control: judged, not refused
    with pytest.raises(Exception):
        _assert_declared_shape(GoodHints(member={"smuggled": "SENTINEL-HOSTNAME"}))


def test_a_typed_dict_field_is_judged_by_its_per_key_hints():
    """A ``TypedDict`` was unjudged in BOTH dimensions.

    The class check abstains because it cannot be instance-checked (r42), and the
    mapping branch found no parametrised arm because ``get_origin`` is None — so
    nothing looked at it at all, for a field type that registers fine.
    """
    from typing import TypedDict as TypedDictType

    from boomi_mcp.recipes.engine import _assert_declared_shape

    # DECLARED-KEYS-ONLY, so the adapter above cannot reject it on an extra key —
    # otherwise the test passes without the per-key hints ever being consulted.
    class TD(TypedDictType):
        member: _DeclaredKeysOnlyLeaf

    class TypedDictFieldInputV1(RecipeInputBase):
        field: TD = {"member": _DeclaredKeysOnlyLeaf()}

    converting = _DeclaredKeysOnlyLeaf.model_validate({"ok": "y"})
    assert isinstance(converting, dict)  # the premise
    assert set(converting) == {"ok"}  # ...and nothing an extra-key check can see

    _assert_declared_shape(
        TypedDictFieldInputV1.model_construct(
            field={"member": _DeclaredKeysOnlyLeaf.model_construct(ok="y")}
        )
    )
    with pytest.raises(Exception):
        _assert_declared_shape(
            TypedDictFieldInputV1.model_construct(field={"member": converting})
        )


def test_a_short_fixed_length_arm_does_not_accept_by_abstaining():
    """``_positional`` returns ``None`` for "no opinion at this index".

    ``_assert_declared_shape(value, None)`` succeeds unconditionally, so one
    ``None`` anywhere in the candidate list accepted the element unjudged.
    ``Union[Tuple[str, str], Tuple[str, str, Leaf]]`` at index 2 produced
    ``[None, Leaf]`` and the ``None`` was tried first (issue #145, live QA).

    Needs an arm with at least TWO fixed parameters that is shorter than the
    value — with a single parameter ``_positional`` returns that parameter rather
    than ``None``, and the leak does not appear.
    """
    from typing import Tuple as TupleType, Union as UnionType

    from boomi_mcp.recipes.engine import _assert_declared_shape

    converting = _DeclaredKeysOnlyLeaf.model_validate({"ok": "y"})

    def _model(name, annotation):
        return type(
            name,
            (RecipeInputBase,),
            {
                "model_config": ConfigDict(extra="forbid", frozen=True),
                "__annotations__": {"field": annotation},
                "field": None,
            },
        )

    leaky = _model(
        "ShortArmInputV1",
        UnionType[
            TupleType[str, str], TupleType[str, str, _DeclaredKeysOnlyLeaf]
        ],
    )
    with pytest.raises(Exception):
        _assert_declared_shape(leaky.model_construct(field=("a", "b", converting)))

    # Control: the discriminating arm alone refuses it too.
    control = _model(
        "ShortArmControlInputV1", TupleType[str, str, _DeclaredKeysOnlyLeaf]
    )
    with pytest.raises(Exception):
        _assert_declared_shape(control.model_construct(field=("a", "b", converting)))


def test_a_failed_trial_does_not_mark_nodes_seen_for_the_next_one():
    """The rollback is load-bearing, and needs a value BOTH arms get past.

    A candidate that walks part of the tree before failing would otherwise leave
    nodes marked in the shared cycle guard and let a later candidate
    short-circuit past them — a false accept manufactured by the disjunction.

    The union must be over CONTAINERS: ``List[Union[A, B]]`` yields a single
    candidate and never exercises the disjunction at all (issue #145, live QA).
    """
    from typing import List as ListType, Union as UnionType

    from boomi_mcp.recipes.engine import _assert_declared_shape

    class A(RecipeInputBase):
        payload: _DeclaredKeysOnlyLeaf = _DeclaredKeysOnlyLeaf()

    class B(RecipeInputBase):
        payload: _DeclaredKeysOnlyLeaf = _DeclaredKeysOnlyLeaf()

    class Both(A, B):
        pass

    converting = _DeclaredKeysOnlyLeaf.model_validate({"ok": "y"})
    assert isinstance(converting, dict)  # the premise

    model = type(
        "RollbackInputV1",
        (RecipeInputBase,),
        {
            "model_config": ConfigDict(extra="forbid", frozen=True),
            "__annotations__": {"field": UnionType[ListType[A], ListType[B]]},
            "field": None,
        },
    )
    element = Both.model_construct(payload=converting)
    assert isinstance(element, A) and isinstance(element, B)  # both trials proceed

    with pytest.raises(Exception):
        _assert_declared_shape(model.model_construct(field=[element]))


def test_the_container_match_is_load_bearing():
    """Y4, which looked unkillable and is not.

    ``_element_candidates`` is a purely SYNTACTIC pass over the arms, so the
    adapter accepting the value through one arm does not stop another arm's
    PARAMETERS being consulted: ``_positional((Any, Any), 0)`` yields ``Any``,
    which admits anything.

    Needs ``Dict[Any, Any]`` and a ONE-element list — ``Dict[Any, str]`` rejects
    at index 1 and hides the effect (issue #145, live QA).
    """
    from typing import Any as AnyType, Dict as DictType, List as ListType, Union as UnionType

    from boomi_mcp.recipes.engine import _assert_declared_shape

    model = type(
        "ContainerMatchInputV1",
        (RecipeInputBase,),
        {
            "model_config": ConfigDict(extra="forbid", frozen=True),
            "__annotations__": {
                "field": UnionType[
                    DictType[AnyType, AnyType], ListType[_DeclaredKeysOnlyLeaf]
                ]
            },
            "field": None,
        },
    )
    converting = _DeclaredKeysOnlyLeaf.model_validate({"ok": "y"})
    with pytest.raises(Exception):
        _assert_declared_shape(model.model_construct(field=[converting]))


@pytest.mark.parametrize("spelling", ["type_alias", "new_type"])
def test_an_unrecognised_annotation_form_is_unwrapped_not_waved_through(spelling):
    """"I do not recognise this" was the same value as "nothing to check here".

    ``_unwrap_annotation`` returned an unrecognised form as its own single
    option, so ``_element_candidates`` found no ``get_origin`` and the elements
    were walked unjudged — while the adapter resolved the alias perfectly well and
    validation succeeded. Same defect as a ``None`` element annotation one level
    up (issue #145, live QA).
    """
    from typing import List as ListType, NewType

    from boomi_mcp.recipes.engine import _assert_declared_shape

    if spelling == "type_alias":
        # ``TypeAliasType``, not ``type X = ...``: PEP 695 syntax is a SyntaxError
        # on Python 3.11, which both Docker stages use (issue #145, Codex review).
        from typing_extensions import TypeAliasType

        annotation = TypeAliasType(
            "AliasOfList", ListType[_DeclaredKeysOnlyLeaf]
        )
    else:
        annotation = NewType("AliasOfList", ListType[_DeclaredKeysOnlyLeaf])

    model = type(
        "AliasInputV1",
        (RecipeInputBase,),
        {
            "model_config": ConfigDict(extra="forbid", frozen=True),
            "__annotations__": {"field": annotation},
            "field": None,
        },
    )
    converting = _DeclaredKeysOnlyLeaf.model_validate({"ok": "y"})
    with pytest.raises(Exception):
        _assert_declared_shape(model.model_construct(field=[converting]))

    # Control: an honest element passes, so the alias is unwrapped rather than
    # simply refused.
    _assert_declared_shape(
        model.model_construct(field=[_DeclaredKeysOnlyLeaf.model_construct(ok="a")])
    )


@pytest.mark.parametrize("module", ["typing", "typing_extensions"])
def test_both_typed_dict_spellings_are_judged(module):
    """``typing.is_typeddict`` is False for a ``typing_extensions.TypedDict``.

    The r44 fix worked for the spelling its fixture used and not the other — and
    an earlier census had already recorded BOTH as usable field types. When a
    census says N spellings are reachable, the fix gets verified against all N
    (issue #145, live QA).
    """
    import typing

    import typing_extensions

    from boomi_mcp.recipes.engine import _assert_declared_shape

    base = typing.TypedDict if module == "typing" else typing_extensions.TypedDict
    TD = base("TD", {"member": _DeclaredKeysOnlyLeaf})

    model = type(
        f"TypedDict{module}InputV1",
        (RecipeInputBase,),
        {
            "model_config": ConfigDict(extra="forbid", frozen=True),
            "__annotations__": {"field": TD},
            "field": None,
        },
    )
    converting = _DeclaredKeysOnlyLeaf.model_validate({"ok": "y"})
    with pytest.raises(Exception):
        _assert_declared_shape(model.model_construct(field={"member": converting}))

    _assert_declared_shape(
        model.model_construct(
            field={"member": _DeclaredKeysOnlyLeaf.model_construct(ok="a")}
        )
    )


@pytest.mark.parametrize(
    "holder", ["ForwardAliasInputV1", "SubscriptedAliasInputV1"], ids=["forward_ref", "subscripted"]
)
def test_an_alias_is_unwrapped_without_breaking_its_forward_references(holder):
    """Unwrapping alone was a REGRESSION.

    It exposed the alias's raw ``__value__``, so a ``ForwardRef`` inside
    ``type X = list['Leaf']`` reached an adapter with no namespace and raised
    ``PydanticUserError: not fully defined`` — refusing every invocation of an
    otherwise valid recipe. Closing one miss opened a false rejection of exactly
    the shape the ``Any`` blocker had.

    The subscripted case is the other half: ``A[Leaf]`` where ``type A[T] =
    list[T]`` is not a ``TypeAliasType`` and carries no ``__value__``, so its
    elements were unjudged until the parameters were substituted.

    Uses a REAL module — an ``exec``-built alias has ``__module__ = None`` and
    resolves for the wrong reason (issue #145, live QA).
    """
    from fixtures_alias import alias_forward_ref as fixture

    from boomi_mcp.recipes.engine import _assert_declared_shape

    model = getattr(fixture, holder)
    honest = fixture.AliasLeaf.model_construct(ok="a")
    converting = fixture.AliasLeaf.model_validate({"ok": "y"})
    assert isinstance(converting, dict)  # the premise

    _assert_declared_shape(model.model_construct(field=[honest]))
    with pytest.raises(Exception):
        _assert_declared_shape(model.model_construct(field=[converting]))


def test_supertype_is_unwrapped_only_for_a_real_NewType():
    """One branch guarded, one not, in the function whose job is introspection.

    An unguarded ``__supertype__`` read unwrapped any object carrying the
    attribute — including a class that is a usable field type via
    ``__get_pydantic_core_schema__``, whose honest instances were then refused.
    """
    from pydantic_core import core_schema

    from boomi_mcp.recipes.engine import _assert_declared_shape, _unwrap_annotation

    class UsableSupertype:
        __supertype__ = str

        @classmethod
        def __get_pydantic_core_schema__(cls, source, handler):
            return core_schema.is_instance_schema(cls)

    assert _unwrap_annotation(UsableSupertype) == (UsableSupertype,)

    model = type(
        "SupertypeInputV1",
        (RecipeInputBase,),
        {
            "model_config": ConfigDict(extra="forbid", frozen=True),
            "__annotations__": {"field": UsableSupertype},
            "field": None,
        },
    )
    _assert_declared_shape(model.model_construct(field=UsableSupertype()))

    # Control: a genuine NewType still unwraps.
    from typing import NewType

    assert _unwrap_annotation(NewType("Real", str)) == (str,)


def test_an_uncheckable_union_arm_does_not_cancel_a_failed_one():
    """Abstaining for the WHOLE union suppressed a failed check.

    In ``Union[SecretStr, SomeTypedDict]`` the ``TypedDict`` cannot be
    instance-checked, and returning outright cancelled the ``SecretStr`` check
    that had already failed — a raw secret string reached the executor. The
    uncheckable option now abstains for itself only, judged by the adapter, which
    can still say a ``str`` is not a ``TypedDict`` (issue #145, Codex review).
    """
    from typing import TypedDict as TypedDictType, Union as UnionType

    from pydantic import SecretStr

    from boomi_mcp.recipes.engine import _assert_declared_shape

    class Shaped(TypedDictType):
        a: str

    model = type(
        "UncheckableArmInputV1",
        (RecipeInputBase,),
        {
            "model_config": ConfigDict(extra="forbid", frozen=True),
            "__annotations__": {"field": UnionType[SecretStr, Shaped]},
            "field": None,
        },
    )
    with pytest.raises(Exception):
        _assert_declared_shape(model.model_construct(field="SENTINEL-CREDENTIAL-REF"))

    # Both honest arms still pass — the abstention is per-option, not blanket.
    _assert_declared_shape(model.model_construct(field={"a": "x"}))
    _assert_declared_shape(model.model_construct(field=SecretStr("s")))


def test_one_union_arm_must_cover_the_whole_container():
    """Per-element choice let a value satisfy no declared arm at all.

    ``[A(), B()]`` under ``Union[List[A], List[B]]``: element 0 chose the ``A``
    arm and element 1 the ``B`` arm, so the stored list matched neither — while
    the adapter accepted it via ``List[A]`` (``A`` converts a ``B``) and discarded
    the conversion (issue #145, Codex review).
    """
    from typing import List as ListType, Union as UnionType

    from boomi_mcp.recipes.engine import _assert_declared_shape

    class ArmB(RecipeInputBase):
        v: str = "b"

    class ArmA(RecipeInputBase):
        v: str = "a"

        @model_validator(mode="before")
        @classmethod
        def _from_b(cls, data):
            return {"v": data.v} if isinstance(data, ArmB) else data

    model = type(
        "OneArmInputV1",
        (RecipeInputBase,),
        {
            "model_config": ConfigDict(extra="forbid", frozen=True),
            "__annotations__": {"field": UnionType[ListType[ArmA], ListType[ArmB]]},
            "field": None,
        },
    )
    with pytest.raises(Exception):
        _assert_declared_shape(model.model_construct(field=[ArmA(), ArmB()]))

    # Either arm, held consistently, is fine.
    _assert_declared_shape(model.model_construct(field=[ArmA(), ArmA()]))
    _assert_declared_shape(model.model_construct(field=[ArmB(), ArmB()]))


def test_every_typed_dict_arm_is_tried_not_only_the_first():
    """A value matching the SECOND ``TypedDict`` arm was measured against the first.

    Its keys were absent from those hints, so it was walked unannotated and
    accepted after adapter conversion (issue #145, Codex review).
    """
    from typing import TypedDict as TypedDictType, Union as UnionType

    from pydantic import SecretStr

    from boomi_mcp.recipes.engine import _assert_declared_shape

    class MetadataTD(TypedDictType):
        meta: str

    class SecretTD(TypedDictType):
        member: SecretStr

    model = type(
        "TypedDictArmInputV1",
        (RecipeInputBase,),
        {
            "model_config": ConfigDict(extra="forbid", frozen=True),
            "__annotations__": {"field": UnionType[MetadataTD, SecretTD]},
            "field": None,
        },
    )
    with pytest.raises(Exception):
        _assert_declared_shape(
            model.model_construct(field={"member": "SENTINEL-CREDENTIAL-REF"})
        )

    _assert_declared_shape(model.model_construct(field={"member": SecretStr("s")}))
    _assert_declared_shape(model.model_construct(field={"meta": "x"}))


def test_a_parametrised_generic_with_a_raising_instancecheck_is_not_refused():
    """``_element_candidates`` caught only ``TypeError``.

    ``_assert_runtime_class`` had already been widened to catch any exception; the
    same reachable false rejection was left in the element path for a legal
    parametrised custom generic (issue #145, Codex review).
    """
    from typing import Generic, TypeVar

    from pydantic_core import core_schema

    from boomi_mcp.recipes.engine import _assert_declared_shape

    T = TypeVar("T")

    class RaisingMeta(type(Generic)):
        def __instancecheck__(cls, instance):
            raise ValueError("deliberately not a TypeError")

    class Parametrised(Generic[T], metaclass=RaisingMeta):
        @classmethod
        def __get_pydantic_core_schema__(cls, source, handler):
            return core_schema.list_schema(core_schema.any_schema())

    model = type(
        "GenericInputV1",
        (RecipeInputBase,),
        {
            "model_config": ConfigDict(extra="forbid", frozen=True),
            "__annotations__": {"field": Parametrised[int]},
            "field": None,
        },
    )
    _assert_declared_shape(model.model_construct(field=[1, 2]))  # must not raise


def test_new_type_is_recognised_by_identity_not_by_class_name():
    """A pydantic annotation MARKER can be an instance of a user class named
    ``NewType`` carrying ``__supertype__``; a name-only check unwrapped it to the
    supertype and refused its otherwise valid instances."""
    from typing import NewType as RealNewType

    from pydantic_core import core_schema

    from boomi_mcp.recipes.engine import _unwrap_annotation

    class NewType:  # deliberately shadows the name
        def __init__(self):
            self.__supertype__ = str

        @classmethod
        def __get_pydantic_core_schema__(cls, source, handler):
            return core_schema.is_instance_schema(cls)

    marker = NewType()
    assert type(marker).__name__ == "NewType"  # the premise
    assert _unwrap_annotation(marker) == (marker,)  # NOT unwrapped to str

    # Control: a genuine NewType still unwraps.
    assert _unwrap_annotation(RealNewType("Genuine", str)) == (str,)


def test_arms_that_exist_but_match_no_container_are_still_applied():
    """"No arm matched" is not "this annotation says nothing".

    A custom generic whose core schema is a list — ``MyList[Leaf]`` — stores a
    plain ``list``, which is never an instance of ``MyList``, so no arm matched by
    container and every element was walked unjudged while the adapter converted
    happily. Ninth instance of abstention-read-as-permission, and the one the r49
    site census was run to find (issue #145).
    """
    from typing import Generic, TypeVar

    from pydantic import GetCoreSchemaHandler
    from pydantic_core import core_schema

    from boomi_mcp.recipes.engine import _assert_declared_shape

    T = TypeVar("T")

    class MyList(Generic[T]):
        @classmethod
        def __get_pydantic_core_schema__(cls, source, handler: GetCoreSchemaHandler):
            args = getattr(source, "__args__", None)
            inner = (
                handler.generate_schema(args[0]) if args else core_schema.any_schema()
            )
            return core_schema.list_schema(inner)

    model = type(
        "CustomGenericInputV1",
        (RecipeInputBase,),
        {
            "model_config": ConfigDict(extra="forbid", frozen=True),
            "__annotations__": {"field": MyList[_DeclaredKeysOnlyLeaf]},
            "field": None,
        },
    )
    converting = _DeclaredKeysOnlyLeaf.model_validate({"ok": "y"})
    with pytest.raises(Exception):
        _assert_declared_shape(model.model_construct(field=[converting]))

    _assert_declared_shape(
        model.model_construct(field=[_DeclaredKeysOnlyLeaf.model_construct(ok="a")])
    )


def test_the_shipped_sources_parse_on_the_oldest_supported_python():
    """The suite runs on ONE interpreter, so a syntax break on another is
    invisible to every gate.

    PEP 695 ``type X = ...`` in a test fixture was a ``SyntaxError`` on 3.11 —
    which both Docker stages use — and would have failed at import before any
    test ran. Nothing that executes code could have caught it; only parsing under
    the older grammar does (issue #145, Codex review).
    """
    import ast

    oldest = (3, 11)
    roots = [
        _project_root / "src" / "boomi_mcp" / "recipes",
        _project_root / "tests" / "fixtures_alias",
    ]
    checked = 0
    for root in roots:
        for path in sorted(root.rglob("*.py")):
            checked += 1
            try:
                ast.parse(path.read_text(), feature_version=oldest)
            except SyntaxError as exc:  # pragma: no cover - the failure IS the point
                raise AssertionError(
                    f"{path.relative_to(_project_root)} does not parse on "
                    f"Python {oldest[0]}.{oldest[1]}: {exc.msg}"
                ) from None
    assert checked > 5, checked

    # This very test file too — it is where the offending `exec` lived.
    ast.parse(Path(__file__).read_text(), feature_version=oldest)


@pytest.mark.parametrize("shape", ["sequence", "mapping"])
def test_a_one_parameter_custom_generic_is_judged_in_both_shapes(shape):
    """The ninth site's fix landed on the sequence path only.

    ``_mapping_candidates`` kept arms of arity 2 (``Dict[K, V]``) and discarded
    the one-parameter arm a dict-shaped custom generic produces — so the fallback
    reached the sequence walk and was filtered out of the mapping walk. The
    eleventh instance of abstention read as permission, and the second time in
    two rounds that a fix landed where the defect was FOUND rather than at its
    sibling path (issue #145, live QA site census).

    Parametrised over both shapes so the sibling cannot be forgotten again.
    """
    from typing import Generic, TypeVar

    from pydantic import GetCoreSchemaHandler
    from pydantic_core import core_schema

    from boomi_mcp.recipes.engine import _assert_declared_shape

    T = TypeVar("T")

    class CustomSequence(Generic[T]):
        @classmethod
        def __get_pydantic_core_schema__(cls, source, handler: GetCoreSchemaHandler):
            args = getattr(source, "__args__", None)
            inner = handler.generate_schema(args[0]) if args else core_schema.any_schema()
            return core_schema.list_schema(inner)

    class CustomMapping(Generic[T]):
        @classmethod
        def __get_pydantic_core_schema__(cls, source, handler: GetCoreSchemaHandler):
            args = getattr(source, "__args__", None)
            inner = handler.generate_schema(args[0]) if args else core_schema.any_schema()
            return core_schema.dict_schema(core_schema.str_schema(), inner)

    generic = CustomSequence if shape == "sequence" else CustomMapping
    converting = _DeclaredKeysOnlyLeaf.model_validate({"ok": "y"})
    honest = _DeclaredKeysOnlyLeaf.model_construct(ok="a")
    poisoned = [converting] if shape == "sequence" else {"k": converting}
    clean = [honest] if shape == "sequence" else {"k": honest}

    model = type(
        f"CustomGeneric{shape}InputV1",
        (RecipeInputBase,),
        {
            "model_config": ConfigDict(extra="forbid", frozen=True),
            "__annotations__": {"field": generic[_DeclaredKeysOnlyLeaf]},
            "field": None,
        },
    )
    with pytest.raises(Exception):
        _assert_declared_shape(model.model_construct(field=poisoned))
    _assert_declared_shape(model.model_construct(field=clean))


def test_a_multi_argument_generic_is_not_read_positionally():
    """Falling back to raw ``get_args`` let a sequence read them as tuple positions.

    A list-backed ``SecondItems[str, SecretStr]`` whose schema uses its SECOND
    argument had index 0 checked against ``str``, so an after-validator could
    leave an unwrapped secret in the field. One parameter is unambiguous; more
    than one has no defined positional meaning for a container whose own origin
    did not match, so it is refused (issue #145, Codex review).
    """
    from typing import Generic, TypeVar

    from pydantic import GetCoreSchemaHandler, SecretStr
    from pydantic_core import core_schema

    from boomi_mcp.recipes.engine import _assert_declared_shape

    T = TypeVar("T")
    U = TypeVar("U")

    class SecondItems(Generic[T, U]):
        @classmethod
        def __get_pydantic_core_schema__(cls, source, handler: GetCoreSchemaHandler):
            args = getattr(source, "__args__", None)
            inner = (
                handler.generate_schema(args[1])
                if args and len(args) > 1
                else core_schema.any_schema()
            )
            return core_schema.list_schema(inner)

    model = type(
        "SecondItemsInputV1",
        (RecipeInputBase,),
        {
            "model_config": ConfigDict(extra="forbid", frozen=True),
            "__annotations__": {"field": SecondItems[str, SecretStr]},
            "field": None,
        },
    )
    with pytest.raises(Exception):
        _assert_declared_shape(model.model_construct(field=["SENTINEL-CREDENTIAL-REF"]))


def test_an_oversized_container_is_refused_not_truncated_or_walked():
    """Not copying the sequence was only half the requirement.

    Removing the eager ``list(value)`` stopped ``range(10**9)`` allocating a
    billion entries and left it WALKING a billion — which ends the process just as
    surely, only slower. Refusing rather than truncating, because walking a prefix
    and accepting the rest unexamined is the abstention-read-as-permission mistake
    this layer spent a dozen findings removing (issue #145, Codex review).
    """
    import time
    from typing import Any as AnyType

    from boomi_mcp.recipes.engine import _MAX_WALKED_ELEMENTS, _assert_declared_shape

    model = type(
        "OversizedInputV1",
        (RecipeInputBase,),
        {
            "model_config": ConfigDict(extra="forbid", frozen=True),
            "__annotations__": {"field": AnyType},
            "field": None,
        },
    )
    started = time.monotonic()
    with pytest.raises(Exception):
        _assert_declared_shape(model.model_construct(field=range(10**9)))
    # It must be refused on the SIZE, not by walking to the end.
    assert time.monotonic() - started < 1.0

    # A container comfortably under the bound is still walked.
    _assert_declared_shape(model.model_construct(field=list(range(100))))
    assert _MAX_WALKED_ELEMENTS >= 10_000  # far above any real recipe input


def test_a_nested_no_arm_walk_carries_the_enclosing_journal():
    """``walk(None)`` dropped the outer journal.

    A nested container with no arms of its own, walked inside an outer trial that
    later fails, left its marks in the shared cycle guard unrecorded — so the
    outer rollback could not remove them and the next arm skipped those nodes
    (issue #145, Codex review).

    Asserted BEHAVIOURALLY. This test used to read the source and assert that
    ``"walk(None, _journal)"`` appeared in it, which pins the text and not the
    effect: ``walk(None, _journal) if False else walk(None)`` restores the bug,
    keeps the substring, and passed the whole suite (issue #145, live QA #361).
    """
    from typing import List as ListType, Tuple as TupleType, Union as UnionType

    from boomi_mcp.recipes.engine import _assert_declared_shape

    # A BARE ``list`` annotation carries no parameters, so it yields no arms at
    # all — which is the branch this test is about. It used to be spelled as a
    # ``list`` SUBCLASS, which the exact-type rule now refuses outright.
    converting = _DeclaredKeysOnlyLeaf.model_validate({"ok": "y"})
    assert isinstance(converting, dict)  # the premise: a raw dict, not a model

    nested = [converting]
    annotation = UnionType[
        TupleType[list, str],
        TupleType[ListType[ListType[_DeclaredKeysOnlyLeaf]], int],
    ]

    def _model(name, default):
        return type(
            name,
            (RecipeInputBase,),
            {
                "model_config": ConfigDict(extra="forbid", frozen=True),
                "__annotations__": {"field": annotation},
                "field": default,
            },
        )

    # Arm A (``Tuple[Opaque, str]``) walks the Opaque — whose no-arm branch marks
    # ``nested`` — and only THEN fails on ``5`` not being a ``str``. Arm B judges
    # the same nodes, so it must not find them already marked.
    attack = _model("JournalAttackInputV1", None).model_construct(
        field=([nested], 5)
    )
    with pytest.raises(Exception):
        _assert_declared_shape(attack)

    # Both arms still accept their own honest values, so the rollback did not
    # simply refuse everything.
    honest_b = _model("JournalHonestBInputV1", None).model_construct(
        field=([[_DeclaredKeysOnlyLeaf.model_construct(ok="a")]], 5)
    )
    _assert_declared_shape(honest_b)
    honest_a = _model("JournalHonestAInputV1", None).model_construct(
        field=([[_DeclaredKeysOnlyLeaf.model_construct(ok="a")]], "s")
    )
    _assert_declared_shape(honest_a)


def test_a_mapping_arm_with_underivable_arity_is_refused():
    """A mapping recovers one and two parameters; three has no reading.

    One parameter is the value type, two are key and value — but a dict-backed
    generic with three has no defined mapping semantics, and returning no pairs
    would put it back on the "nothing to say" path that accepts unjudged
    (issue #145, Codex review).
    """
    from typing import Generic, TypeVar

    from pydantic import GetCoreSchemaHandler
    from pydantic_core import core_schema

    from boomi_mcp.recipes.engine import _assert_declared_shape

    T = TypeVar("T")
    U = TypeVar("U")
    V = TypeVar("V")

    class ThreeParamMapping(Generic[T, U, V]):
        @classmethod
        def __get_pydantic_core_schema__(cls, source, handler: GetCoreSchemaHandler):
            args = getattr(source, "__args__", None)
            inner = (
                handler.generate_schema(args[2])
                if args and len(args) > 2
                else core_schema.any_schema()
            )
            return core_schema.dict_schema(core_schema.str_schema(), inner)

    model = type(
        "ThreeParamInputV1",
        (RecipeInputBase,),
        {
            "model_config": ConfigDict(extra="forbid", frozen=True),
            "__annotations__": {
                "field": ThreeParamMapping[str, str, _DeclaredKeysOnlyLeaf]
            },
            "field": None,
        },
    )
    converting = _DeclaredKeysOnlyLeaf.model_validate({"ok": "y"})
    with pytest.raises(Exception):
        _assert_declared_shape(model.model_construct(field={"k": converting}))


def test_a_typed_dict_arm_survives_an_unreadable_sibling_arm():
    """The mapping refusal fired before the ``TypedDict`` arms were concatenated.

    ``_typed_dict_arms(...) + _mapping_candidates(...)`` evaluated the pair helper
    first, and its raise destroyed the half that DID describe the value: a
    ``Union[Tuple[X, ...], SomeTypedDict]`` was refused on every invocation, even
    though the ``TypedDict`` arm judged the value correctly one commit earlier.
    Building both arm sources in one place is what makes "nothing usable came out
    of this annotation" a statement about the WHOLE annotation
    (issue #145, live QA #357).
    """
    from typing import Tuple as TupleType, Union as UnionType

    from typing_extensions import TypedDict

    from boomi_mcp.recipes.engine import _assert_declared_shape

    class Meta(TypedDict):
        label: str
        count: int

    for name, annotation in (
        ("ellipsis", UnionType[TupleType[_DeclaredKeysOnlyLeaf, ...], Meta]),
        ("reversed", UnionType[Meta, TupleType[_DeclaredKeysOnlyLeaf, ...]]),
        ("arity3", UnionType[TupleType[int, int, int], Meta]),
    ):
        model = type(
            f"TypedDictSibling{name}InputV1",
            (RecipeInputBase,),
            {
                "model_config": ConfigDict(extra="forbid", frozen=True),
                "__annotations__": {"field": annotation},
                "field": None,
            },
        )
        # The TypedDict arm describes this exactly.
        _assert_declared_shape(model.model_construct(field={"label": "x", "count": 1}))
        # ...and is still JUDGING: an undeclared key is refused.
        with pytest.raises(Exception):
            _assert_declared_shape(
                model.model_construct(field={"label": "x", "count": 1, "extra": "e"})
            )



@pytest.mark.parametrize("shape", ["sequence", "mapping"])
def test_a_matched_generic_is_not_read_positionally_either(shape):
    """The arity rule guarded only the UNMATCHED path.

    ``_element_candidates`` sets ``matched`` as soon as ``isinstance(value, origin)``
    holds, and ``_sequence_arms`` returned matched arms unfiltered. A generic that
    SUBCLASSES its backing container is an instance of its own origin, so it took
    the matched path, skipped the rule entirely, and was read positionally anyway:
    index 0 judged against argument 0 while the schema used argument 1. Both
    directions were wrong at once — the plaintext passed and the correctly wrapped
    value failed. The mapping twin returned no pairs, raised nothing (the guard was
    suppressed by ``matched``), and walked every entry unjudged
    (issue #145, live QA #359 and #360).
    """
    from typing import Generic, TypeVar

    from pydantic import GetCoreSchemaHandler, SecretStr
    from pydantic_core import core_schema

    from boomi_mcp.recipes.engine import _assert_declared_shape, _element_candidates

    T = TypeVar("T")
    U = TypeVar("U")
    V = TypeVar("V")

    class SecondList(list, Generic[T, U]):
        """A real ``list``, whose element type is its SECOND argument."""

        @classmethod
        def __get_pydantic_core_schema__(cls, source, handler: GetCoreSchemaHandler):
            args = getattr(source, "__args__", None)
            inner = (
                handler.generate_schema(args[1])
                if args and len(args) > 1
                else core_schema.any_schema()
            )
            return core_schema.list_schema(inner)

    class ThreeMap(dict, Generic[T, U, V]):
        """A real ``dict``, whose value type is its THIRD argument."""

        @classmethod
        def __get_pydantic_core_schema__(cls, source, handler: GetCoreSchemaHandler):
            args = getattr(source, "__args__", None)
            inner = (
                handler.generate_schema(args[2])
                if args and len(args) > 2
                else core_schema.any_schema()
            )
            return core_schema.dict_schema(core_schema.str_schema(), inner)

    if shape == "sequence":
        annotation = SecondList[str, SecretStr]
        poisoned = SecondList(["SENTINEL-UNWRAPPED"])
        honest = SecondList([SecretStr("ok")])
    else:
        annotation = ThreeMap[str, str, _DeclaredKeysOnlyLeaf]
        poisoned = ThreeMap({"k": _DeclaredKeysOnlyLeaf.model_validate({"ok": "y"})})
        honest = ThreeMap({"k": _DeclaredKeysOnlyLeaf.model_construct(ok="a")})

    # The premise this test exists for: the value really does match by container,
    # so it really does take the path the rule used to skip.
    arms, matched = _element_candidates(annotation, poisoned)
    assert matched is True, (arms, matched)
    assert len(arms[0][1]) >= 2  # ...with an arm of arity 2 or more

    model = type(
        f"MatchedArity{shape}InputV1",
        (RecipeInputBase,),
        {
            "model_config": ConfigDict(extra="forbid", frozen=True),
            "__annotations__": {"field": annotation},
            "field": None,
        },
    )
    with pytest.raises(Exception):
        _assert_declared_shape(model.model_construct(field=poisoned))
    # Refused wrong-way-safe: an arm nobody can read is refused for its honest
    # values too, rather than silently accepting whatever it holds.
    with pytest.raises(Exception):
        _assert_declared_shape(model.model_construct(field=honest))


def test_a_matched_tuple_is_still_read_positionally():
    """``tuple`` is the one origin whose arguments really ARE positions.

    The arity rule must not swallow it, or every ``Tuple[A, B]`` field in a recipe
    input would be refused (issue #145, live QA #359).
    """
    from typing import Tuple as TupleType

    from boomi_mcp.recipes.engine import _assert_declared_shape

    model = type(
        "MatchedTupleInputV1",
        (RecipeInputBase,),
        {
            "model_config": ConfigDict(extra="forbid", frozen=True),
            "__annotations__": {"field": TupleType[str, int]},
            "field": None,
        },
    )
    _assert_declared_shape(model.model_construct(field=("a", 1)))
    # ...and the positions are genuinely checked, in both slots.
    with pytest.raises(Exception):
        _assert_declared_shape(model.model_construct(field=(1, 1)))
    with pytest.raises(Exception):
        _assert_declared_shape(model.model_construct(field=("a", "b")))


def test_an_unmatched_uniform_tuple_arm_is_usable():
    """``Tuple[X, ...]`` says "every element is X" whatever the container is.

    The first arity rule accepted only one-argument arms on the unmatched path,
    which would refuse a perfectly derivable uniform annotation — the false
    rejection half of this layer's two recurring failures (issue #145).
    """
    from typing import Generic, Tuple as TupleType, TypeVar, Union as UnionType

    from pydantic import GetCoreSchemaHandler
    from pydantic_core import core_schema

    from boomi_mcp.recipes.engine import _assert_declared_shape, _element_candidates

    T = TypeVar("T")
    U = TypeVar("U")

    class SecondBacked(Generic[T, U]):
        """Two parameters, so it contributes NO usable arm of its own."""

        @classmethod
        def __get_pydantic_core_schema__(cls, source, handler: GetCoreSchemaHandler):
            args = getattr(source, "__args__", None)
            inner = (
                handler.generate_schema(args[1])
                if args and len(args) > 1
                else core_schema.any_schema()
            )
            return core_schema.list_schema(inner)

    # The uniform tuple arm is the ONLY readable arm here. Pairing it with a
    # one-parameter sibling would let the sibling carry the test, and dropping the
    # ellipsis clause would go unnoticed.
    annotation = UnionType[
        TupleType[_DeclaredKeysOnlyLeaf, ...],
        SecondBacked[str, _DeclaredKeysOnlyLeaf],
    ]
    honest = [_DeclaredKeysOnlyLeaf.model_construct(ok="a")]
    arms, matched = _element_candidates(annotation, honest)
    assert matched is False, (arms, matched)  # the premise: a plain list matches neither
    assert sorted(len(a) for _o, a in arms) == [2, 2]  # ...and no one-parameter arm

    model = type(
        "UniformTupleArmInputV1",
        (RecipeInputBase,),
        {
            "model_config": ConfigDict(extra="forbid", frozen=True),
            "__annotations__": {"field": annotation},
            "field": None,
        },
    )
    _assert_declared_shape(model.model_construct(field=honest))
    converting = _DeclaredKeysOnlyLeaf.model_validate({"ok": "y"})
    assert isinstance(converting, dict)  # the premise
    with pytest.raises(Exception):
        _assert_declared_shape(model.model_construct(field=[converting]))


def test_the_walk_bound_is_enforced_on_mappings_and_at_its_exact_edge():
    """The size guard's mapping call site and its boundary were both unobserved.

    The only test used ``range(10**9)`` — a sequence — so deleting the mapping
    guard, and moving ``>`` to ``>=``, both survived (issue #145, live QA #363).
    """
    from typing import Any as AnyType

    from boomi_mcp.recipes.engine import _MAX_WALKED_ELEMENTS, _assert_declared_shape

    model = type(
        "BoundEdgeInputV1",
        (RecipeInputBase,),
        {
            "model_config": ConfigDict(extra="forbid", frozen=True),
            "__annotations__": {"field": AnyType},
            "field": None,
        },
    )
    for build in (
        lambda n: list(range(n)),
        lambda n: {i: i for i in range(n)},
    ):
        # EXACTLY at the bound is accepted; one more is refused.
        _assert_declared_shape(model.model_construct(field=build(_MAX_WALKED_ELEMENTS)))
        with pytest.raises(Exception):
            _assert_declared_shape(
                model.model_construct(field=build(_MAX_WALKED_ELEMENTS + 1))
            )



def test_an_unmatched_tuple_arm_is_not_read_positionally():
    """``matched`` gates the positional clause, and gating is the whole point.

    ``tuple`` is the one origin whose arguments really are positions — but only
    for a value that IS a tuple. Allowing the positional reading whenever the
    origin is ``tuple`` would index a ``Tuple[str, SecretStr]`` arm into a LIST
    that arm does not describe, judging element 0 against ``str`` and passing the
    plaintext straight through: one arm's answer used for another arm's value
    (issue #145).
    """
    from typing import Generic, Tuple as TupleType, TypeVar, Union as UnionType

    from pydantic import GetCoreSchemaHandler, SecretStr
    from pydantic_core import core_schema

    from boomi_mcp.recipes.engine import _assert_declared_shape, _element_candidates

    T = TypeVar("T")

    class ListBacked(Generic[T]):
        @classmethod
        def __get_pydantic_core_schema__(cls, source, handler: GetCoreSchemaHandler):
            args = getattr(source, "__args__", None)
            inner = handler.generate_schema(args[0]) if args else core_schema.any_schema()
            return core_schema.list_schema(inner)

    # The tuple arm comes FIRST, so a positional reading would be tried first and
    # would accept before the honest arm was ever consulted.
    annotation = UnionType[TupleType[str, SecretStr], ListBacked[SecretStr]]
    poisoned = ["SENTINEL-PLAINTEXT"]
    arms, matched = _element_candidates(annotation, poisoned)
    assert matched is False, (arms, matched)  # the premise: a list is neither

    model = type(
        "UnmatchedTupleArmInputV1",
        (RecipeInputBase,),
        {
            "model_config": ConfigDict(extra="forbid", frozen=True),
            "__annotations__": {"field": annotation},
            "field": None,
        },
    )
    with pytest.raises(Exception):
        _assert_declared_shape(model.model_construct(field=poisoned))
    # The honest reading — every element the one-parameter arm's type — still passes.
    _assert_declared_shape(model.model_construct(field=[SecretStr("ok")]))


def test_a_container_cannot_lie_about_its_own_enumeration():
    """Asking the author's CLASS how it enumerates is the exhausted surface again.

    The first version of this guard compared ``getattr(cls, name)`` against the
    base's method. Three bypasses, all measured through the engine:

    * a metaclass ``__getattribute__`` answers with ``list.__iter__`` while the
      ``tp_iter`` slot keeps a one-shot override;
    * the same lie hides a ``__len__`` that under-reports, so a 15,000-element
      value passed a 10,000 bound;
    * an INSTANCE attribute — ``self.items = ...`` — shadows ``dict.items`` for
      the ordinary lookup the walk performs, needing no metaclass at all.

    So the class is read through ``type``'s own descriptors, the instance through
    ``object``'s, and the walk consumes through the BASE — ``dict.items(value)``,
    ``list.__iter__(value)`` — which no override can redirect
    (issue #145, live QA #364).
    """
    from typing import Dict as DictType, List as ListType, Union as UnionType

    from boomi_mcp.recipes.engine import _MAX_WALKED_ELEMENTS, _assert_declared_shape

    class Other(RecipeInputBase):
        other: str = "y"

    converting = _DeclaredKeysOnlyLeaf.model_validate({"ok": "y"})
    assert isinstance(converting, dict)  # the premise

    class Liar(type):
        """Answers every enumeration question with the base's implementation."""

        def __getattribute__(cls, name):
            if name in ("__iter__", "__len__", "__getitem__", "items", "keys", "values", "get"):
                base = list if issubclass(cls, list) else dict
                return getattr(base, name)
            return type.__getattribute__(cls, name)

    class OneShot(list, metaclass=Liar):
        def __init__(self, items):
            super().__init__(items)
            self._n = 0

        def __iter__(self):
            self._n += 1
            return iter(list.__iter__(self)) if self._n == 1 else iter(())

    class Undercount(list, metaclass=Liar):
        def __len__(self):
            return 1

    class ShadowMap(dict):
        pass

    def _model(name, ann):
        return type(
            name,
            (RecipeInputBase,),
            {
                "model_config": ConfigDict(extra="forbid", frozen=True),
                "__annotations__": {"field": ann},
                "field": None,
            },
        )

    # The metaclass really does hide the override from ordinary introspection.
    assert getattr(OneShot, "__iter__") is list.__iter__  # the premise
    one = OneShot([converting])
    assert list(list.__iter__(one)) == [converting]  # ...and the storage is dirty
    with pytest.raises(Exception):
        _assert_declared_shape(
            _model(
                "LiarOneShotInputV1",
                UnionType[ListType[_DeclaredKeysOnlyLeaf], ListType[Other]],
            ).model_construct(field=one)
        )

    big = Undercount([_DeclaredKeysOnlyLeaf.model_construct(ok="a")] * (_MAX_WALKED_ELEMENTS + 1))
    assert len(big) == 1  # the premise: it under-reports
    assert list.__len__(big) > _MAX_WALKED_ELEMENTS
    with pytest.raises(Exception):
        _assert_declared_shape(
            _model("LiarLenInputV1", ListType[_DeclaredKeysOnlyLeaf]).model_construct(field=big)
        )

    # An INSTANCE attribute, no metaclass: the walk is shown clean pairs while the
    # real storage keeps the payload for anyone who subscripts.
    shadow = ShadowMap({"k": converting})
    object.__setattr__(
        shadow, "items", lambda: iter([("k", _DeclaredKeysOnlyLeaf.model_construct(ok="a"))])
    )
    assert dict.__getitem__(shadow, "k") is converting  # the premise
    assert [type(v) for _, v in shadow.items()] == [_DeclaredKeysOnlyLeaf]
    with pytest.raises(Exception):
        _assert_declared_shape(
            _model("ShadowItemsInputV1", DictType[str, _DeclaredKeysOnlyLeaf]).model_construct(
                field=shadow
            )
        )


def test_ordinary_container_types_are_walked_and_judged():
    """Refusing supported Python ranks above closing residue (§7).

    An exact-type rule closed the whole subclass-divergence family at once and
    refused ``OrderedDict``, a ``NamedTuple`` field and ``Counter`` on the way —
    three refusals of ordinary Python, to close attacks that need author class
    machinery and are dominated by the module-global channel §12 already accepts.
    (``__slots__`` dataclasses and ``cached_property`` were refused by two other
    guards, which are still present; a container guard never saw them.)

    Each case here is walked AND judged — a container that walks zero entries
    would satisfy the first half vacuously, so every fixture carries at least one
    entry (issue #145, live QA #400).
    """
    from collections import Counter, OrderedDict, defaultdict
    from typing import Dict as DictType, NamedTuple

    from boomi_mcp.recipes.engine import _assert_declared_shape

    converting = _DeclaredKeysOnlyLeaf.model_validate({"ok": "y"})
    assert isinstance(converting, dict)  # the premise
    honest = _DeclaredKeysOnlyLeaf.model_construct(ok="a")

    class Selection(NamedTuple):
        keys: str
        values: str

    def _model(name, ann):
        return type(
            name,
            (RecipeInputBase,),
            {
                "model_config": ConfigDict(extra="forbid", frozen=True),
                "__annotations__": {"field": ann},
                "field": None,
            },
        )

    mapping_model = _model("OrdinaryMapInputV1", DictType[str, _DeclaredKeysOnlyLeaf])
    # NON-EMPTY, every one of them: an empty container walks no entries, so it
    # would pass whatever the element rule said. ``Counter`` and ``defaultdict``
    # appear here as walked VALUES under a ``Dict[str, Leaf]`` declaration — the
    # ``Counter[K]`` ANNOTATION has its own test, and ``DefaultDict[...]`` cannot
    # register at all because the wrap/plain validator ban rejects it (§7).
    for built in (
        dict({"k": honest}),
        OrderedDict({"k": honest}),
        Counter({"k": honest}),
        defaultdict(list, {"k": honest}),
    ):
        _assert_declared_shape(mapping_model.model_construct(field=built))
    for poisoned in (
        dict({"k": converting}),
        OrderedDict({"k": converting}),
        Counter({"k": converting}),
        defaultdict(list, {"k": converting}),
    ):
        with pytest.raises(Exception):
            _assert_declared_shape(mapping_model.model_construct(field=poisoned))

    _assert_declared_shape(
        _model("NamedTupleOkInputV1", Selection).model_construct(
            field=Selection(keys="a", values="b")
        )
    )

def test_a_tuple_subclass_generic_is_not_read_positionally():
    """``origin is tuple``, not ``issubclass(origin, tuple)``.

    A generic that SUBCLASSES ``tuple`` is not a ``tuple`` annotation: its
    arguments mean whatever its own schema says, and reading them as positions is
    the ``list``-backed defect with a different base class. Widening the test to
    ``issubclass`` would reopen exactly that (issue #145, live QA #367).
    """
    from typing import Generic, TypeVar

    from pydantic import GetCoreSchemaHandler, SecretStr
    from pydantic_core import core_schema

    from boomi_mcp.recipes.engine import (
        _assert_declared_shape,
        _declared_shape_adapter,
        _element_candidates,
    )

    T = TypeVar("T")
    U = TypeVar("U")

    class SecondTuple(tuple, Generic[T, U]):
        """A real ``tuple`` whose element type is its SECOND argument.

        A VARIADIC TUPLE schema, deliberately. Spelling it as a list schema made
        the strict adapter refuse the value outright — a tuple is not a list —
        so the test passed without the arm logic ever running.
        """

        @classmethod
        def __get_pydantic_core_schema__(cls, source, handler: GetCoreSchemaHandler):
            args = getattr(source, "__args__", None)
            inner = (
                handler.generate_schema(args[1])
                if args and len(args) > 1
                else core_schema.any_schema()
            )
            return core_schema.tuple_schema([inner], variadic_item_index=0)

    annotation = SecondTuple[str, SecretStr]
    poisoned = SecondTuple(["SENTINEL-UNWRAPPED"])
    # The premise the fixture exists for: the ADAPTER accepts this, so whatever
    # refuses it is the arm logic and not a schema mismatch.
    _declared_shape_adapter(annotation).validate_python(poisoned, strict=True)
    arms, matched = _element_candidates(annotation, poisoned)
    assert matched is True  # ...it matches by container...
    assert arms[0][0] is not tuple and issubclass(arms[0][0], tuple)  # ...but is not `tuple`

    model = type(
        "SecondTupleInputV1",
        (RecipeInputBase,),
        {
            "model_config": ConfigDict(extra="forbid", frozen=True),
            "__annotations__": {"field": annotation},
            "field": None,
        },
    )
    with pytest.raises(Exception):
        _assert_declared_shape(model.model_construct(field=poisoned))




def test_a_model_that_answers_its_own_attribute_reads_is_refused():
    """The model walk read every field with ``getattr`` — author-answerable.

    A REGISTERED ROOT input model defining ``__getattribute__`` needs no
    container, no validator and no unusual annotation. It is author code, so it
    can simply count the reads: honest while the walk looks, the caller's mapping
    to everything afterwards. The gate passed and the executor read a raw dict at
    a position declared as a model (issue #145, live QA #373).

    Neither container rule touches this, and ``_check_input_schema_closed`` cannot
    see it — that reads the emitted JSON schema, and a hook is a statement about
    the class body.
    """
    from typing import Tuple as TupleType

    from boomi_mcp.recipes.engine import _assert_declared_shape

    converting = _DeclaredKeysOnlyLeaf.model_validate({"ok": "y"})
    assert isinstance(converting, dict)  # the premise
    honest = _DeclaredKeysOnlyLeaf.model_construct(ok="a")

    class CountingRoot(RecipeInputBase):
        model_config = ConfigDict(extra="forbid", frozen=True)
        field: TupleType[_DeclaredKeysOnlyLeaf, ...] = ()

        def __getattribute__(self, name):
            if name == "field":
                seen = object.__getattribute__(self, "__dict__").setdefault("_n", [0])
                seen[0] += 1
                if seen[0] > 1:  # honest only while the walk is looking
                    return (converting,)
            return super().__getattribute__(name)

    counting = CountingRoot()
    object.__getattribute__(counting, "__dict__")["field"] = (honest,)
    with pytest.raises(Exception):
        _assert_declared_shape(counting)

    # A NESTED model, reached as a field value, gets the same treatment.
    class SneakyLeaf(RecipeInputBase):
        model_config = ConfigDict(extra="forbid", frozen=True)
        ok: str = "x"

        def __getattribute__(self, name):
            if name == "ok":
                return converting
            return super().__getattribute__(name)

    class Outer(RecipeInputBase):
        model_config = ConfigDict(extra="forbid", frozen=True)
        inner: SneakyLeaf = None  # type: ignore[assignment]

    with pytest.raises(Exception):
        _assert_declared_shape(Outer.model_construct(inner=SneakyLeaf()))

    # A __getattr__ hook is refused too — it answers any lookup that FAILS.
    class LazyRoot(RecipeInputBase):
        model_config = ConfigDict(extra="forbid", frozen=True)
        field: str = "x"

        def __getattr__(self, name):
            return converting

    with pytest.raises(Exception):
        _assert_declared_shape(LazyRoot())

    # ...and an ordinary model with neither hook is still walked, and judged.
    class Honest(RecipeInputBase):
        model_config = ConfigDict(extra="forbid", frozen=True)
        field: TupleType[_DeclaredKeysOnlyLeaf, ...] = ()

    _assert_declared_shape(Honest.model_construct(field=(honest,)))
    with pytest.raises(Exception):
        _assert_declared_shape(Honest.model_construct(field=(converting,)))


def test_the_instance_dict_descriptor_is_looked_for_up_the_whole_mro():
    """The ``__dict__`` getset can land on a BASE, not on the value's own class.

    A walk that inspected only ``type(value)`` would find no ``__dict__`` entry on
    the derived class, fall through to "no instance storage", and miss a forged
    descriptor sitting one level up (issue #145, live QA #376).
    """
    from boomi_mcp.recipes.engine import _instance_vars

    class Base(dict):
        @property
        def __dict__(self):  # noqa: A003 - the forgery is the point
            return {}

    class Derived(Base):
        pass

    # The premise: the forged descriptor is on Base, and Derived declares none.
    assert "__dict__" not in Derived.__dict__
    assert type(Base.__dict__["__dict__"]).__name__ == "property"
    assert _instance_vars(Derived()) is None

    # A plain hierarchy still reads real storage, from whichever class holds it.
    class PlainBase(dict):
        pass

    class PlainDerived(PlainBase):
        pass

    plain = PlainDerived()
    object.__setattr__(plain, "marker", 1)
    assert "__dict__" not in PlainDerived.__dict__  # it is on PlainBase
    assert _instance_vars(plain) == {"marker": 1}


def test_a_dataclass_that_answers_its_own_attribute_reads_is_refused():
    """The dataclass branch reads fields by attribute too, and its class can lie.

    Three things are pinned here at once, because one fixture exercises all three
    and none of them had a test: the dataclass hook check exists at all; the hook
    scan walks the whole MRO rather than the value's own class; and the class body
    is read through ``type``'s descriptor, so a metaclass answering ``__dict__``
    with ``{}`` cannot hide the hook (issue #145, live QA #370, #373, #376).
    """
    from dataclasses import dataclass as std_dataclass

    from boomi_mcp.recipes.engine import _assert_declared_shape

    converting = _DeclaredKeysOnlyLeaf.model_validate({"ok": "y"})
    assert isinstance(converting, dict)  # the premise
    honest = _DeclaredKeysOnlyLeaf.model_construct(ok="a")

    # The lie is armed AFTER class construction: ``@dataclass`` itself reads
    # ``cls.__dict__`` to find the fields, so a metaclass that lies from the start
    # cannot be decorated at all.
    lying = []

    class HidesBody(type):
        @property
        def __dict__(cls):  # noqa: A003 - the lie is the point
            real = type.__dict__["__dict__"].__get__(cls)
            return {} if lying else real

    class Hooked(metaclass=HidesBody):
        def __getattribute__(self, name):
            if name == "leaf":
                return converting
            return object.__getattribute__(self, name)

    @std_dataclass
    class Carrier(Hooked):
        leaf: _DeclaredKeysOnlyLeaf = None  # type: ignore[assignment]

    lying.append(True)
    # The metaclass really does hide the hook from ordinary introspection, and the
    # hook itself sits on a BASE rather than on the dataclass.
    assert Hooked.__dict__ == {}
    assert Carrier.__dict__ == {}
    carrier = Carrier(leaf=honest)
    assert object.__getattribute__(carrier, "__dict__")["leaf"] is honest  # storage clean
    assert carrier.leaf is converting  # ...an ordinary read is not

    model = type(
        "HookedDataclassInputV1",
        (RecipeInputBase,),
        {
            "model_config": ConfigDict(extra="forbid", frozen=True),
            "__annotations__": {"field": Carrier},
            "field": None,
        },
    )
    with pytest.raises(Exception):
        _assert_declared_shape(model.model_construct(field=carrier))

    # An ordinary dataclass in the same position is still walked, and judged.
    @std_dataclass
    class Plain:
        leaf: _DeclaredKeysOnlyLeaf = None  # type: ignore[assignment]

    plain_model = type(
        "PlainDataclassInputV1",
        (RecipeInputBase,),
        {
            "model_config": ConfigDict(extra="forbid", frozen=True),
            "__annotations__": {"field": Plain},
            "field": None,
        },
    )
    _assert_declared_shape(plain_model.model_construct(field=Plain(leaf=honest)))
    with pytest.raises(Exception):
        _assert_declared_shape(plain_model.model_construct(field=Plain(leaf=converting)))


def test_a_model_with_a_forged_instance_dict_is_refused_not_read_as_empty():
    """A forged ``__dict__`` must refuse, not degrade to "no fields to check".

    Treating an unreadable instance dict as an empty one walks every field as
    ``None`` — which an optional field accepts — so the model sails through with
    its real storage never examined (issue #145, live QA #373).
    """
    from typing import Optional as OptionalType

    from boomi_mcp.recipes.engine import _assert_declared_shape

    converting = _DeclaredKeysOnlyLeaf.model_validate({"ok": "y"})
    assert isinstance(converting, dict)  # the premise

    class Forged(RecipeInputBase):
        model_config = ConfigDict(extra="forbid", frozen=True)
        leaf: OptionalType[_DeclaredKeysOnlyLeaf] = None

        @property
        def __dict__(self):  # noqa: A003 - the forgery is the point
            return {}

    forged = object.__new__(Forged)
    object.__setattr__(forged, "leaf", converting)
    # The premise: real storage holds the payload while the forged view is empty.
    assert object.__getattribute__(Forged, "__dict__")["__dict__"].__get__(forged) == {}
    with pytest.raises(Exception):
        _assert_declared_shape(forged)


def test_a_descriptor_under_a_field_name_is_judged_by_what_it_returns():
    """A descriptor under a field name is refused by the ANNOTATION, not by a ban.

    A ``property`` there runs author code for that field alone, wearing neither
    attribute hook's name, and a ban on "any descriptor under a field name" was
    added for it. That ban also refused every value of an ordinary
    descriptor-typed dataclass field, so it was removed (issue #145, Codex
    review) — and nothing was lost, because whatever the ordinary read returns is
    judged against the declared annotation regardless of how it got there.

    Asserted in both directions here: the payload is still refused, and a
    descriptor returning exactly what was stored is now accepted.
    """
    from dataclasses import dataclass as std_dataclass
    from typing import Optional as OptionalType

    from boomi_mcp.recipes.engine import _assert_declared_shape

    converting = _DeclaredKeysOnlyLeaf.model_validate({"ok": "y"})
    assert isinstance(converting, dict)  # the premise
    honest = _DeclaredKeysOnlyLeaf.model_construct(ok="a")

    class Intercepted(RecipeInputBase):
        model_config = ConfigDict(extra="forbid", frozen=True)
        leaf: OptionalType[_DeclaredKeysOnlyLeaf] = None

    # Installed AFTER class creation, so pydantic never sees it.
    Intercepted.leaf = property(lambda self: converting)
    inst = Intercepted.model_construct(leaf=honest)
    assert object.__getattribute__(inst, "__dict__")["leaf"] is honest  # storage honest
    assert inst.leaf is converting  # ...an ordinary read is not
    with pytest.raises(Exception):
        _assert_declared_shape(inst)

    # ...and a descriptor that returns exactly what was stored is ACCEPTED, which
    # the ban could not express: at the point it fired the two are identical.
    class Faithful(RecipeInputBase):
        model_config = ConfigDict(extra="forbid", frozen=True)
        leaf: OptionalType[_DeclaredKeysOnlyLeaf] = None

    faithful = Faithful.model_construct(leaf=honest)
    Faithful.leaf = property(
        lambda self: object.__getattribute__(self, "__dict__")["leaf"]
    )
    assert faithful.leaf is honest  # the premise: it returns the stored value
    _assert_declared_shape(faithful)

    # The descriptor can sit on a BASE, so the scan must walk the whole MRO.
    class Base(RecipeInputBase):
        model_config = ConfigDict(extra="forbid", frozen=True)
        leaf: OptionalType[_DeclaredKeysOnlyLeaf] = None

    class Derived(Base):
        pass

    Base.leaf = property(lambda self: converting)
    derived = Derived.model_construct(leaf=honest)
    assert "leaf" not in Derived.__dict__ and "leaf" in Base.__dict__  # the premise
    assert derived.leaf is converting
    with pytest.raises(Exception):
        _assert_declared_shape(derived)

    # The dataclass branch reads fields the same way and needs the same check.
    @std_dataclass
    class Carrier:
        leaf: OptionalType[_DeclaredKeysOnlyLeaf] = None

    plain = Carrier(leaf=honest)
    _assert_declared_shape(plain)  # honest control, before the descriptor lands
    Carrier.leaf = property(lambda self: converting)
    assert plain.leaf is converting
    with pytest.raises(Exception):
        _assert_declared_shape(plain)


def test_the_walk_does_not_ask_the_class_which_fields_exist():
    """``type(value).model_fields`` is attribute access a METACLASS can answer.

    A metaclass lives on ``type(type(value))``, which no scan of the value's own
    MRO ever reaches, so returning the real mapping minus one entry meant that
    field was never visited at all (issue #145, live QA #377).

    Two things close it, and both are needed: the field list is read out of the
    class BODY, and it is cross-checked against what the instance actually stores
    — a class body can be written to as well as read from.
    """
    from typing import Optional as OptionalType

    from pydantic._internal._model_construction import ModelMetaclass

    from boomi_mcp.recipes.engine import _assert_declared_shape

    converting = _DeclaredKeysOnlyLeaf.model_validate({"ok": "y"})
    assert isinstance(converting, dict)  # the premise

    class HidesAField(ModelMetaclass):
        def __getattribute__(cls, name):
            real = ModelMetaclass.__getattribute__(cls, name)
            if name == "model_fields" and ModelMetaclass.__getattribute__(cls, "_hide"):
                return {k: v for k, v in real.items() if k != "secret"}
            return real

    class Dropped(RecipeInputBase, metaclass=HidesAField):
        model_config = ConfigDict(extra="forbid", frozen=True)
        _hide = False
        label: str = "ok"
        secret: OptionalType[_DeclaredKeysOnlyLeaf] = None

    Dropped._hide = True
    hidden = Dropped.model_construct(label="ok", secret=converting)
    assert "secret" not in type(hidden).model_fields  # the premise: the lie works
    assert object.__getattribute__(hidden, "__dict__")["secret"] is converting
    with pytest.raises(Exception):
        _assert_declared_shape(hidden)

    # A forged field list written INTO the class body is caught by the cross-check.
    class Forged(RecipeInputBase):
        model_config = ConfigDict(extra="forbid", frozen=True)
        label: str = "ok"
        secret: OptionalType[_DeclaredKeysOnlyLeaf] = None

    forged = Forged.model_construct(label="ok", secret=converting)
    Forged.__pydantic_fields__ = {
        k: v for k, v in Forged.__pydantic_fields__.items() if k != "secret"
    }
    with pytest.raises(Exception):
        _assert_declared_shape(forged)


def test_a_model_cannot_redefine_the_storage_the_gate_reads():
    """``object.__getattribute__`` still runs descriptor lookup.

    A ``__pydantic_extra__`` property — with a setter, which is what makes it
    constructible — answered the undeclared-key check with ``None`` while its
    setter kept the real extras somewhere else. Reading pydantic's own slot then
    finds nothing, so the redefinition itself has to be refused
    (issue #145, live QA #378).
    """
    from boomi_mcp.recipes.engine import _assert_declared_shape

    class BlindExtras(RecipeInputBase):
        model_config = ConfigDict(extra="allow", frozen=True)
        label: str = "ok"

        @property
        def __pydantic_extra__(self):
            return None

        @__pydantic_extra__.setter
        def __pydantic_extra__(self, value):
            object.__getattribute__(self, "__dict__")["_diverted"] = value

    blind = BlindExtras.model_construct(label="ok", **{"smuggled": "SENTINEL"})
    # The premise: the extras are real, and pydantic's own slot no longer holds them.
    assert object.__getattribute__(blind, "__dict__").get("_diverted") == {
        "smuggled": "SENTINEL"
    }
    with pytest.raises(Exception):
        _assert_declared_shape(blind)


def test_a_hook_on_a_base_after_basemodel_is_still_found():
    """``class Evil(RecipeInputBase, Mixin)`` puts ``BaseModel`` BEFORE ``Mixin``.

    C3 linearises it as ``[Evil, RecipeInputBase, BaseModel, Mixin, object]``, so a
    scan that STOPPED at ``BaseModel`` never reached the mixin while Python's own
    lookup found the hook there. Skipping a trusted base is not the same as
    stopping at one (issue #145, live QA #379).
    """
    from typing import Optional as OptionalType

    from boomi_mcp.recipes.engine import _assert_declared_shape

    converting = _DeclaredKeysOnlyLeaf.model_validate({"ok": "y"})
    assert isinstance(converting, dict)  # the premise

    class HookMixin:
        def __getattribute__(self, name):
            if name == "leaf":
                return converting
            return object.__getattribute__(self, name)

    class EvilOrder(RecipeInputBase, HookMixin):
        model_config = ConfigDict(extra="forbid", frozen=True)
        leaf: OptionalType[_DeclaredKeysOnlyLeaf] = None

    # The premise: the trusted base really does precede the mixin, and the hook
    # really is live.
    mro = list(EvilOrder.__mro__)
    from pydantic import BaseModel as _BaseModel

    assert mro.index(_BaseModel) < mro.index(HookMixin)
    assert EvilOrder.__getattribute__ is HookMixin.__getattribute__
    with pytest.raises(Exception):
        _assert_declared_shape(EvilOrder.model_construct(leaf=_DeclaredKeysOnlyLeaf()))

    class DescriptorMixin:
        leaf = property(lambda self: converting)

    class EvilDescriptor(RecipeInputBase, DescriptorMixin):
        model_config = ConfigDict(extra="forbid", frozen=True)
        leaf: OptionalType[_DeclaredKeysOnlyLeaf] = None

    with pytest.raises(Exception):
        _assert_declared_shape(EvilDescriptor.model_construct(leaf=_DeclaredKeysOnlyLeaf()))


def test_a_slots_dataclass_is_walked_not_refused():
    """``__slots__`` puts a ``member_descriptor`` under EVERY field name.

    A blanket "is it a descriptor?" test read that as interception and refused 15
    slotted dataclasses on honest input — 8 of them our own process-IR types — on
    every invocation. ``__objclass__`` separates the compiler's from an author's,
    and the values behind them are storage that reads exactly as unforgeably
    (issue #145, live QA #380).
    """
    from dataclasses import dataclass as std_dataclass
    from typing import Optional as OptionalType

    from boomi_mcp.recipes.engine import _assert_declared_shape

    converting = _DeclaredKeysOnlyLeaf.model_validate({"ok": "y"})
    assert isinstance(converting, dict)  # the premise

    @std_dataclass(frozen=True)
    class Slotted:
        __slots__ = ("leaf",)
        leaf: OptionalType[_DeclaredKeysOnlyLeaf]

    assert type(Slotted.__dict__["leaf"]).__name__ == "member_descriptor"  # the premise

    model = type(
        "SlottedInputV1",
        (RecipeInputBase,),
        {
            "model_config": ConfigDict(extra="forbid", frozen=True),
            "__annotations__": {"field": Slotted},
            "field": None,
        },
    )
    _assert_declared_shape(
        model.model_construct(field=Slotted(leaf=_DeclaredKeysOnlyLeaf.model_construct(ok="a")))
    )
    # ...and the slot's value is genuinely judged, not skipped as unreadable.
    with pytest.raises(Exception):
        _assert_declared_shape(model.model_construct(field=Slotted(leaf=converting)))


def test_a_borrowed_slot_descriptor_is_not_mistaken_for_the_compiler_s():
    """``__objclass__`` is what makes the ``__slots__`` allowance safe.

    A ``member_descriptor`` can be lifted off one class and installed on another
    under a field name — it is still a ``member_descriptor``, so a type check
    alone would wave it through as compiler-generated while it reads a slot of a
    class the value has nothing to do with (issue #145).
    """
    from dataclasses import dataclass as std_dataclass
    from typing import Optional as OptionalType

    from boomi_mcp.recipes.engine import _assert_declared_shape, _is_slot_descriptor

    class Donor:
        __slots__ = ("leaf",)

    @std_dataclass
    class Thief:
        leaf: OptionalType[_DeclaredKeysOnlyLeaf] = None

    borrowed = Donor.__dict__["leaf"]
    Thief.leaf = borrowed
    # The premise: it really is the same KIND of descriptor, owned elsewhere.
    assert type(borrowed).__name__ == "member_descriptor"
    assert borrowed.__objclass__ is Donor
    assert _is_slot_descriptor(borrowed, Thief) is False
    assert _is_slot_descriptor(Donor.__dict__["leaf"], Donor) is True

    model = type(
        "BorrowedSlotInputV1",
        (RecipeInputBase,),
        {
            "model_config": ConfigDict(extra="forbid", frozen=True),
            "__annotations__": {"field": Thief},
            "field": None,
        },
    )
    with pytest.raises(Exception):
        _assert_declared_shape(model.model_construct(field=Thief.__new__(Thief)))


def test_a_plain_class_attribute_under_a_field_name_is_refused():
    """Neither a hook nor a descriptor, and Python's lookup still returns it.

    The descriptor ban asks "is it a descriptor?" and the storage read asks "is it
    in storage?". A PLAIN class attribute under a field name, with the instance's
    own entry popped, is neither — so every enumerated check passed it while an
    ordinary read returned the caller's mapping (issue #145, live QA #383).

    What refuses it is not another entry on that list: the walk now compares an
    ordinary read against what it judged, which tests the property the layer
    needs instead of the ways it can fail to hold.
    """
    from typing import Optional as OptionalType

    from boomi_mcp.recipes.engine import _assert_declared_shape

    converting = _DeclaredKeysOnlyLeaf.model_validate({"ok": "y"})
    assert isinstance(converting, dict)  # the premise

    class PlainAttr(RecipeInputBase):
        model_config = ConfigDict(extra="forbid", frozen=True)
        leaf: OptionalType[_DeclaredKeysOnlyLeaf] = None

    value = PlainAttr.model_construct(leaf=_DeclaredKeysOnlyLeaf.model_construct(ok="a"))
    object.__getattribute__(value, "__dict__").pop("leaf")
    PlainAttr.leaf = converting  # a plain dict: not a descriptor, not a hook
    assert not hasattr(type(PlainAttr.__dict__["leaf"]), "__get__")  # the premise
    assert value.leaf is converting
    with pytest.raises(Exception):
        _assert_declared_shape(value)


def test_the_dataclass_branch_cannot_be_switched_off_or_truncated():
    """``is_dataclass`` is class-level attribute access, so a metaclass answers it.

    Raising ``AttributeError`` for ``__dataclass_fields__`` skipped the whole
    branch — including its own hook scan — leaving the value walked by nothing. A
    branch-selection predicate the author can answer disables every guard inside
    that branch (issue #145, live QA #385). And the branch itself had no storage
    cross-check, so a truncated ``__dataclass_fields__`` hid a field
    (live QA #384).
    """
    from dataclasses import dataclass as std_dataclass
    from typing import Any as AnyType, Optional as OptionalType

    from boomi_mcp.recipes.engine import _assert_declared_shape

    converting = _DeclaredKeysOnlyLeaf.model_validate({"ok": "y"})
    assert isinstance(converting, dict)  # the premise

    class DeniesBeingADataclass(type):
        def __getattribute__(cls, name):
            if name == "__dataclass_fields__":
                raise AttributeError(name)
            return type.__getattribute__(cls, name)

    @std_dataclass
    class Hidden(metaclass=DeniesBeingADataclass):
        leaf: OptionalType[_DeclaredKeysOnlyLeaf] = None

        def __getattribute__(self, name):
            if name == "leaf":
                return converting
            return object.__getattribute__(self, name)

    import dataclasses as _dc

    hidden = Hidden(leaf=_DeclaredKeysOnlyLeaf.model_construct(ok="a"))
    assert _dc.is_dataclass(hidden) is False  # the premise: the lie works
    holder = type(
        "HiddenDcInputV1",
        (RecipeInputBase,),
        {
            "model_config": ConfigDict(extra="forbid", frozen=True),
            "__annotations__": {"field": AnyType},
            "field": None,
        },
    )
    with pytest.raises(Exception):
        _assert_declared_shape(holder.model_construct(field=hidden))

    @std_dataclass
    class Truncated:
        leaf: OptionalType[_DeclaredKeysOnlyLeaf] = None
        other: str = "x"

    truncated = Truncated(leaf=converting, other="x")
    Truncated.__dataclass_fields__ = {
        k: v for k, v in Truncated.__dataclass_fields__.items() if k != "leaf"
    }
    trunc_holder = type(
        "TruncDcInputV1",
        (RecipeInputBase,),
        {
            "model_config": ConfigDict(extra="forbid", frozen=True),
            "__annotations__": {"field": Truncated},
            "field": None,
        },
    )
    with pytest.raises(Exception):
        _assert_declared_shape(trunc_holder.model_construct(field=truncated))


def test_two_ordinary_idioms_are_not_read_as_hidden_fields():
    """``cached_property`` and a private attribute both land in a model's dict.

    The first makes the refusal STATE-DEPENDENT — untouched accepts, warmed
    refuses — which is the reads-as-intermittent failure this module already
    warns about; the second is the canonical way to stash a private value on a
    frozen model. Neither can be a hidden field: pydantic cannot declare a field
    whose name starts with an underscore (issue #145, live QA #387).
    """
    from functools import cached_property

    from pydantic import model_validator

    from boomi_mcp.recipes.engine import _assert_declared_shape

    class WithCached(RecipeInputBase):
        model_config = ConfigDict(extra="forbid", frozen=True)
        label: str = "x"

        @cached_property
        def total(self) -> int:
            return len(self.label)

    warmed = WithCached.model_construct(label="abc")
    _assert_declared_shape(warmed)
    assert warmed.total == 3  # populates ``total`` in the instance dict
    assert "total" in object.__getattribute__(warmed, "__dict__")  # the premise
    _assert_declared_shape(warmed)

    class WithPrivate(RecipeInputBase):
        model_config = ConfigDict(extra="forbid", frozen=True)
        label: str = "x"

        @model_validator(mode="after")
        def _stash(self):
            object.__setattr__(self, "_priv", "internal")
            return self

    stashed = WithPrivate.model_validate({"label": "a"})
    assert "_priv" in object.__getattribute__(stashed, "__dict__")  # the premise
    _assert_declared_shape(stashed)


def test_a_registered_model_cannot_change_what_it_declared():
    """Everything the registration gates read stays writable afterwards.

    Rewriting ``__pydantic_fields__[name].annotation`` to ``Any`` leaves the field
    NAME set identical, so a cross-check on names sees nothing — and re-running
    the SCHEMA gate does not catch it either, because pydantic caches the
    compiled schema while the walk reads ``FieldInfo.annotation`` directly
    (issue #145, live QA #382).
    """
    from typing import Any as AnyType

    from boomi_mcp.recipes.engine import _assert_declared_shape_unchanged
    from boomi_mcp.recipes.registry import production_registry

    shape = production_registry().declared_shape()
    assert shape, "the premise: registration recorded a shape at all"
    _assert_declared_shape_unchanged(shape)

    cls = next(iter(shape))
    name = next(iter(shape[cls]))
    original = cls.__pydantic_fields__[name].annotation
    try:
        cls.__pydantic_fields__[name].annotation = AnyType
        with pytest.raises(Exception):
            _assert_declared_shape_unchanged(shape)
    finally:
        cls.__pydantic_fields__[name].annotation = original
    _assert_declared_shape_unchanged(shape)

    # Dropping a field from the class body is caught by the same comparison.
    fields = cls.__pydantic_fields__
    try:
        cls.__pydantic_fields__ = {k: v for k, v in fields.items() if k != name}
        with pytest.raises(Exception):
            _assert_declared_shape_unchanged(shape)
    finally:
        cls.__pydantic_fields__ = fields
    _assert_declared_shape_unchanged(shape)


def test_a_counter_annotation_is_read_with_its_value_type():
    """``Counter[K]`` parametrises its KEY; its value is always ``int``.

    Reading a single-parameter mapping generic's parameter as the VALUE type is
    right for a dict-shaped generic and wrong here, so ``counts: Counter[str]``
    was refused on every honest invocation with ordinary caller input — a
    declaration-plus-caller-data failure, which is the highest class of bug this
    layer has (§7). The helper's own docstring named this generic as a
    hypothetical; it is in the standard library (issue #145, live QA #396).
    """
    import typing
    from collections import Counter

    from boomi_mcp.recipes.engine import _assert_declared_shape, _mapping_arms

    assert _mapping_arms(typing.Counter[str], Counter({"a": 1})) == ((str, int),)

    model = type(
        "CounterInputV1",
        (RecipeInputBase,),
        {
            "model_config": ConfigDict(extra="forbid", frozen=True),
            "__annotations__": {"counts": typing.Counter[str]},
            "counts": None,
        },
    )
    # Ordinary caller JSON, no class machinery anywhere.
    _assert_declared_shape(model.model_validate({"counts": {"a": 1, "b": 2}}))

    # ...and the entries are genuinely judged, in both slots.
    with pytest.raises(Exception):
        _assert_declared_shape(model.model_construct(counts=Counter({1: 1})))
    with pytest.raises(Exception):
        _assert_declared_shape(model.model_construct(counts={"a": "not-an-int"}))


def test_a_classvar_is_not_walked_as_a_dataclass_field():
    """``__dataclass_fields__`` is unfiltered; ``dataclasses.fields()`` is not.

    Reading the raw mapping let ``ClassVar`` and ``InitVar`` pseudo-fields into
    the walk, and neither is in instance storage — so an honest dataclass was
    refused on every invocation. Pinned here because the filter that fixes it
    reverted cleanly with the whole suite green (issue #145, live QA #393, #398).
    """
    from dataclasses import InitVar, dataclass as std_dataclass
    from typing import ClassVar

    from boomi_mcp.recipes.engine import _assert_declared_shape, _dataclass_field_map

    @std_dataclass
    class WithPseudoFields:
        label: str = "x"
        KIND: ClassVar[str] = "k"
        seed: InitVar[int] = 1

        def __post_init__(self, seed):  # pragma: no cover - construction only
            pass

    value = WithPseudoFields()
    # The premise: the raw class-body mapping carries all three names.
    assert set(WithPseudoFields.__dataclass_fields__) == {"label", "KIND", "seed"}
    assert set(_dataclass_field_map(value)) == {"label"}

    model = type(
        "PseudoFieldInputV1",
        (RecipeInputBase,),
        {
            "model_config": ConfigDict(extra="forbid", frozen=True),
            "__annotations__": {"field": WithPseudoFields},
            "field": None,
        },
    )
    _assert_declared_shape(model.model_construct(field=value))


def test_a_length_beyond_py_ssize_t_is_oversized_not_unsized():
    """``len(range(10**100))`` raises ``OverflowError``.

    Reporting that as "unsized" let the walk enumerate the container forever —
    and the eager ``list(value)`` this replaced failed immediately, so removing
    the copy turned a fast failure into a hang. The walk is now bounded twice:
    an overflow counts as oversized, and the enumeration stops at the bound
    whatever the value claims its length is (issue #145, Codex review).
    """
    import time
    from typing import Any as AnyType

    from boomi_mcp.recipes.engine import (
        _MAX_WALKED_ELEMENTS,
        _assert_declared_shape,
        _walkable_length,
    )

    huge = range(10**100)
    with pytest.raises(OverflowError):  # the premise
        len(huge)
    assert _walkable_length(huge) > _MAX_WALKED_ELEMENTS

    model = type(
        "OverflowInputV1",
        (RecipeInputBase,),
        {
            "model_config": ConfigDict(extra="forbid", frozen=True),
            "__annotations__": {"field": AnyType},
            "field": None,
        },
    )
    started = time.monotonic()
    with pytest.raises(Exception):
        _assert_declared_shape(model.model_construct(field=huge))
    assert time.monotonic() - started < 1.0  # refused on the bound, not by walking

    # A value that cannot report a length at all is still bounded, by the walk.
    class Unsized(list):
        def __len__(self):  # pragma: no cover - raising is the point
            raise TypeError("no length here")

    unsized = Unsized(range(_MAX_WALKED_ELEMENTS + 5))
    assert _walkable_length(unsized) is None or _walkable_length(unsized) > 0
    with pytest.raises(Exception):
        _assert_declared_shape(model.model_construct(field=unsized))


def test_a_dataclass_field_not_in_instance_storage_is_read_the_ordinary_way():
    """Two ordinary layouts keep a field's value off the instance dict.

    ``field(init=False, default=2)`` leaves the value on the CLASS — the
    generated ``__init__`` never assigns it — and a descriptor-typed field keeps
    it under a name of the descriptor's choosing. Treating "absent from storage"
    as ``None`` made the read-back check refuse every request that omitted the
    field (issue #145, Codex review).
    """
    from dataclasses import dataclass as std_dataclass, field as dc_field
    from typing import Optional as OptionalType

    from boomi_mcp.recipes.engine import _assert_declared_shape, _stored_attribute

    converting = _DeclaredKeysOnlyLeaf.model_validate({"ok": "y"})
    assert isinstance(converting, dict)  # the premise
    honest = _DeclaredKeysOnlyLeaf.model_construct(ok="a")

    @std_dataclass
    class ClassDefault:
        x: int = 1
        y: int = dc_field(init=False, default=2)

    value = ClassDefault()
    stored = object.__getattribute__(value, "__dict__")
    assert "y" not in stored  # the premise: it lives on the class
    assert value.y == 2
    assert _stored_attribute(value, "y", stored) == 2

    def _model(name, ann):
        return type(
            name,
            (RecipeInputBase,),
            {
                "model_config": ConfigDict(extra="forbid", frozen=True),
                "__annotations__": {"field": ann},
                "field": None,
            },
        )

    _assert_declared_shape(_model("ClassDefaultInputV1", ClassDefault).model_construct(field=value))

    class Passthrough:
        """Returns exactly what was stored, under a name of its own choosing."""

        def __set_name__(self, owner, name):
            self._name = "_" + name

        def __get__(self, obj, owner=None):
            return self if obj is None else object.__getattribute__(obj, self._name)

        def __set__(self, obj, value):
            object.__setattr__(obj, self._name, value)

    @std_dataclass
    class DescriptorBacked:
        leaf: OptionalType[_DeclaredKeysOnlyLeaf] = Passthrough()

    holder = _model("DescriptorBackedInputV1", DescriptorBacked)
    _assert_declared_shape(holder.model_construct(field=DescriptorBacked(leaf=honest)))
    # ...and the value behind the descriptor is genuinely judged.
    with pytest.raises(Exception):
        _assert_declared_shape(holder.model_construct(field=DescriptorBacked(leaf=converting)))
