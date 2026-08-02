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

    # The registration gates are fooled — that is the premise, so assert it.
    registry = _registry_with_input(OpenInputV1, executor=_recording_executor)

    # ...and the model really does still accept undeclared keys.
    smuggled = OpenInputV1.model_validate({"label": "a", "smuggled": "S"})
    assert getattr(smuggled, "smuggled", None) == "S"

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
