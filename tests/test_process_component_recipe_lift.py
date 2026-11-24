"""The recipe -> unit lifting bridge (issue #153 / M12.15).

A recipe still emits its process as an ``IntegrationComponentSpec`` alongside a
composed ProcessIR root; #159 migrates recipe authoring to author units
directly. Until then ``_lift_recipe_roots_into_units`` is what turns those two
halves into the single ``ProcessAuthoringUnitV1`` the canonical chain requires,
and it sits on the production normalization path for every typed recipe intent.

Exercised directly rather than only through a registered recipe: the sole
recipe-intent test in the suite deliberately invokes an UNREGISTERED recipe, so
it refuses before reaching this code. Covering the bridge only through that path
would leave it unexecuted, which is how untested code ships behind a passing
suite.
"""

import copy
import sys
from pathlib import Path

import pytest

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)
sys.path.insert(0, str(Path(__file__).resolve().parent))

from boomi_mcp.authoring.workflow import AuthoringWorkflowError  # noqa: E402
from boomi_mcp.errors import AUTHORING_COMPILE_BLOCKED  # noqa: E402
from boomi_mcp.models.integration_models import IntegrationComponentSpec  # noqa: E402
from boomi_mcp.models.process_ir import parse_process_ir_v1  # noqa: E402

from _m12_11_support import VALID_IR_DOC, components  # noqa: E402


def _lift_recipe_roots_into_units(*args, **kwargs):
    """The #153 lift, resolved at CALL time.

    #159 S3 removes the lift once every surviving executable emits a unit, so
    this module must still collect without it: only the tests of the lift itself
    are affected, and the direct-unit envelope tests below keep running.
    """
    from boomi_mcp.authoring import workflow

    return getattr(workflow, "_lift_recipe_roots_into_units")(*args, **kwargs)


def _ir():
    return parse_process_ir_v1(copy.deepcopy(VALID_IR_DOC))


def _roots(*keys):
    return tuple((key, _ir()) for key in keys)


def _process(key="proc", **kwargs):
    kwargs.setdefault("type", "process")
    kwargs.setdefault("name", "Recipe Process")
    # Explicit since §6 AR1-08: the lift inspects `model_fields_set` and
    # refuses the model default.
    kwargs.setdefault("action", "create")
    return IntegrationComponentSpec(key=key, **kwargs)


# ---------------------------------------------------------------------------
# #159 S3: the same envelope behaviour on DIRECT recipe output
# ---------------------------------------------------------------------------
# The lift above retires with the #153 bridge. Recipes now emit a whole
# ``ProcessUnitContributionV1`` (one root AND its envelope), so the behaviours
# the lift's tests pinned are re-homed here on that direct output, through the
# production normalization (`authoring.workflow._normalize_recipe_intent`) with
# a test registration installed at the engine's registry seam: the explicit
# name, create vs update, the update id, folder, description, dependencies,
# typed extension order and bindings, and reference-only components.

from pydantic import Field  # noqa: E402

from boomi_mcp.recipes.contracts import RecipeInputBase  # noqa: E402


from typing import Literal, Optional, Tuple  # noqa: E402

from boomi_mcp.models.process_component import ProcessExtensionBindingsV1  # noqa: E402


class _DirectEnvelopeInputV1(RecipeInputBase):
    name: str
    action: Literal["create", "update"]
    component_id: Optional[str] = None
    description: str = ""
    folder_name: Optional[str] = None
    # Spelled ``bound_keys`` here: ``depends_on`` is admitted ONLY on a unit
    # envelope (the contribution scan refuses it and ``dependencies`` in a recipe input).
    bound_keys: Tuple[str, ...] = ()
    process_extensions: ProcessExtensionBindingsV1 = Field(default_factory=ProcessExtensionBindingsV1)


class _DirectUnitInputV1(RecipeInputBase):
    version: Literal["1"]
    process_key: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    envelope: _DirectEnvelopeInputV1


from boomi_mcp.recipes.component_material import _FanoutMaterialModel  # noqa: E402


class _DirectUnitMaterialV1(_FanoutMaterialModel):
    version: Literal["1"]


def _direct_unit_preparer(inp, material, *, base_catalog):
    """Generates nothing; SELECTS the base components the unit depends on, which
    is how a direct-output recipe brings existing components into the spec."""
    from boomi_mcp.recipes.component_material import PreparedRecipeComponentsV1

    return PreparedRecipeComponentsV1(
        supporting_components=(),
        selected_base_keys=tuple(key for key in inp.envelope.bound_keys if key in base_catalog),
    )


def _direct_unit_executor(inp):
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
    fields = inp.envelope.model_dump(mode="python")
    fields["depends_on"] = fields.pop("bound_keys")
    envelope = ProcessComponentEnvelopeV1(component_key=inp.process_key, **fields)
    return (
        ProcessUnitContributionV1(
            contribution_kind="process_unit", version="1", contribution_id="unit.process",
            unit=ProcessAuthoringUnitV1(envelope=envelope, process_ir=root),
        ),
        ConstraintRequirementV1(
            contribution_kind="constraint_requirement", version="1", requirement_id="req.process",
            requirement=RequireProcessV1(kind="process", process_key=inp.process_key),
        ),
    )


def _install_direct_unit_recipe(monkeypatch):
    from boomi_mcp.recipes import engine
    from boomi_mcp.recipes.contracts import RecipeConflictPolicyV1, RecipeRegistrationV1
    from boomi_mcp.recipes.registry import build_test_registry

    registration = RecipeRegistrationV1(
        recipe_id="boomi.archetype.direct_unit_witness",
        recipe_version="1.0.0",
        entry_kind="executable_recipe",
        is_default=True,
        input_model=_DirectUnitInputV1,
        executor=_direct_unit_executor,
        output_types=("constraint_requirement", "process_unit"),
        conflict_policy=RecipeConflictPolicyV1(),
        component_material_model=_DirectUnitMaterialV1,
        component_preparer=_direct_unit_preparer,
    )
    registry = build_test_registry((registration,))
    monkeypatch.setattr(engine, "production_registry", lambda: registry)


def _normalize_direct(envelope, base_components=(), process_envelopes=()):
    from boomi_mcp.authoring.workflow import _normalize_intent
    from boomi_mcp.models.authoring_workflow import parse_authoring_request_v1

    request = parse_authoring_request_v1({"intent": {
        "intent_kind": "recipe",
        "integration_name": "direct unit witness",
        "invocations": [{
            "recipe_id": "boomi.archetype.direct_unit_witness",
            "recipe_version": "1.0.0",
            "invocation_id": "direct",
            "raw_input": {"version": "1", "process_key": "proc", "envelope": envelope},
            "component_material": {"version": "1"},
        }],
        "base_components": list(base_components),
        **({"process_envelopes": list(process_envelopes)} if process_envelopes else {}),
    }})
    return _normalize_intent(request).integration_spec


_BASE_CONNECTION = {"key": "db_conn", "type": "connector-settings", "action": "create",
                    "name": "Existing DB", "component_id": "11111111-1111-4111-8111-111111111111",
                    "config": {"connector_type": "database"}}


@pytest.mark.parametrize("action, component_id", [
    ("create", None),
    ("update", "22222222-2222-4222-8222-222222222222"),
])
def test_a_direct_recipe_unit_keeps_its_explicit_envelope(monkeypatch, action, component_id):
    """name, create/update and the update id, folder, description and declared
    dependencies reach the normalized unit exactly as the recipe authored them;
    the unit is the spec's process and no process COMPONENT is left beside it."""
    _install_direct_unit_recipe(monkeypatch)
    envelope = {"name": "Direct Unit", "action": action, "description": "A described process",
                "folder_name": "Some/Folder", "bound_keys": ["db_conn"]}
    if component_id:
        envelope["component_id"] = component_id
    spec = _normalize_direct(envelope, base_components=[_BASE_CONNECTION])
    assert [unit.envelope.component_key for unit in spec.processes] == ["proc"]
    got = spec.processes[0].envelope
    assert (got.name, got.action, got.component_id) == ("Direct Unit", action, component_id)
    assert got.description == "A described process"
    assert got.folder_name == "Some/Folder"
    assert got.depends_on == ("db_conn",)
    assert "proc" not in {c.key for c in spec.components}
    assert not [c for c in spec.components if c.type == "process"]


def test_a_direct_recipe_unit_keeps_typed_extension_order_and_bindings(monkeypatch):
    """Typed extension bindings travel on the unit unchanged: connection order,
    field order, the reference, and the label's exact bytes."""
    _install_direct_unit_recipe(monkeypatch)
    other = dict(_BASE_CONNECTION, key="api_conn", name="Existing API",
                 component_id="33333333-3333-4333-8333-333333333333",
                 config={"connector_type": "rest"})
    extensions = {"connections": [
        {"connection_id": "$ref:db_conn", "connector_type": "database",
         "fields": [{"id": "username", "label": "  Keep My Spaces  ",
                     "xpath": "DatabaseConnectionSettings/@username"},
                    {"id": "host", "label": "Host",
                     "xpath": "DatabaseConnectionSettings/@host"}]},
        {"connection_id": "$ref:api_conn", "connector_type": "rest",
         "fields": [{"id": "url", "label": "URL"}]},
    ]}
    spec = _normalize_direct(
        {"name": "Direct Unit", "action": "create", "bound_keys": ["api_conn", "db_conn"],
         "process_extensions": extensions},
        base_components=[_BASE_CONNECTION, other],
    )
    connections = spec.processes[0].envelope.process_extensions.connections
    assert [c.connection_id for c in connections] == ["$ref:db_conn", "$ref:api_conn"]
    assert [f.id for f in connections[0].fields] == ["username", "host"]
    assert connections[0].fields[0].label == "  Keep My Spaces  "
    assert connections[0].fields[0].xpath == "DatabaseConnectionSettings/@username"


def test_a_reference_only_base_process_stays_a_component_beside_a_direct_unit(monkeypatch):
    """Reuse of an EXISTING process authors no XML: it stays a component and is
    never taken for the unit's envelope."""
    _install_direct_unit_recipe(monkeypatch)
    reused = {"key": "reused", "type": "process", "action": "create", "name": "Reused",
              "config": {"reference_only": True,
                         "component_id": "44444444-4444-4444-8444-444444444444"}}
    spec = _normalize_direct(
        {"name": "Direct Unit", "action": "create", "bound_keys": ["reused"]},
        base_components=[reused])
    assert [unit.envelope.component_key for unit in spec.processes] == ["proc"]
    assert spec.processes[0].envelope.name == "Direct Unit"
    assert [c.key for c in spec.components] == ["reused"]


def test_a_per_root_envelope_cannot_second_source_a_direct_unit(monkeypatch):
    """One governance authority per root: a typed per-root envelope naming a
    recipe-emitted unit is refused, never merged."""
    _install_direct_unit_recipe(monkeypatch)
    with pytest.raises(Exception) as excinfo:
        _normalize_direct(
            {"name": "Direct Unit", "action": "create"},
            process_envelopes=[{"component_key": "proc", "name": "Other"}],
        )
    # #159 S3 (AR1-05): the field is retired, so the second source is refused
    # as an unknown field by the typed request, before normalization.
    assert "process_envelopes" in str(excinfo.value) and "extra_forbidden" in str(excinfo.value)
