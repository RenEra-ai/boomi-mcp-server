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

from boomi_mcp.authoring.workflow import (  # noqa: E402
    AuthoringWorkflowError,
    _lift_recipe_roots_into_units,
)
from boomi_mcp.errors import AUTHORING_COMPILE_BLOCKED  # noqa: E402
from boomi_mcp.models.integration_models import IntegrationComponentSpec  # noqa: E402
from boomi_mcp.models.process_ir import parse_process_ir_v1  # noqa: E402

from _m12_11_support import VALID_IR_DOC, components  # noqa: E402


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


def test_the_root_and_its_component_become_one_unit():
    supporting, units = _lift_recipe_roots_into_units(list(components()), _roots("proc"))

    assert [u.envelope.component_key for u in units] == ["proc"]
    envelope = units[0].envelope
    assert envelope.name == "M12.11 Process"
    assert envelope.action == "create"
    assert units[0].process_ir.version == "1"

    # The lifted process is REMOVED from the components. Leaving it in both
    # would put one process in two tuples of one shared key namespace, which the
    # spec validator refuses — and would make it ambiguous which of the two
    # descriptions apply should build from.
    assert "proc" not in {c.key for c in supporting}
    assert {c.key for c in supporting} == {"db_conn", "db_op", "api_conn", "api_op"}


def test_several_roots_each_pair_with_their_own_component():
    given = [_process("a", name="A"), _process("b", name="B"), _process("c", name="C")]
    supporting, units = _lift_recipe_roots_into_units(given, _roots("a", "c"))

    assert [u.envelope.component_key for u in units] == ["a", "c"]
    assert [u.envelope.name for u in units] == ["A", "C"]
    # A process component with NO composed root is not a unit — it stays a
    # component. Only roots are lifted.
    assert [c.key for c in supporting] == ["b"]


def test_envelope_config_keys_are_promoted_from_an_allowlist():
    given = [
        _process(
            "proc",
            name="Named",
            component_id="abc-123",
            action="update",
            depends_on=["db_conn"],
            config={
                "description": "A described process",
                "folder_name": "Some/Folder",
                # Not on the allowlist: legacy component surface, and it must
                # NOT silently become envelope contract.
                "process_kind": "database_to_api_sync",
                "unrelated_legacy_key": "ignored",
            },
        )
    ]
    _supporting, units = _lift_recipe_roots_into_units(given, _roots("proc"))
    envelope = units[0].envelope

    assert envelope.description == "A described process"
    assert envelope.folder_name == "Some/Folder"
    assert envelope.component_id == "abc-123"
    assert envelope.action == "update"
    assert envelope.depends_on == ("db_conn",)
    # `process_kind` is never read — that is the whole point of the milestone.
    assert "database_to_api_sync" not in repr(envelope.model_dump())


def test_typed_extension_bindings_are_built_from_legacy_config():
    given = [
        _process(
            "proc",
            config={
                "process_extensions": {
                    "connections": [
                        {
                            # Padded exactly as the legacy reader tolerates. The
                            # adapter normalizes; the typed model would refuse it.
                            "connection_id": "  $ref:db_conn  ",
                            "connector_type": " Database ",
                            "fields": [
                                {
                                    "id": "  username  ",
                                    "label": "  Keep My Spaces  ",
                                    "xpath": "  DatabaseConnectionSettings/@username  ",
                                }
                            ],
                        }
                    ]
                }
            },
        )
    ]
    _supporting, units = _lift_recipe_roots_into_units(given, _roots("proc"))
    connection = units[0].envelope.process_extensions.connections[0]

    assert connection.connection_id == "$ref:db_conn"
    assert connection.connector_type == "database"
    field = connection.fields[0]
    assert field.id == "username"
    assert field.xpath == "DatabaseConnectionSettings/@username"
    # `label` keeps its exact bytes — the legacy renderer emits them verbatim, so
    # stripping here would move emitted XML.
    assert field.label == "  Keep My Spaces  "


def test_a_reference_only_process_is_not_an_envelope():
    """Reuse of an EXISTING component authors no XML, so it is not a root's envelope."""
    given = [
        _process("proc", name="Authored"),
        _process("reused", name="Reused", config={"reference_only": True}),
    ]
    supporting, units = _lift_recipe_roots_into_units(given, _roots("proc"))

    assert [u.envelope.component_key for u in units] == ["proc"]
    assert [c.key for c in supporting] == ["reused"]


def test_a_reference_only_component_cannot_satisfy_a_root():
    """A root whose only same-key component is reference-only has no envelope."""
    given = [_process("proc", config={"reference_only": True})]
    with pytest.raises(AuthoringWorkflowError) as excinfo:
        _lift_recipe_roots_into_units(given, _roots("proc"))
    assert excinfo.value.code == AUTHORING_COMPILE_BLOCKED


@pytest.mark.parametrize(
    "given,label",
    [
        ([], "no component at all"),
        ([_process("proc"), _process("proc")], "two authored components"),
    ],
)
def test_cardinality_other_than_exactly_one_is_refused(given, label):
    """Exactly ONE envelope per root — enforced, not assumed."""
    with pytest.raises(AuthoringWorkflowError) as excinfo:
        _lift_recipe_roots_into_units(list(given), _roots("proc"))
    assert excinfo.value.code == AUTHORING_COMPILE_BLOCKED, label
    causes = {c for d in excinfo.value.diagnostics for c in d.cause_codes}
    assert "PROCESS_COMPONENT_SCHEMA_INVALID_CARDINALITY" in causes, label


@pytest.mark.parametrize("name", [None, "", "   "])
def test_a_root_without_a_usable_name_is_refused_here_not_at_apply(name):
    """The legacy assembler refuses a blank process name much later.

    Failing at the lift names the recipe and the root; failing at the assembler
    surfaces as a builder error with no idea which recipe produced it.
    """
    given = [IntegrationComponentSpec(key="proc", type="process", name=name)]
    with pytest.raises(AuthoringWorkflowError) as excinfo:
        _lift_recipe_roots_into_units(given, _roots("proc"))
    assert excinfo.value.code == AUTHORING_COMPILE_BLOCKED
    causes = {c for d in excinfo.value.diagnostics for c in d.cause_codes}
    assert "PROCESS_COMPONENT_SCHEMA_INVALID" in causes


def test_a_name_carried_only_in_config_is_REFUSED():
    """INVERTED at §6 AR1-08 — the plan's allowlist does not include the name.

    This test used to assert that `config.component_name` was honoured as the
    envelope name. The plan is explicit (L550-551): both `name` and `action`
    must be caller-authored on the component itself, checked via
    `model_fields_set`, and the config allowlist is exactly `description`,
    `folder_name`, `process_extensions` — a config-carried name is not on it.
    Renamed rather than edited in place, because a name asserting "is honoured"
    on a test that now asserts refusal would be actively misleading.
    """
    given = [
        IntegrationComponentSpec(
            key="proc", type="process", action="create",
            config={"component_name": "From Config"},
        )
    ]
    with pytest.raises(AuthoringWorkflowError) as excinfo:
        _lift_recipe_roots_into_units(given, _roots("proc"))
    assert excinfo.value.code == AUTHORING_COMPILE_BLOCKED

    # ...and the missing-action shape is refused the same way.
    unauthored_action = [
        IntegrationComponentSpec(key="proc", type="process", name="Named")
    ]
    with pytest.raises(AuthoringWorkflowError):
        _lift_recipe_roots_into_units(unauthored_action, _roots("proc"))


def test_no_roots_means_no_units_and_no_components_removed():
    given = list(components())
    supporting, units = _lift_recipe_roots_into_units(given, ())
    assert units == []
    assert [c.key for c in supporting] == [c.key for c in given]


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
        "process_envelopes": list(process_envelopes),
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
    with pytest.raises(AuthoringWorkflowError) as excinfo:
        _normalize_direct(
            {"name": "Direct Unit", "action": "create"},
            process_envelopes=[{"component_key": "proc", "name": "Other"}],
        )
    assert excinfo.value.code == "GOVERNANCE_SOURCE_CONFLICT"
