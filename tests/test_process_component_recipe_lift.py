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
