"""ONE dependency graph over components AND process roots (issue #153).

`components[].key` and `processes[].envelope.component_key` share one key
namespace, and all four dependency directions enter a single topological order
with a single cycle check. That is the design constraint the issue states, and
the reason it matters is concrete: an API Service route's ``$ref:KEY`` process
must already appear in the webservice component's ``depends_on``, so a
component -> process edge is real TODAY, not hypothetical. A second sorter for
process roots would have no way to see both ends of that edge.

Before #153 the graph raised a bare ``ValueError`` with prose. It now raises
named codes — additively: ``IntegrationDependencyError`` subclasses
``ValueError``, so every existing caller still catches it and still reads
``str(exc)``.
"""

import sys
from pathlib import Path

import pytest

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)
sys.path.insert(0, str(Path(__file__).resolve().parent))

from boomi_mcp.categories.integration_builder import (  # noqa: E402
    IntegrationDependencyError,
    _check_process_root_dependencies,
    _topological_order,
)
from boomi_mcp.errors import (  # noqa: E402
    INTEGRATION_COMPONENT_KEY_DUPLICATE,
    INTEGRATION_DEPENDENCY_CYCLE,
    INTEGRATION_DEPENDENCY_NOT_FOUND,
    INTEGRATION_DEPENDENCY_REQUIRED,
)
from boomi_mcp.models.integration_models import (  # noqa: E402
    IntegrationComponentSpec,
    IntegrationSpecV1,
)
from boomi_mcp.models.process_component import (  # noqa: E402
    ProcessAuthoringUnitV1,
    ProcessComponentEnvelopeV1,
)
from boomi_mcp.models.process_ir import parse_process_ir_v1  # noqa: E402

from _m12_11_support import VALID_IR_DOC, components  # noqa: E402


def _ir():
    return parse_process_ir_v1(dict(VALID_IR_DOC))


def _unit(key, deps=()):
    return ProcessAuthoringUnitV1(
        envelope=ProcessComponentEnvelopeV1(
            component_key=key, name=key, action="create", depends_on=tuple(deps)
        ),
        process_ir=_ir(),
    )


def _comp(key, deps=()):
    return IntegrationComponentSpec(
        key=key, type="connector-settings", name=key, depends_on=list(deps)
    )


def _spec(comps=(), procs=()):
    return IntegrationSpecV1(name="spec", components=list(comps), processes=list(procs))


# ---------------------------------------------------------------------------
# All four directions, one order
# ---------------------------------------------------------------------------


def test_all_four_dependency_directions_order_in_one_pass():
    """component->component, component->process, process->component, process->process."""
    spec = _spec(
        comps=[_comp("c1"), _comp("c2", ["p1"])],
        procs=[_unit("p1", ["c1"]), _unit("p2", ["p1"])],
    )
    order = _topological_order(spec)

    assert set(order) == {"c1", "c2", "p1", "p2"}
    assert order.index("c1") < order.index("p1")   # process -> component
    assert order.index("p1") < order.index("c2")   # component -> process
    assert order.index("p1") < order.index("p2")   # process -> process


def test_a_components_only_spec_orders_exactly_as_before():
    """The additive field must not change the legacy surface's behaviour."""
    spec = _spec(comps=[_comp("b", ["a"]), _comp("a"), _comp("c", ["b"])])
    assert _topological_order(spec) == ["a", "b", "c"]


def test_the_order_is_deterministic_not_insertion_ordered():
    """Ties break on the sorted key, so the caller's listing order cannot leak."""
    forward = _spec(comps=[_comp("z"), _comp("a")], procs=[_unit("m")])
    backward = _spec(comps=[_comp("a"), _comp("z")], procs=[_unit("m")])
    assert _topological_order(forward) == _topological_order(backward)
    assert _topological_order(forward) == ["a", "m", "z"]


def test_a_process_only_spec_orders():
    spec = _spec(procs=[_unit("p2", ["p1"]), _unit("p1")])
    assert _topological_order(spec) == ["p1", "p2"]


# ---------------------------------------------------------------------------
# Named failures — one code per condition, across BOTH tuples
# ---------------------------------------------------------------------------


def test_a_cycle_that_crosses_the_two_tuples_is_detected():
    """The case a second sorter would structurally miss."""
    spec = _spec(comps=[_comp("c", ["p"])], procs=[_unit("p", ["c"])])
    with pytest.raises(IntegrationDependencyError) as excinfo:
        _topological_order(spec)
    assert excinfo.value.error_code == INTEGRATION_DEPENDENCY_CYCLE
    assert "c" in str(excinfo.value) and "p" in str(excinfo.value)


def test_a_process_only_cycle_is_detected():
    spec = _spec(procs=[_unit("p1", ["p2"]), _unit("p2", ["p1"])])
    with pytest.raises(IntegrationDependencyError) as excinfo:
        _topological_order(spec)
    assert excinfo.value.error_code == INTEGRATION_DEPENDENCY_CYCLE


@pytest.mark.parametrize(
    "spec_factory,label",
    [
        (lambda: _spec(procs=[_unit("p", ["nope"])]), "process -> unknown"),
        (lambda: _spec(comps=[_comp("c", ["nope"])]), "component -> unknown"),
    ],
)
def test_an_unknown_dependency_is_named(spec_factory, label):
    with pytest.raises(IntegrationDependencyError) as excinfo:
        _topological_order(spec_factory())
    assert excinfo.value.error_code == INTEGRATION_DEPENDENCY_NOT_FOUND, label


def test_a_duplicate_key_across_the_two_tuples_is_named():
    """The spec model refuses this too; the sorter must not rely on that.

    ``_topological_order`` is called with specs rebuilt from stored dumps and
    with hand-assembled ones, so it defends its own invariant rather than
    assuming an earlier validator ran.
    """
    spec = IntegrationSpecV1.model_construct(
        name="spec", components=[_comp("shared")], processes=[_unit("shared")]
    )
    with pytest.raises(IntegrationDependencyError) as excinfo:
        _topological_order(spec)
    assert excinfo.value.error_code == INTEGRATION_COMPONENT_KEY_DUPLICATE


def test_the_named_errors_stay_catchable_as_ValueError():
    """Additive by construction — existing callers catch ``ValueError``.

    If this ever stopped holding, every caller that reports ``str(exc)`` would
    start propagating instead, which is a behaviour change disguised as a
    diagnostic improvement.
    """
    assert issubclass(IntegrationDependencyError, ValueError)
    spec = _spec(procs=[_unit("p", ["missing"])])
    with pytest.raises(ValueError):
        _topological_order(spec)


# ---------------------------------------------------------------------------
# $ref must be declared in depends_on
# ---------------------------------------------------------------------------


def _root_spec(deps):
    supporting = [c for c in components() if c.type != "process"]
    return _spec(comps=supporting, procs=[_unit("root", deps)])


def test_a_root_declaring_every_reference_passes():
    """Positive control — the refusals below are meaningless without it."""
    assert _check_process_root_dependencies(
        _root_spec(("db_conn", "db_op", "api_conn", "api_op"))
    ) is None


@pytest.mark.parametrize(
    "declared",
    [(), ("db_conn",), ("db_conn", "db_op", "api_conn")],
)
def test_an_undeclared_reference_is_refused(declared):
    """Ordered apply binds references from the id registry in TOPOLOGICAL order.

    A reference the envelope does not declare is one whose component may not
    exist yet when the root is materialized — so the omission is caught at plan
    time rather than surfacing as an unresolved token mid-apply.
    """
    error = _check_process_root_dependencies(_root_spec(declared))
    assert error is not None
    assert error.error_code == INTEGRATION_DEPENDENCY_REQUIRED


def test_the_message_names_the_missing_keys():
    error = _check_process_root_dependencies(
        _root_spec(("db_conn", "db_op", "api_conn"))
    )
    assert "api_op" in str(error)
    assert "root" in str(error)


def test_extension_binding_references_count_as_references():
    """A ``$ref`` in an extension binding is bound at apply like any other."""
    from boomi_mcp.models.process_component import (
        ProcessConnectionOverrideV1,
        ProcessExtensionBindingsV1,
        ProcessOverrideFieldV1,
    )

    supporting = [c for c in components() if c.type != "process"]
    unit = ProcessAuthoringUnitV1(
        envelope=ProcessComponentEnvelopeV1(
            component_key="root",
            name="R",
            action="create",
            # Every IR reference declared, but the EXTENSION reference is not.
            depends_on=("db_conn", "db_op", "api_conn", "api_op"),
            process_extensions=ProcessExtensionBindingsV1(
                connections=(
                    ProcessConnectionOverrideV1(
                        connection_id="$ref:undeclared_conn",
                        fields=(ProcessOverrideFieldV1(id="p", label="P"),),
                    ),
                )
            ),
        ),
        process_ir=_ir(),
    )
    error = _check_process_root_dependencies(_spec(comps=supporting, procs=[unit]))
    assert error is not None
    assert error.error_code == INTEGRATION_DEPENDENCY_REQUIRED
    assert "undeclared_conn" in str(error)


def test_a_root_may_declare_more_than_it_references():
    """Extra declared dependencies stay legal — they only add ordering."""
    assert _check_process_root_dependencies(
        _root_spec(("db_conn", "db_op", "api_conn", "api_op", "extra"))
    ) is None
