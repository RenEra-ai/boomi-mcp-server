"""The component DAG stays separate from the runtime CFG (issue #140, M12.5).

This file exists because acceptance criterion 4 — "the representative flow's
IntegrationSpec component DAG includes every referenced component in valid
materialization order, **distinct from runtime CFG ordering**" — was otherwise
argued but never *evidenced*. The argument is sound (#140 changes no component
planning at all) and these tests are what make it checkable rather than merely
asserted:

* ADR-001 §1 names the component materialization DAG and the compiler-derived
  CFG as two of five separately-owned concern planes;
* `integration_builder._topological_order` remains the sole component-order
  algorithm, and #140 does not touch `integration_builder`;
* so the two orders must be derivable independently, and must genuinely differ
  for the representative flow — otherwise "distinct" is an untested claim.

The mixed flow's components mirror `test_connector_call_mixed_flow.mixed_symbols`.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
if str(_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_ROOT / "src"))

from boomi_mcp.categories.integration_builder import _topological_order
from boomi_mcp.compiler.process_ir.lowering import lower_process_ir_to_cfg
from boomi_mcp.models.integration_models import IntegrationSpecV1
from boomi_mcp.models.process_ir import parse_process_ir_v1

from tests.test_connector_call_mixed_flow import MIXED_DOC

#: Real Boomi component types, so the plan is built from the same model the
#: production build path uses rather than a stand-in.
COMPONENT_TYPES = {
    "conn_rest": "connector-settings",
    "conn_soap": "connector-settings",
    "conn_db": "connector-settings",
    "prof_get_response": "profile.json",
    "prof_soap_request": "profile.xml",
    "prof_soap_response": "profile.xml",
    "prof_patch_response": "profile.json",
    "prof_db_write": "profile.db",
    "op_rest_get": "connector-action",
    "map_get_to_soap": "transform.map",
    "op_soap_execute": "connector-action",
    "op_rest_patch": "connector-action",
    "op_db_send": "connector-action",
    "process_mixed": "process",
}


# ---------------------------------------------------------------------------
# The component plan for the representative flow.
#
# Dependencies are the REAL materialization edges: an operation needs its
# connection and its profiles; a map needs both profiles; the process needs
# everything it references. Note there is no edge between two operations — the
# component DAG has no notion of "runs after", which is exactly why it cannot be
# the runtime order.
# ---------------------------------------------------------------------------

COMPONENTS = {
    "conn_rest": [],
    "conn_soap": [],
    "conn_db": [],
    "prof_get_response": [],
    "prof_soap_request": [],
    "prof_soap_response": [],
    "prof_patch_response": [],
    "prof_db_write": [],
    "op_rest_get": ["conn_rest", "prof_get_response"],
    "map_get_to_soap": ["prof_get_response", "prof_soap_request"],
    "op_soap_execute": ["conn_soap", "prof_soap_request", "prof_soap_response"],
    "op_rest_patch": ["conn_rest", "prof_soap_response", "prof_patch_response"],
    "op_db_send": ["conn_db", "prof_db_write"],
    "process_mixed": [
        "op_rest_get",
        "map_get_to_soap",
        "op_soap_execute",
        "op_rest_patch",
        "op_db_send",
    ],
}


def _plan(components):
    """`_topological_order` over a real `IntegrationSpecV1`, in declaration order."""
    spec = IntegrationSpecV1(
        name="connector-call mixed flow",
        components=[
            {
                "key": key,
                "type": COMPONENT_TYPES[key],
                "name": key,
                "depends_on": list(deps),
            }
            for key, deps in components.items()
        ],
    )
    return _topological_order(spec)


def _order_keys(ordered):
    return [item["key"] if isinstance(item, dict) else item for item in ordered]


def _cfg_order():
    """The runtime order: the authored refs in CFG node order."""
    cfg = lower_process_ir_to_cfg(parse_process_ir_v1(MIXED_DOC))
    refs = []
    for node in cfg.nodes:
        semantic = node.semantic
        ref = getattr(semantic, "operation_ref", None) or getattr(
            semantic, "map_ref", None
        )
        if ref:
            refs.append(ref)
    return refs


# ---------------------------------------------------------------------------


def test_the_component_plan_covers_every_referenced_component():
    """AC4, first half: every component the flow references materializes."""
    ordered = _order_keys(_plan(COMPONENTS))
    assert set(ordered) == set(COMPONENTS)
    assert len(ordered) == len(COMPONENTS)


def test_every_dependency_materializes_before_its_dependent():
    """AC4, second half: the order is a VALID materialization order — a component
    can only be created once everything it references exists."""
    ordered = _order_keys(_plan(COMPONENTS))
    position = {key: index for index, key in enumerate(ordered)}
    for key, deps in COMPONENTS.items():
        for dep in deps:
            assert position[dep] < position[key], (dep, key)


def test_the_materialization_order_is_not_the_runtime_cfg_order():
    """AC4's "distinct from runtime CFG ordering" — the load-bearing half.

    These are two different graphs over overlapping nodes: the CFG orders the
    five calls by EXECUTION, while the component DAG orders by DEPENDENCY and
    has no edge between two operations at all. Connections and profiles must all
    precede the operations that bind them, which no runtime order requires."""
    materialization = _order_keys(_plan(COMPONENTS))
    runtime = _cfg_order()

    assert runtime == [
        "op_rest_get",
        "map_get_to_soap",
        "op_soap_execute",
        "op_rest_patch",
        "op_db_send",
    ]
    # Restricting materialization to just the runtime nodes must NOT reproduce
    # the runtime order — if it did, "distinct" would be vacuous here.
    restricted = [key for key in materialization if key in set(runtime)]
    assert set(restricted) == set(runtime)
    assert restricted != runtime, (
        "the component order coincided with the runtime order, so this fixture "
        "cannot demonstrate that the two planes are distinct"
    )
    # The sharpest instance: the Database Send materializes FIRST among the five
    # calls (a sorted Kahn walk emits a component as soon as ITS OWN deps are
    # satisfied, and `op_db_send` has the fewest) while it EXECUTES LAST. The two
    # planes therefore order the same node in opposite positions, which is what
    # "distinct" has to mean to be worth asserting.
    assert materialization.index("op_db_send") < materialization.index("op_rest_get")
    assert runtime.index("op_db_send") > runtime.index("op_rest_get")


def test_declaration_order_cannot_change_the_materialization_order():
    """Determinism, the ComponentPlan half of AC7. `_topological_order` is a
    sorted-everywhere Kahn walk, so a shuffled spec must plan identically."""
    shuffled = dict(reversed(list(COMPONENTS.items())))
    assert _order_keys(_plan(shuffled)) == _order_keys(_plan(COMPONENTS))
    rotated = dict(list(COMPONENTS.items())[5:] + list(COMPONENTS.items())[:5])
    assert _order_keys(_plan(rotated)) == _order_keys(_plan(COMPONENTS))


def test_repeated_planning_is_identical():
    assert _order_keys(_plan(COMPONENTS)) == _order_keys(_plan(COMPONENTS))


def test_140_does_not_touch_the_component_order_algorithm():
    """The argument this file makes checkable: the materialization order comes
    from `integration_builder`, which #140 never modified. If a future change
    moves component ordering into the compiler, this import breaks and the claim
    above stops being true silently."""
    import boomi_mcp.categories.integration_builder as builder

    assert callable(builder._topological_order)
    source = Path(builder.__file__).read_text()
    assert "connector_call" not in source
    assert "ConnectorCall" not in source
