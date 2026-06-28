"""M10 primitive↔builder contract-parity sweep (issue #116 C1).

Each of the 8 M10 fragment-only primitives emits a ``process_config`` fragment
plus a ``depends_on`` list. This test proves, for *all eight* in one place, that
the fragment a *validated* primitive emits is one the ``ProcessFlowBuilder``
accepts — i.e. the primitive layer and the builder layer never diverge on the
shape contract. It is the systematic complement to the per-primitive
``test_emit_fragment_feeds_builder_clean`` checks in
``test_primitives_source_transform.py``: it covers the remaining six shapes that
had no parity check, and it additionally threads each fragment's ``depends_on``
into ``validate_config`` so the ``$ref`` reachability path is exercised too.

No deferred shape behavior is introduced — this is coverage only (issue #116
ratifies the deferrals; see ``.codex/plans/issue-116.md``).
"""

from __future__ import annotations

from typing import Any, Dict

import pytest

from boomi_mcp.categories.components.builders.process_flow_builder import (
    ProcessFlowBuilder,
)
from boomi_mcp.patterns.base import PrimitiveBuildContext
from boomi_mcp.patterns.primitives import (
    BranchPrimitive,
    DataProcessPrimitive,
    DecisionPrimitive,
    DocumentCacheRemovePrimitive,
    DocumentCacheRetrievePrimitive,
    FlowControlPrimitive,
    ReturnDocumentsPrimitive,
    ThrowExceptionPrimitive,
)


def _ctx() -> PrimitiveBuildContext:
    return PrimitiveBuildContext(
        integration_name="Demo", component_prefix="DEMO", folder_path="/Demo"
    )


def _base_process_config() -> Dict[str, Any]:
    """A fresh database_to_api_sync base config (DB source -> REST target).

    Same literal connector IDs the existing parity checks use, returned fresh
    each call so the per-case ``**fragment["process_config"]`` merge never
    mutates a shared dict.
    """
    return {
        "process_kind": "database_to_api_sync",
        "source": {
            "connector_type": "database",
            "action_type": "Get",
            "connection_id": "11111111-1111-1111-1111-111111111111",
            "operation_id": "22222222-2222-2222-2222-222222222222",
        },
        "target": {
            "connector_type": "rest",
            "action_type": "POST",
            "connection_id": "33333333-3333-3333-3333-333333333333",
            "operation_id": "44444444-4444-4444-4444-444444444444",
        },
    }


def _fragment(primitive, params: Dict[str, Any]) -> Dict[str, Any]:
    """validate_parameters + emit_fragment in one step."""
    return primitive.emit_fragment(_ctx(), primitive.validate_parameters(params))


# (id, primitive, params, expected_depends_on)
_CASES = [
    (
        "branch_fanout",
        BranchPrimitive,
        {
            "targets": [
                {
                    "connector_type": "rest",
                    "action_type": "PUT",
                    "connection_id": "$ref:branch_conn",
                    "operation_id": "$ref:branch_op",
                }
            ]
        },
        ["branch_conn", "branch_op"],
    ),
    (
        "decision_route",
        DecisionPrimitive,
        {
            "comparison": "equals",
            "left": {"value_type": "track", "property_id": "dynamicdocument.DDP_STATUS"},
            "right": {"value_type": "static", "static_value": "active"},
            "false_notify": "status was not active",
        },
        [],
    ),
    (
        "data_process_split_combine",
        DataProcessPrimitive,
        {
            "steps": [
                {
                    "operation": "split_documents",
                    "profile_type": "json",
                    "profile_id": "$ref:orders_profile",
                    "link_element_key": "9",
                    "link_element_name": "ArrayElement1 (Root/Object/list/list/ArrayElement1)",
                },
                {
                    "operation": "combine_documents",
                    "profile_type": "xml",
                    "profile_id": "$ref:groups_profile",
                    "link_element_key": "4",
                    "link_element_name": "Group (Envelope/Body/Groups/Group)",
                },
            ]
        },
        ["orders_profile", "groups_profile"],
    ),
    (
        "return_documents",
        ReturnDocumentsPrimitive,
        {"label": "Status Updates"},
        [],
    ),
    (
        "throw_exception",
        ThrowExceptionPrimitive,
        {"message_template": "halt: {1}", "parameter_source": "current_document"},
        [],
    ),
    (
        "flow_control_batching",
        FlowControlPrimitive,
        {"for_each_count": 10},
        [],
    ),
    (
        "document_cache_retrieve",
        DocumentCacheRetrievePrimitive,
        {"document_cache_id": "$ref:doc_cache"},
        ["doc_cache"],
    ),
    (
        "document_cache_remove",
        DocumentCacheRemovePrimitive,
        {"document_cache_id": "$ref:doc_cache"},
        ["doc_cache"],
    ),
]


@pytest.mark.parametrize(
    "primitive,params,expected_depends_on",
    [(c[1], c[2], c[3]) for c in _CASES],
    ids=[c[0] for c in _CASES],
)
def test_m10_primitive_fragments_pass_process_flow_builder_validation(
    primitive, params, expected_depends_on
):
    """Every M10 primitive's validated fragment must merge into a base config
    that ProcessFlowBuilder.validate_config accepts, declaring exactly the $ref
    dependencies it introduces."""
    fragment = _fragment(primitive, params)

    assert fragment["depends_on"] == expected_depends_on

    cfg = {**_base_process_config(), **fragment["process_config"]}
    assert (
        ProcessFlowBuilder.validate_config(cfg, depends_on=fragment["depends_on"])
        is None
    )
