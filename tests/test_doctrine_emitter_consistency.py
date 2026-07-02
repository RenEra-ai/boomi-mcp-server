"""Doctrine -> emitter consistency guardrail (M10.1, issue #105).

Pure-unit test that pins ``design_doctrine.EMITTABLE_SHAPE_REGISTRY`` — the
structured ``{shapetype -> {emittable, emitter_kind}}`` source of truth — against
the REAL process-flow dispatch/emission paths in ``process_flow_builder.py``. It
fails loudly if doctrine ever claims a shape is emittable without a backing
emitter branch (or vice versa), so doctrine and emitter cannot drift apart.

The dispatch keys are extracted from the actual ``_emit_flow_shape`` source via
AST (not by matching method names), and every emittable entry is additionally
proven by emitting a real shape and checking the produced ``shapetype``.
"""

import ast
import inspect
import sys
import textwrap
import xml.etree.ElementTree as ET
from pathlib import Path

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

from boomi_mcp.kb.design_doctrine import (  # noqa: E402
    EMITTABLE_SHAPE_REGISTRY,
    get_design_doctrine_catalog,
)
from boomi_mcp.categories.components.builders import (  # noqa: E402
    PROCESS_FLOW_BUILDERS,
)
from boomi_mcp.categories.components.builders import (  # noqa: E402
    process_flow_builder as pfb,
)


# Issue #105: the exact emittable shapetype set shipped at M10.1. A later M10
# shape issue that flips a shape to emittable MUST update both the registry and
# this pin in lockstep — that is the whole point of the guardrail.
PINNED_EMITTABLE = frozenset(
    {
        "start",
        "connectoraction",
        "message",
        "map",
        "documentproperties",
        "stop",
        "catcherrors",
        "notify",
        "doccacheload",
        "processcall",
        # M10.2 (issue #106): process-level Data Process Custom Scripting shape.
        "dataprocess",
        # M10.3 (issue #107): process-level Return Documents terminal shape.
        "returndocuments",
        # M10.4 (issue #108): deliberate Exception (Throw) catch-leg terminal.
        "exception",
        # M10.8 (issue #112): Branch (N-way forward fan-out) shape — its own
        # emitter (_emit_branch / _emit_branch_shapes), neither a _emit_flow_shape
        # dispatch kind nor a catch-path shape.
        "branch",
        # M10.9 (issue #113): Decision (conditional two-path routing) shape — its
        # own emitter (_emit_decision / _emit_decision_shapes); a Decision carries
        # two labelled (true/false) edges, so it is NOT a _emit_flow_shape dispatch
        # kind.
        "decision",
        # M10.5 (issue #109): process-level Document Cache Retrieve shape — the
        # doccacheretrieve transform mode / _emit_flow_shape dispatch kind.
        "doccacheretrieve",
        # M10.6 (issue #110): process-level Document Cache Remove shape — the
        # doccacheremove transform mode / _emit_flow_shape dispatch kind.
        "doccacheremove",
        # M10.7 (issue #111): process-level Flow Control (per-document batching)
        # shape — the flow_control config block / _emit_flow_shape dispatch kind.
        "flowcontrol",
    }
)

# Minimal valid params per flow-dispatch ``kind`` (verified against each emitter:
# ``_emit_connectoraction`` uses ``params[...]`` direct access, so all four
# connector keys are mandatory; the rest read via ``params.get(...)``).
_FLOW_PARAMS = {
    "start_noaction": {},
    "connectoraction_source": {
        "connector_type": "database",
        "action_type": "Get",
        "connection_id": "CONN-1",
        "operation_id": "OP-1",
    },
    "message": {"text": "hello"},
    "map": {"map_id": "MAP-1"},
    "dataprocess": {
        "steps": [{"operation": "custom_scripting", "script": "dataContext.storeStream(is, props);"}],
    },
    # M10.5 (issue #109): the Document Cache Retrieve shape needs only the required
    # document_cache_id (empty_cache_behavior / load_all_documents default).
    "doccacheretrieve": {"document_cache_id": "CACHE-1"},
    # M10.6 (issue #110): the Document Cache Remove shape needs only the required
    # document_cache_id (remove_all_documents defaults to True).
    "doccacheremove": {"document_cache_id": "CACHE-1"},
    # M10.7 (issue #111): the Flow Control shape needs only a positive
    # for_each_count (the batch size); the userlabel reads via params.get(...).
    "flowcontrol": {"for_each_count": 10},
    # M10.3 (issue #107): the Return Documents terminal reads only an optional
    # label via params.get(...), so empty params emit a valid (unlabeled) shape.
    "returndocuments": {},
    "setproperties": {
        "ddp_name": "path",
        "request_profile_id": "PROF-1",
        "segments": [{"type": "static", "value": "/v1"}],
    },
    "processcall": {"process_id": "PROC-1"},
    "stop": {"continue_": True},
}


def _flow_dispatch_kinds():
    """Return the literal ``kind`` dispatch keys in ``_emit_flow_shape``.

    Parses the function's own source with ``ast`` and collects every literal in a
    ``kind == "..."`` or ``kind in (...)``/``[...]`` comparison. This keys off the
    real dispatch ladder, not emitter method names.
    """
    source = textwrap.dedent(inspect.getsource(pfb._emit_flow_shape))
    tree = ast.parse(source)
    kinds = set()
    for node in ast.walk(tree):
        if not (
            isinstance(node, ast.Compare)
            and isinstance(node.left, ast.Name)
            and node.left.id == "kind"
        ):
            continue
        for op, comparator in zip(node.ops, node.comparators):
            if isinstance(op, ast.Eq) and isinstance(comparator, ast.Constant):
                if isinstance(comparator.value, str):
                    kinds.add(comparator.value)
            elif isinstance(op, ast.In) and isinstance(
                comparator, (ast.Tuple, ast.List, ast.Set)
            ):
                for elt in comparator.elts:
                    if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                        kinds.add(elt.value)
    return kinds


_CONN_PARAMS = {
    "connector_type": "database",
    "action_type": "Get",
    "connection_id": "CONN-1",
    "operation_id": "OP-1",
}


def _catch_flow():
    """A minimal linear flow whose ``flow[0]`` is the start (the wrapper slices
    ``flow[1:]`` for the normal Try chain)."""
    return [
        ("start_noaction", {}),
        ("connectoraction_source", dict(_CONN_PARAMS)),
        ("connectoraction_target", dict(_CONN_PARAMS)),
        ("stop", {"continue_": True}),
    ]


def _shapetypes_from_parts(parts):
    root = ET.fromstring("<shapes>" + "".join(parts) + "</shapes>")
    return {shape.attrib["shapetype"] for shape in root}


def _emit_full_catch_shapetypes():
    """Drive the real Try/Catch emitter (notify + DLQ + Exception throw) and return
    all shapetypes produced — covers the catch-path-only shapes
    catcherrors/notify/doccacheload AND the M10.4 exception terminal (issue #108)."""
    parts = pfb._emit_try_catch_shapes(
        _catch_flow(),
        {"mode": "document_cache_ref", "document_cache_id": "CACHE-1"},
        retry_count=0,
        catch_notify={
            "level": "ERROR",
            "message_template": "failed: " + pfb._NOTIFY_CAUGHT_ERROR_TOKEN,
        },
        catch_exception={
            "title": "Halt",
            "message_template": "halting: {1}",
            "parameter_source": "caught_error",
        },
    )
    return _shapetypes_from_parts(parts)


def _emit_branch_shapetypes():
    """Drive the real Branch fan-out emitter and return all shapetypes produced.

    Branch (issue #112 M10.8) is neither a ``_emit_flow_shape`` dispatch kind nor a
    catch-path shape: it has its own ``_emit_branch`` / ``_emit_branch_shapes``
    emitter (an N-way fan-out carries N labelled edges, not one). This is the third
    emission category the registry guardrail must recognize.
    """
    pre_branch = [
        ("start_noaction", {}),
        ("connectoraction_source", dict(_CONN_PARAMS)),
    ]
    legs = [
        [("connectoraction_target", dict(_CONN_PARAMS)), ("stop", {"continue_": True})],
        [("connectoraction_target", dict(_CONN_PARAMS)), ("stop", {"continue_": True})],
    ]
    parts = pfb._emit_branch_shapes(pre_branch, legs)
    return _shapetypes_from_parts(parts)


def _emit_decision_shapetypes():
    """Drive the real Decision emitter and return all shapetypes produced.

    Decision (issue #113 M10.9) is neither a ``_emit_flow_shape`` dispatch kind nor
    a catch-path shape: it has its own ``_emit_decision`` / ``_emit_decision_shapes``
    emitter (a Decision carries two labelled true/false edges). This is the same
    non-dispatch emission category as Branch.
    """
    pre_decision = [
        ("start_noaction", {}),
        ("connectoraction_source", dict(_CONN_PARAMS)),
    ]
    decision_config = {
        "comparison": "equals",
        "label": "Check",
        "left": {"value_type": "track", "property_id": "dynamicdocument.DDP_X"},
        "right": {"value_type": "static", "static_value": "y"},
    }
    true_leg = [("connectoraction_target", dict(_CONN_PARAMS)), ("stop", {"continue_": True})]
    false_leg = [("message", {"text": "rejected"}), ("stop", {"continue_": True})]
    parts = pfb._emit_decision_shapes(pre_decision, decision_config, true_leg, false_leg, None)
    return _shapetypes_from_parts(parts)


def test_registry_entry_shape():
    """Every registry value is exactly ``{"emittable": bool, "emitter_kind": str}``."""
    assert EMITTABLE_SHAPE_REGISTRY, "registry must not be empty"
    for shapetype, entry in EMITTABLE_SHAPE_REGISTRY.items():
        assert isinstance(shapetype, str) and shapetype, shapetype
        assert set(entry) == {"emittable", "emitter_kind"}, (shapetype, entry)
        assert isinstance(entry["emittable"], bool), shapetype
        assert isinstance(entry["emitter_kind"], str) and entry["emitter_kind"], shapetype


def test_pinned_emittable_set():
    """The shipped emittable shapetype set is exactly the M10.1 pin (issue #105)."""
    emittable = {k for k, v in EMITTABLE_SHAPE_REGISTRY.items() if v["emittable"]}
    assert emittable == set(PINNED_EMITTABLE)


def test_flow_dispatch_ladder_keys():
    """The extracted ``_emit_flow_shape`` dispatch keys are exactly the known set,
    including BOTH connectoraction directions (issue #105 'assert ... has a
    dispatch branch')."""
    assert _flow_dispatch_kinds() == {
        "start_noaction",
        "connectoraction_source",
        "connectoraction_target",
        "message",
        "map",
        "flowcontrol",
        "dataprocess",
        "doccacheretrieve",
        "doccacheremove",
        "returndocuments",
        "setproperties",
        # Issue #121 M11.2: the generic set_ddp/set_dpp step dispatch key. It
        # emits the SAME documentproperties shapetype as "setproperties" (the
        # dynamic-path adapter), so PINNED_EMITTABLE is unchanged.
        "setproperties_step",
        "processcall",
        "stop",
    }


def test_every_emittable_entry_is_backed_by_a_real_emitter():
    """Core guardrail: each emittable registry entry maps to a real emission path.

    Flow-dispatched entries are emitted through ``_emit_flow_shape`` and the
    produced ``shapetype`` must equal the registry key (proves the dispatch
    branch, not a name coincidence). Catch-path-only entries are proven by the
    real Try/Catch emission producing their shapetype; the Branch fan-out (issue
    #112) is proven by the real ``_emit_branch_shapes`` emission.
    """
    dispatch = _flow_dispatch_kinds()
    # Non-dispatch emission categories: catch-path-only shapes (catcherrors /
    # notify / doccacheload / exception), the Branch fan-out shape, and the
    # Decision two-path shape.
    non_dispatch_shapetypes = (
        _emit_full_catch_shapetypes()
        | _emit_branch_shapetypes()
        | _emit_decision_shapetypes()
    )
    for shapetype, entry in EMITTABLE_SHAPE_REGISTRY.items():
        if not entry["emittable"]:
            continue
        emitter_kind = entry["emitter_kind"]
        if emitter_kind in dispatch:
            params = _FLOW_PARAMS[emitter_kind]
            xml = pfb._emit_flow_shape(emitter_kind, params, "shape1", "shape2", 1)
            produced = ET.fromstring(xml).attrib["shapetype"]
            assert produced == shapetype, (
                f"registry shapetype {shapetype!r} (emitter_kind {emitter_kind!r}) "
                f"emitted shapetype {produced!r}"
            )
        else:
            assert shapetype in non_dispatch_shapetypes, (
                f"{shapetype!r} marked emittable but no real emitter produces it "
                f"(emitter_kind {emitter_kind!r})"
            )


def test_catch_path_shapes_match_registry():
    """catcherrors/notify/doccacheload are emitted by the real catch leg, and for
    these the registry ``emitter_kind`` equals the emitted shapetype token."""
    catch_shapetypes = _emit_full_catch_shapetypes()
    # Issue #108 M10.4: ``exception`` joins the catch-path-only shapes — it is the
    # catch-leg terminal throw, not a _emit_flow_shape dispatch kind.
    for shapetype in ("catcherrors", "notify", "doccacheload", "exception"):
        assert shapetype in catch_shapetypes, shapetype
        entry = EMITTABLE_SHAPE_REGISTRY[shapetype]
        assert entry["emittable"] is True
        assert entry["emitter_kind"] == shapetype


def test_branch_shape_matches_registry():
    """Issue #112 M10.8: the Branch fan-out is emitted by its own
    ``_emit_branch_shapes`` (NOT a ``_emit_flow_shape`` dispatch kind — a Branch
    carries N labelled edges). Its registry entry is consistent and the emitter
    really produces a ``shapetype="branch"`` shape."""
    branch_shapetypes = _emit_branch_shapetypes()
    assert "branch" in branch_shapetypes
    entry = EMITTABLE_SHAPE_REGISTRY["branch"]
    assert entry["emittable"] is True
    assert entry["emitter_kind"] == "branch"
    # Branch is deliberately NOT a single-edge flow-dispatch kind.
    assert "branch" not in _flow_dispatch_kinds()


def test_decision_shape_matches_registry():
    """Issue #113 M10.9: the Decision two-path router is emitted by its own
    ``_emit_decision_shapes`` (NOT a ``_emit_flow_shape`` dispatch kind — a Decision
    carries two labelled true/false edges). Its registry entry is consistent and the
    emitter really produces a ``shapetype="decision"`` shape."""
    decision_shapetypes = _emit_decision_shapetypes()
    assert "decision" in decision_shapetypes
    entry = EMITTABLE_SHAPE_REGISTRY["decision"]
    assert entry["emittable"] is True
    assert entry["emitter_kind"] == "decision"
    # Decision is deliberately NOT a single-edge flow-dispatch kind.
    assert "decision" not in _flow_dispatch_kinds()


def test_supported_transform_modes_are_dispatch_backed():
    """Every supported transform mode maps to a dispatch-backed shape, while
    ``passthrough`` intentionally emits no transform shape (issue #105 optional)."""
    assert pfb._SUPPORTED_TRANSFORM_MODES == frozenset(
        {"passthrough", "message", "map_ref", "dataprocess", "doccacheretrieve", "doccacheremove"}
    )
    dispatch = _flow_dispatch_kinds()
    # message mode -> the "message" shape; map_ref mode -> the "map" shape;
    # dataprocess mode -> the "dataprocess" shape (issue #106 M10.2);
    # doccacheretrieve mode -> the "doccacheretrieve" shape (issue #109 M10.5);
    # doccacheremove mode -> the "doccacheremove" shape (issue #110 M10.6).
    assert EMITTABLE_SHAPE_REGISTRY["message"]["emitter_kind"] in dispatch
    assert EMITTABLE_SHAPE_REGISTRY["map"]["emitter_kind"] in dispatch
    assert EMITTABLE_SHAPE_REGISTRY["dataprocess"]["emitter_kind"] in dispatch
    assert EMITTABLE_SHAPE_REGISTRY["doccacheretrieve"]["emitter_kind"] in dispatch
    assert EMITTABLE_SHAPE_REGISTRY["doccacheremove"]["emitter_kind"] in dispatch
    # passthrough has no dedicated transform shape/kind.
    assert "passthrough" not in dispatch


def test_process_flow_builder_kinds_registered():
    """The process-kind registry holds exactly the shipped process kinds.

    sync_pipeline (issue #70 M5.2) is a verified-linear PipelineSpec lowering
    layer that delegates XML emission to ProcessFlowBuilder — it adds a process
    KIND but no new emittable SHAPE (PINNED_EMITTABLE below is unchanged).
    """
    assert set(PROCESS_FLOW_BUILDERS) == {
        "database_to_api_sync",
        "wrapper_subprocess",
        "sync_pipeline",
    }


def test_registry_not_served_in_doctrine_catalog():
    """The registry is internal structure, not served prose — keeping it out of
    the served catalog is what keeps the doctrine token-lint green."""
    catalog = get_design_doctrine_catalog()
    assert "emittable_shape_registry" not in catalog
    blob = repr(catalog)
    # The registry's mechanic tokens must not have leaked into the served surface.
    assert "emitter_kind" not in blob
