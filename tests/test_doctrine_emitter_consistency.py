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
    """Drive the real Try/Catch emitter (with notify) and return all shapetypes
    produced — covers the catch-path-only shapes catcherrors/notify/doccacheload."""
    parts = pfb._emit_try_catch_shapes(
        _catch_flow(),
        {"mode": "document_cache_ref", "document_cache_id": "CACHE-1"},
        retry_count=0,
        catch_notify={
            "level": "ERROR",
            "message_template": "failed: " + pfb._NOTIFY_CAUGHT_ERROR_TOKEN,
        },
    )
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
        "dataprocess",
        "returndocuments",
        "setproperties",
        "processcall",
        "stop",
    }


def test_every_emittable_entry_is_backed_by_a_real_emitter():
    """Core guardrail: each emittable registry entry maps to a real emission path.

    Flow-dispatched entries are emitted through ``_emit_flow_shape`` and the
    produced ``shapetype`` must equal the registry key (proves the dispatch
    branch, not a name coincidence). Catch-path-only entries are proven by the
    real Try/Catch emission producing their shapetype.
    """
    dispatch = _flow_dispatch_kinds()
    catch_shapetypes = _emit_full_catch_shapetypes()
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
            assert shapetype in catch_shapetypes, (
                f"{shapetype!r} marked emittable but no real emitter produces it "
                f"(emitter_kind {emitter_kind!r})"
            )


def test_catch_path_shapes_match_registry():
    """catcherrors/notify/doccacheload are emitted by the real catch leg, and for
    these the registry ``emitter_kind`` equals the emitted shapetype token."""
    catch_shapetypes = _emit_full_catch_shapetypes()
    for shapetype in ("catcherrors", "notify", "doccacheload"):
        assert shapetype in catch_shapetypes, shapetype
        entry = EMITTABLE_SHAPE_REGISTRY[shapetype]
        assert entry["emittable"] is True
        assert entry["emitter_kind"] == shapetype


def test_supported_transform_modes_are_dispatch_backed():
    """Every supported transform mode maps to a dispatch-backed shape, while
    ``passthrough`` intentionally emits no transform shape (issue #105 optional)."""
    assert pfb._SUPPORTED_TRANSFORM_MODES == frozenset(
        {"passthrough", "message", "map_ref", "dataprocess"}
    )
    dispatch = _flow_dispatch_kinds()
    # message mode -> the "message" shape; map_ref mode -> the "map" shape;
    # dataprocess mode -> the "dataprocess" shape (issue #106 M10.2).
    assert EMITTABLE_SHAPE_REGISTRY["message"]["emitter_kind"] in dispatch
    assert EMITTABLE_SHAPE_REGISTRY["map"]["emitter_kind"] in dispatch
    assert EMITTABLE_SHAPE_REGISTRY["dataprocess"]["emitter_kind"] in dispatch
    # passthrough has no dedicated transform shape/kind.
    assert "passthrough" not in dispatch


def test_process_flow_builder_kinds_registered():
    """The process-kind registry holds exactly the two shipped process kinds."""
    assert set(PROCESS_FLOW_BUILDERS) == {"database_to_api_sync", "wrapper_subprocess"}


def test_registry_not_served_in_doctrine_catalog():
    """The registry is internal structure, not served prose — keeping it out of
    the served catalog is what keeps the doctrine token-lint green."""
    catalog = get_design_doctrine_catalog()
    assert "emittable_shape_registry" not in catalog
    blob = repr(catalog)
    # The registry's mechanic tokens must not have leaked into the served surface.
    assert "emitter_kind" not in blob
