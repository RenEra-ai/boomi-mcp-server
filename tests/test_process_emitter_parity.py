"""Byte-parity tests for the ProcessIR process-emitter registry (issue #138 M12.3).

The hard acceptance gate: for every golden IR document, the typed registry's
``emit_process`` produces process XML byte-identical to the UNCHANGED legacy
builder's ``<shapes>`` content — and to a committed pre-extraction fixture, so
"registry equals the current builder" is never the ONLY oracle. The registry
verifier result must also match ``verify_process_graph`` on the legacy XML.

The three golden IR documents collectively exercise all 17 registry emitter
kinds (start, both connector roles, message, map, flowcontrol, dataprocess,
doccache load/retrieve/remove, set-properties, processcall, branch, decision,
exception, stop, returndocuments), so this is full-vocabulary byte parity.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
if str(_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_ROOT / "src"))

from boomi_mcp.categories.components.builders.process_flow_builder import (
    ProcessFlowBuilder,
    WrapperSubprocessBuilder,
)
from boomi_mcp.categories.components.process_graph_verifier import verify_process_graph
from boomi_mcp.compiler.process_ir import lowering
from boomi_mcp.compiler.process_ir.contracts import ComponentSymbolV1, SymbolTableV1
from boomi_mcp.compiler.process_ir.emitter_registry import emit_process
from boomi_mcp.models._process_ir_compat import (
    ConnectorBindingV1,
    ConnectorResolutionContextV1,
    ir_to_legacy_flow_sequence,
)
from boomi_mcp.models.process_ir import parse_process_ir_v1

_FIXTURES = _ROOT / "tests" / "fixtures" / "process_ir"
_PARITY_DIR = _FIXTURES / "emitter_parity"
GOLDEN_DOCS = json.loads((_FIXTURES / "process_ir_v1.json").read_text())
_SHARED = json.loads((_FIXTURES / "flow_sequence_compat_cases.json").read_text())["shared"]
_BINDINGS = _SHARED["bindings"]

# A Data Process step declares its profile KIND (json/xml); the registry requires
# the matching Boomi component type on the resolved symbol.
_DP_PROFILE_COMPONENT_TYPE = {"json": "profile.json", "xml": "profile.xml"}


def _sentinel_symbols(cfg) -> SymbolTableV1:
    """A symbol per authored ref, typed only enough for lowering (which resolves
    ref -> component_id and ignores the type). Component id == the ref token."""
    refs = set()
    for node in cfg.nodes:
        s = node.semantic
        for field in ("connection_ref", "operation_ref", "map_ref", "cache_ref", "process_ref"):
            if getattr(s, field, None):
                refs.add(getattr(s, field))
        for step in getattr(s, "steps", ()):
            if getattr(step, "profile_ref", None):
                refs.add(step.profile_ref)
        for src in getattr(s, "source_values", ()):
            if getattr(src, "profile_ref", None):
                refs.add(src.profile_ref)
    symbols = []
    for ref in sorted(refs):
        b = _BINDINGS.get(ref)
        symbols.append(
            ComponentSymbolV1(
                ref=ref,
                component_id=ref,
                component_type="sentinel",
                connector_type=b["connector_type"] if b else None,
                action_type=b["action_type"] if b else None,
            )
        )
    return SymbolTableV1(symbols=tuple(symbols))


def _symbols_from_plan(plan) -> SymbolTableV1:
    """The registry-canonical symbol table, typed from the plan's RESOLVED emitter
    inputs — exactly the component types the registry requirement check validates
    (component id == the resolved id, which equals the authored ref token)."""
    id_type = {}
    for node in plan.nodes:
        e = node.emitter_input
        k = e.emitter_kind
        if k in ("connectoraction_source", "connectoraction_target"):
            id_type[e.connection_id] = "connector-settings"
            id_type[e.operation_id] = "connector-action"
        elif k == "map":
            id_type[e.map_id] = "transform.map"
        elif k in ("doccacheload", "doccacheretrieve", "doccacheremove"):
            id_type[e.document_cache_id] = "documentcache"
        elif k == "processcall":
            id_type[e.process_id] = "process"
        elif k == "dataprocess":
            for st in e.steps:
                pid = getattr(st, "profile_id", "")
                if pid:
                    kind = str(getattr(st, "profile_type", "")).strip().lower()
                    id_type[pid] = _DP_PROFILE_COMPONENT_TYPE.get(kind, "profile.json")
        elif k == "setproperties_step":
            for src in e.source_values:
                if src.value_type == "profile":
                    id_type[src.profile_id] = src.profile_type
    symbols = []
    for cid in sorted(id_type):
        b = _BINDINGS.get(cid)
        symbols.append(
            ComponentSymbolV1(
                ref=cid,
                component_id=cid,
                component_type=id_type[cid],
                connector_type=b["connector_type"] if b else None,
                action_type=b["action_type"] if b else None,
            )
        )
    return SymbolTableV1(symbols=tuple(symbols))


def _context():
    return ConnectorResolutionContextV1(
        operation_bindings={ref: ConnectorBindingV1(**b) for ref, b in _BINDINGS.items()},
        fallback_target=_SHARED["target"],
    )


def _legacy_shapes_inner(process_xml: str) -> str:
    return re.search(r"<shapes>(.*)</shapes>", process_xml, re.DOTALL).group(1)


def _build_legacy(config, name="ParityProcess"):
    builder = (
        WrapperSubprocessBuilder
        if config.get("process_kind") == "wrapper_subprocess"
        else ProcessFlowBuilder
    )
    return builder.build(config, name=name, folder_name="ParityFolder")


def _emit(doc_name):
    ir = parse_process_ir_v1(GOLDEN_DOCS[doc_name])
    cfg = lowering.lower_process_ir_to_cfg(ir)
    plan = lowering.lower_cfg_to_emission_plan(cfg, _sentinel_symbols(cfg))
    symbols = _symbols_from_plan(plan)
    return ir, emit_process(plan, symbols)


@pytest.mark.parametrize("doc_name", sorted(GOLDEN_DOCS))
def test_registry_shapes_match_legacy_builder_bytes(doc_name):
    """Registry shape XML == the legacy builder's <shapes>, byte-for-byte."""
    ir, artifact = _emit(doc_name)
    legacy_config = ir_to_legacy_flow_sequence(ir, _context())
    legacy_inner = _legacy_shapes_inner(_build_legacy(legacy_config))
    assert "".join(artifact.shape_xml_parts) == legacy_inner


@pytest.mark.parametrize("doc_name", sorted(GOLDEN_DOCS))
def test_registry_process_xml_matches_committed_fixture(doc_name):
    """Independent byte anchor: not just "registry == current builder"."""
    _ir, artifact = _emit(doc_name)
    fixture = (_PARITY_DIR / f"{doc_name}.process.xml").read_text()
    assert artifact.process_xml == fixture


@pytest.mark.parametrize("doc_name", sorted(GOLDEN_DOCS))
def test_registry_emission_is_deterministic(doc_name):
    _ir, a = _emit(doc_name)
    _ir2, b = _emit(doc_name)
    assert a.process_xml == b.process_xml
    assert a.shape_xml_parts == b.shape_xml_parts


@pytest.mark.parametrize("doc_name", sorted(GOLDEN_DOCS))
def test_registry_verifier_matches_legacy_verifier(doc_name):
    """The registry's verifier summary equals verify_process_graph on legacy XML."""
    ir, artifact = _emit(doc_name)
    legacy_config = ir_to_legacy_flow_sequence(ir, _context())
    legacy = verify_process_graph(_build_legacy(legacy_config))
    assert artifact.verifier.errors == ()
    assert [dict(code=e["code"], shape=e["shape"]) for e in legacy["errors"]] == []
    assert artifact.verifier.shapes_checked == legacy["shapes_checked"]
    assert len(artifact.verifier.warnings) == len(legacy["warnings"])
