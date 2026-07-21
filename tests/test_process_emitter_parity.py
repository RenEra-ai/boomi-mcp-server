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

# Canonical Boomi component types per reference role — the registry validates
# these (unlike the compiler's sentinel-typed symbol fixtures).
_ROLE_TYPE = {
    "connection_ref": "connector-settings",
    "operation_ref": "connector-action",
    "map_ref": "transform.map",
    "cache_ref": "documentcache",
    "process_ref": "process",
}
_REF_FIELDS = {
    "connector": ("connection_ref", "operation_ref"),
    "map": ("map_ref",),
    "cache_put": ("cache_ref",),
    "cache_get": ("cache_ref",),
    "document_cache_retrieve": ("cache_ref",),
    "cache_remove": ("cache_ref",),
    "process_call": ("process_ref",),
}


def _canonical_symbols(cfg) -> SymbolTableV1:
    ref_type = {}
    for node in cfg.nodes:
        s = node.semantic
        for field in _REF_FIELDS.get(s.semantic_kind, ()):
            ref_type[getattr(s, field)] = _ROLE_TYPE[field]
        if s.semantic_kind == "data_process":
            for step in s.steps:
                if getattr(step, "profile_ref", None):
                    ref_type[step.profile_ref] = "profile.json"
        if s.semantic_kind == "set_property":
            for src in s.source_values:
                if getattr(src, "profile_ref", None):
                    ref_type[src.profile_ref] = "profile.json"
    symbols = []
    for ref in sorted(ref_type):
        binding = _BINDINGS.get(ref)
        symbols.append(
            ComponentSymbolV1(
                ref=ref,
                component_id=ref,
                component_type=ref_type[ref],
                connector_type=binding["connector_type"] if binding else None,
                action_type=binding["action_type"] if binding else None,
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
    symbols = _canonical_symbols(cfg)
    plan = lowering.lower_cfg_to_emission_plan(cfg, symbols)
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
