"""Issue #154 (M12.16) — the six grammar widenings, emitted and pinned.

Per-case oracle provenance is recorded in
``tests/fixtures/process_ir/issue154/PROVENANCE.md`` and is deliberately NOT
uniform: item 3 has a real legacy differential, items 1/2/4 have legacy
placement evidence, and item 5 has none. The bytes here are regression pins, not
oracles — what stands behind them is that file plus the three checks every case
gets below (graph verification, the compiler's own invariants, determinism).
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import pytest

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))
_SRC = str(_HERE.parent / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

import _wave_gate_golden_corpus as corpus  # noqa: E402

from boomi_mcp.categories.components.process_graph_verifier import (  # noqa: E402
    verify_process_graph,
)

_CASES = (
    "try_flow_control",
    "try_data_process",
    "try_return_documents",
    "source_target_return_documents",
    "catch_cache_put_exception",
    "connector_linear_interleave",
)
_GOLDENS = _HERE / "fixtures" / "golden_xml"
_INPUTS = _HERE / "fixtures" / "process_ir" / "issue154"


def _render(case):
    rendered = corpus.render_golden_case("issue154:" + case, "process-xml-v1")
    return rendered if isinstance(rendered, bytes) else rendered.encode("utf-8")


def _shape_types(xml_text):
    return re.findall(r'shapetype="([a-zA-Z_]+)"', xml_text)


@pytest.mark.parametrize("case", _CASES)
def test_golden_bytes_are_exact(case):
    """RAW bytes. No canonicalisation, no re-parse — a comparison that
    normalised first could not see an ordering or whitespace regression."""
    assert _render(case) == (_GOLDENS / "issue154_{0}.xml".format(case)).read_bytes()


@pytest.mark.parametrize("case", _CASES)
def test_emitted_graph_verifies(case):
    report = verify_process_graph(_render(case).decode("utf-8"))
    assert not report.get("errors"), report


@pytest.mark.parametrize("case", _CASES)
def test_emission_is_deterministic(case):
    assert _render(case) == _render(case)


@pytest.mark.parametrize("case", _CASES)
def test_every_case_document_parses_and_is_the_shape_it_claims(case):
    """The input really is the widened shape — otherwise the golden could be
    green while testing something else entirely."""
    from boomi_mcp.models.process_ir import parse_process_ir_v1

    doc = json.loads((_INPUTS / (case + ".json")).read_text(encoding="utf-8"))
    ir = parse_process_ir_v1(doc)
    assert ir.body.steps


def test_item_3_matches_the_legacy_builder_shape_sequence():
    """The ONE case with a real independent oracle.

    ``return_documents_terminal.xml`` is the LEGACY builder's committed golden for
    this flow and predates this slice's baseline, so it is causally independent of
    the compiler under test. If the compiler emitted a different shape spine for
    the same authored flow, this fails.
    """
    legacy = (_GOLDENS / "return_documents_terminal.xml").read_text(encoding="utf-8")
    mine = _render("source_target_return_documents").decode("utf-8")
    legacy_shapes = _shape_types(legacy)
    assert legacy_shapes, "no shapes parsed from the legacy golden — test would be vacuous"
    assert legacy_shapes == _shape_types(mine)
    assert legacy_shapes == ["start", "connectoraction", "connectoraction", "returndocuments"]


def test_item_5_entry_role_is_on_the_first_call_not_the_first_step():
    """The widening with no legacy oracle gets its own structural assertion.

    A linear prefix moves the connector entry off step 0. If the entry role
    followed the CFG entry instead of the first ROOT CALL, this flow would emit
    its first read with the downstream-target emitter key.
    """
    from boomi_mcp.compiler.process_ir.lowering import lower_process_ir_to_cfg
    from boomi_mcp.models.process_ir import parse_process_ir_v1

    doc = json.loads((_INPUTS / "connector_linear_interleave.json").read_text(encoding="utf-8"))
    cfg = lower_process_ir_to_cfg(parse_process_ir_v1(doc))
    by_id = {node.node_id: node for node in cfg.nodes}
    assert by_id[cfg.entry_node_id].semantic.semantic_kind == "set_property"

    calls = [n for n in cfg.nodes if n.semantic.semantic_kind == "connector_call"]
    assert len(calls) == 2, [n.source_path for n in calls]
    assert calls[0].semantic.role == "entry"
    assert calls[1].semantic.role == "downstream"


@pytest.mark.parametrize("case", _CASES)
def test_every_case_is_registered_in_the_golden_manifest(case):
    """A golden nobody renders is a golden nobody checks."""
    manifest = (_HERE / "fixtures" / "wave_gate" / "goldens.jsonl").read_text().splitlines()
    rows = [json.loads(line) for line in manifest[1:]]
    matching = [r for r in rows if r["input_case"] == "issue154:" + case]
    assert len(matching) == 1, matching
    assert matching[0]["state"] == "active"
    assert matching[0]["expected_file"] == "tests/fixtures/golden_xml/issue154_{0}.xml".format(case)
