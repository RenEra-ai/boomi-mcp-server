"""Issue #154 (M12.16) — the six grammar widenings, emitted and pinned.

Per-case oracle provenance is recorded in
``tests/fixtures/process_ir/issue154/PROVENANCE.md`` and is deliberately NOT
uniform: item 3 has a real legacy differential, items 1/2/4 have legacy
placement evidence, and item 5 has none. The bytes here are regression pins, not
oracles — what stands behind them is that file plus the three checks every case
gets below (graph verification, the compiler's own invariants, determinism).

One case is RETIRED. ``catch_cache_put_exception`` (item 4) authored a catch body
that writes to a document cache and then raises. #184 amendment 3 measured that
Add to Cache hands on zero documents, so the platform skips the Exception after
it and the run reads COMPLETE with the caught error swallowed
(``cap184-cache-put-successor``). The document is now refused, its golden
(``golden-000066``) is tombstoned with its XML deleted, and the corpus no longer
registers it. Its parametrize id is kept — the node ids are registered — and each
test asserts the retirement truthfully, plus the replacement
``issue184:catch_exception_without_cache_put`` (``golden-000085``) where the
check still has a subject.
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
_MANIFEST = _HERE / "fixtures" / "wave_gate" / "goldens.jsonl"

#: Retired case -> (its tombstoned golden id, the replacement input case, the
#: replacement's golden id, the replacement's expected file). #184 amendment 3.
_RETIRED = {
    "catch_cache_put_exception": (
        "golden-000066",
        "issue184:catch_exception_without_cache_put",
        "golden-000085",
        "tests/fixtures/golden_xml/issue184_catch_exception_without_cache_put.xml",
    ),
}


def _render(case):
    rendered = corpus.render_golden_case("issue154:" + case, "process-xml-v1")
    return rendered if isinstance(rendered, bytes) else rendered.encode("utf-8")


def _render_input_case(input_case):
    rendered = corpus.render_golden_case(input_case, "process-xml-v1")
    return rendered if isinstance(rendered, bytes) else rendered.encode("utf-8")


def _manifest_rows():
    manifest = _MANIFEST.read_text().splitlines()
    return [json.loads(line) for line in manifest[1:]]


def _assert_retired_case_is_not_renderable(case):
    """The corpus no longer registers the retired case, so nothing renders it."""
    assert "issue154:" + case not in corpus.CASE_REGISTRY
    with pytest.raises(corpus.UnknownCase):
        _render(case)


def _shape_types(xml_text):
    return re.findall(r'shapetype="([a-zA-Z_]+)"', xml_text)


@pytest.mark.parametrize("case", _CASES)
def test_golden_bytes_are_exact(case):
    """RAW bytes. No canonicalisation, no re-parse — a comparison that
    normalised first could not see an ordering or whitespace regression.

    For the retired case: its golden file is GONE and the case no longer
    renders, and the replacement renders its own frozen file byte-for-byte.
    """
    retired = _RETIRED.get(case)
    if retired is None:
        assert _render(case) == (_GOLDENS / "issue154_{0}.xml".format(case)).read_bytes()
        return

    _tombstone_id, replacement_case, _replacement_id, replacement_file = retired
    assert not (_GOLDENS / "issue154_{0}.xml".format(case)).exists()
    _assert_retired_case_is_not_renderable(case)
    assert (
        _render_input_case(replacement_case)
        == (_HERE.parent / replacement_file).read_bytes()
    )


@pytest.mark.parametrize("case", _CASES)
def test_emitted_graph_verifies(case):
    """For the retired case, the subject is its replacement: the case itself no
    longer renders, and the replacement's emitted graph verifies clean."""
    retired = _RETIRED.get(case)
    if retired is None:
        report = verify_process_graph(_render(case).decode("utf-8"))
        assert not report.get("errors"), report
        return

    _assert_retired_case_is_not_renderable(case)
    rendered = _render_input_case(retired[1]).decode("utf-8")
    report = verify_process_graph(rendered)
    assert not report.get("errors"), report
    # Non-vacuity: the verifier really walked the replacement's shapes.
    assert report.get("shapes_checked") == len(_shape_types(rendered)), report


@pytest.mark.parametrize("case", _CASES)
def test_emission_is_deterministic(case):
    """For the retired case, the replacement's render is byte-stable and equals
    its frozen file (a render that was stable but wrong would satisfy the first
    comparison alone)."""
    retired = _RETIRED.get(case)
    if retired is None:
        assert _render(case) == _render(case)
        return

    _assert_retired_case_is_not_renderable(case)
    first = _render_input_case(retired[1])
    assert first == _render_input_case(retired[1])
    assert first == (_HERE.parent / retired[3]).read_bytes()


@pytest.mark.parametrize("case", _CASES)
def test_every_case_document_parses_and_is_the_shape_it_claims(case):
    """The input really is the widened shape — otherwise the golden could be
    green while testing something else entirely.

    For the retired case the document stays in the tree as the REFUSAL WITNESS:
    it is still the write-then-raise shape it claims (catch steps `[cache_put]`
    and an `exception` terminal), and it is refused at that cache node's
    `/cache_ref` with `PROCESS_IR_SCHEMA_INVALID_CARDINALITY`, because Add to
    Cache hands on zero documents and the Exception after it never runs.
    """
    from boomi_mcp.models.process_ir import (
        ProcessIRValidationError,
        parse_process_ir_v1,
    )

    doc = json.loads((_INPUTS / (case + ".json")).read_text(encoding="utf-8"))
    if case not in _RETIRED:
        ir = parse_process_ir_v1(doc)
        assert ir.body.steps
        return

    [handler] = doc["body"]["steps"]
    assert handler["kind"] == "try_catch"
    assert [s["kind"] for s in handler["catch_body"]["steps"]] == ["cache_put"]
    assert handler["catch_body"]["terminal"]["kind"] == "exception"

    with pytest.raises(ProcessIRValidationError) as excinfo:
        parse_process_ir_v1(doc)
    served = [(d.code, d.path) for d in excinfo.value.diagnostics]
    assert served == [
        (
            "PROCESS_IR_SCHEMA_INVALID_CARDINALITY",
            "/body/steps/0/catch_body/steps/0/cache_ref",
        )
    ], served

    # The control: the replacement keeps the protected flow and ends the catch
    # in the Exception alone, and it parses — so the refusal is about the write
    # before the Exception and nothing else.
    replacement = json.loads(
        (
            _HERE / "fixtures" / "process_ir" / "issue184"
            / "catch_exception_without_cache_put.json"
        ).read_text(encoding="utf-8")
    )
    assert (
        replacement["body"]["steps"][0]["try_body"] == handler["try_body"]
    ), "the replacement must keep the retired case's protected flow"
    ir = parse_process_ir_v1(replacement)
    assert list(ir.body.steps[0].catch_body.steps) == []
    assert ir.body.steps[0].catch_body.terminal.kind == "exception"


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
    """A golden nobody renders is a golden nobody checks.

    For the retired case: exactly one manifest row still names it, as a
    TOMBSTONE of `golden-000066` whose expected file is gone; the corpus no
    longer registers it; and its replacement is registered ACTIVE with a file
    that exists.
    """
    rows = _manifest_rows()
    matching = [r for r in rows if r["input_case"] == "issue154:" + case]
    assert len(matching) == 1, matching
    expected_file = "tests/fixtures/golden_xml/issue154_{0}.xml".format(case)
    assert matching[0]["expected_file"] == expected_file

    retired = _RETIRED.get(case)
    if retired is None:
        assert matching[0]["state"] == "active"
        return

    tombstone_id, replacement_case, replacement_id, replacement_file = retired
    assert matching[0]["state"] == "tombstone", matching
    assert matching[0]["id"] == tombstone_id, matching
    assert not (_HERE.parent / expected_file).exists()
    assert "issue154:" + case not in corpus.CASE_REGISTRY

    replacement = [r for r in rows if r["input_case"] == replacement_case]
    assert len(replacement) == 1, replacement
    assert replacement[0]["state"] == "active", replacement
    assert replacement[0]["id"] == replacement_id, replacement
    assert replacement[0]["expected_file"] == replacement_file, replacement
    assert (_HERE.parent / replacement_file).is_file()
    assert replacement_case in corpus.CASE_REGISTRY
