"""#184 A5: an ordinary document-property read after a stream-replacing step.

Before #184 the general lineage lattice was deliberately NOT invalidated at a step
that replaces the document stream (`lineage.py`, the #155 decision): only a
property that became a BOUND request path was refused when read after a cache
retrieval. So `[GET, set_ddp X, cache_put, cache_get, set_dpp Y <- ddp X, PATCH,
stop]` compiled, although the documents reaching the reader never carried X.

The kinds that invalidate are read from `PROPERTY_SURVIVAL_V1` — the MEASURED
table — never listed here: every test that needs "a replacing step" enumerates
the table, and a table row with no node factory below fails rather than being
silently skipped.

Expected codes and pointers come from the issue's own acceptance text (A5) and
from the branch-point measurement archived at
`docs/architecture/evidence/issue-184/predicates/probe_cbab28f.jsonl`, where the
same shape compiled — never from this implementation's output.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
for _p in (str(_ROOT), str(_ROOT / "src")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from boomi_mcp.compiler.process_ir import connector_capabilities as CC  # noqa: E402
from boomi_mcp.compiler.process_ir.contracts import (  # noqa: E402
    ComponentSymbolV1,
    SymbolTableV1,
)
from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError  # noqa: E402
from boomi_mcp.compiler.process_ir.pipeline import compile_process_ir_v1  # noqa: E402
from boomi_mcp.compiler.process_ir.semantic_validation import lineage  # noqa: E402
from boomi_mcp.errors import (  # noqa: E402
    PROCESS_IR_SEMANTIC_LINEAGE_DDP_SCOPE_INVALID,
    PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE,
)
from boomi_mcp.models.process_ir import parse_process_ir_v1  # noqa: E402


def _symbols():
    rest = CC.REST_FAMILY
    return SymbolTableV1(symbols=(
        ComponentSymbolV1(ref="$ref:RCONN", component_id="RCONN",
                          component_type="connector-settings", connector_type=rest),
        ComponentSymbolV1(ref="$ref:GET", component_id="GETOP",
                          component_type="connector-action", connector_type=rest,
                          action_type="GET", connection_ref="$ref:RCONN"),
        ComponentSymbolV1(ref="$ref:PATCH", component_id="PATCHOP",
                          component_type="connector-action", connector_type=rest,
                          action_type="PATCH", connection_ref="$ref:RCONN"),
        ComponentSymbolV1(ref="$ref:PROF", component_id="PROF",
                          component_type="profile.json"),
        ComponentSymbolV1(ref="$ref:CACHE", component_id="CACHE",
                          component_type="documentcache"),
    ))


_GET = {"kind": "connector_call", "operation_ref": "$ref:GET"}
_PATCH = {"kind": "connector_call", "operation_ref": "$ref:PATCH"}
_STOP = {"kind": "stop"}
_PUT = {"kind": "cache_put", "cache_ref": "$ref:CACHE"}
_WRITE_X = {"kind": "set_ddp", "name": "X",
            "source_values": [{"value_type": "static", "value": "v"}]}


def _read_x(default=None):
    source = {"value_type": "ddp", "property_name": "X"}
    if default is not None:
        source["default_value"] = default
    return {"kind": "set_dpp", "name": "Y", "source_values": [source]}


def _profile_step(operation):
    return {"operation": operation, "profile_type": "json", "profile_ref": "$ref:PROF",
            "link_element_key": "1", "link_element_name": "root"}


#: `(semantic kind, step operation)` -> the authored node(s) that realise it, placed
#: between the writer and the reader. A cache retrieval needs a writer of its own,
#: so its factory returns the put and the read together.
_REPLACING_NODE_FACTORIES = {
    ("message", None): lambda: [{"kind": "message", "text": "hello"}],
    ("cache_get", None): lambda: [_PUT, {"kind": "cache_get", "cache_ref": "$ref:CACHE"}],
    ("document_cache_retrieve", None): lambda: [
        _PUT, {"kind": "document_cache_retrieve", "cache_ref": "$ref:CACHE"}],
    ("data_process", "split_documents"): lambda: [
        {"kind": "data_process", "steps": [_profile_step("split_documents")]}],
    ("data_process", "combine_documents"): lambda: [
        {"kind": "data_process", "steps": [_profile_step("combine_documents")]}],
    ("data_process", "custom_scripting"): lambda: [
        {"kind": "data_process", "steps": [
            {"operation": "custom_scripting", "language": "groovy2",
             "script": "// emits its own documents"}]}],
}


def _cells(verdict_is_survives):
    return [
        pytest.param(cell, id="{0}.{1}".format(cell[0], cell[1]))
        for cell, verdict in sorted(
            lineage.PROPERTY_SURVIVAL_V1.items(), key=lambda item: (item[0][0], str(item[0][1]))
        )
        if (verdict == "survives") == verdict_is_survives
    ]


def _diagnostics(steps):
    doc = {"version": "1", "body": {"kind": "sequence", "steps": steps}}
    try:
        compile_process_ir_v1(parse_process_ir_v1(doc), _symbols())
    except ProcessIRCompileError as exc:
        return tuple((item.code, item.path) for item in exc.diagnostics)
    return ()


def _codes(steps):
    return {code for code, _path in _diagnostics(steps)}


def test_every_survival_table_row_has_a_node_factory():
    """The coverage claim is the table's own row set, in both directions."""
    assert set(_REPLACING_NODE_FACTORIES) == set(lineage.PROPERTY_SURVIVAL_V1), {
        "rows_without_a_factory": sorted(
            map(str, set(lineage.PROPERTY_SURVIVAL_V1) - set(_REPLACING_NODE_FACTORIES))),
        "factories_without_a_row": sorted(
            map(str, set(_REPLACING_NODE_FACTORIES) - set(lineage.PROPERTY_SURVIVAL_V1))),
    }
    assert _cells(True) and _cells(False), "a side of the table is empty — the tests would be vacuous"


def test_the_issue_probe_is_refused_as_a_read_before_write():
    """A5's own example, at the reader's pointer, under the read-before-write code.

    The branch point compiled this shape (archived probe row
    `stale_ddp_after_cacheget`). A first scratch patch that invalidated the lattice
    but kept the old classification reported the different-document-copy scope
    code for it, which blames sibling paths for a defect on this one — so the
    scope code is asserted ABSENT as well.
    """
    steps = [_GET, _WRITE_X, _PUT, {"kind": "cache_get", "cache_ref": "$ref:CACHE"},
             _read_x(), _PATCH, _STOP]
    diagnostics = _diagnostics(steps)
    assert (PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE, "/body/steps/4") in diagnostics, diagnostics
    assert PROCESS_IR_SEMANTIC_LINEAGE_DDP_SCOPE_INVALID not in {c for c, _ in diagnostics}, diagnostics


def test_the_branch_point_compiled_the_probe():
    """The 'before' half of the pair, read from the archived pre-baseline measurement."""
    probe = _ROOT / "docs/architecture/evidence/issue-184/predicates/probe_cbab28f.jsonl"
    rows = {json.loads(line)["form"]: json.loads(line) for line in probe.read_text().splitlines() if line}
    assert rows["stale_ddp_after_cacheget"].get("compile") == "ok+emit", rows["stale_ddp_after_cacheget"]


@pytest.mark.parametrize("cell", _cells(False))
def test_every_kind_the_table_does_not_mark_survives_invalidates_the_read(cell):
    between = _REPLACING_NODE_FACTORIES[cell]()
    steps = [_GET, _WRITE_X] + between + [_read_x(), _PATCH, _STOP]
    reader = "/body/steps/{0}".format(2 + len(between))
    diagnostics = _diagnostics(steps)
    assert (PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE, reader) in diagnostics, diagnostics


@pytest.mark.parametrize("cell", _cells(True))
def test_a_kind_the_table_marks_survives_keeps_the_value(cell):
    between = _REPLACING_NODE_FACTORIES[cell]()
    steps = [_GET, _WRITE_X] + between + [_read_x(), _PATCH, _STOP]
    assert PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE not in _codes(steps)


def test_a_defaulted_read_is_still_satisfied():
    steps = [_GET, _WRITE_X, _PUT, {"kind": "cache_get", "cache_ref": "$ref:CACHE"},
             _read_x(default="d"), _PATCH, _STOP]
    assert _diagnostics(steps) == ()


def test_a_rewrite_after_the_replacement_satisfies_the_read():
    steps = [_GET, _WRITE_X, _PUT, {"kind": "cache_get", "cache_ref": "$ref:CACHE"},
             _WRITE_X, _read_x(), _PATCH, _STOP]
    assert _diagnostics(steps) == ()


def test_a_sibling_leg_write_keeps_its_scope_diagnostic():
    """No replacement on the reader's path: the different-copy code still applies."""
    doc_steps = [{"kind": "branch", "legs": [
        {"steps": [_GET, _WRITE_X], "terminal": _STOP},
        {"steps": [_GET, _read_x()], "terminal": _STOP},
    ]}]
    diagnostics = _diagnostics(doc_steps)
    assert (PROCESS_IR_SEMANTIC_LINEAGE_DDP_SCOPE_INVALID, "/body/steps/0/legs/1/steps/1") in diagnostics, diagnostics


def test_the_lattice_drop_is_load_bearing(monkeypatch):
    """Non-vacuity: with the drop disabled, the probe compiles again."""
    steps = [_GET, _WRITE_X, _PUT, {"kind": "cache_get", "cache_ref": "$ref:CACHE"},
             _read_x(), _PATCH, _STOP]
    assert PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE in _codes(steps)
    monkeypatch.setattr(lineage, "_drop_replaced_document_keys", lambda state, keep: (state, frozenset()))
    assert PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE not in _codes(steps)
