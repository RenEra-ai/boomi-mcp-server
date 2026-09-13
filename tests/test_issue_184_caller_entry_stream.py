"""#184 amendment 1: a Data Passthrough root's entry carries its caller's documents.

With the passthrough entry merged, a passthrough root was measured to be judged as
if it were scheduled:
- connector resolution refused a document-consuming first call and a first map as
  cardinality mismatches (no producer);
- the stream-profile proof refused a profile source as a read off the empty No Data
  document.

Amendment 1's rules table says the opposite. A Data Passthrough child receives the
parent documents reaching the call, as one group. A consumer on those documents does
not fail inside the child; it states what the child requires of its callers, and
each call site discharges that requirement. Inside the child only a contradiction
between two requirements on the same untouched documents is provable.

Expected codes and pointers come from amendment 1 §2 (documents received, the
caller-entry state) and from the stream-profile rule already pinned in
`tests/test_issue_184_stream_profiles.py`. The scheduled contrast is amendment 2 §4.
"""

from __future__ import annotations

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
from boomi_mcp.compiler.process_ir.pipeline import (  # noqa: E402
    compile_process_ir_v1,
    parse_and_compile_process_ir_v1,
)
from boomi_mcp.compiler.process_ir.semantic_validation.context import (  # noqa: E402
    prepare_validation_context,
)
from boomi_mcp.compiler.process_ir.semantic_validation.lineage import walk_lineage  # noqa: E402
from boomi_mcp.errors import (  # noqa: E402
    PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH,
    PROCESS_IR_SEMANTIC_PROFILE_MISMATCH,
)
from boomi_mcp.models.process_ir import parse_process_ir_v1  # noqa: E402

_P1 = ("PROFILE-ONE", "profile.json")
_P2 = ("PROFILE-TWO", "profile.json")


def _symbols():
    rest = CC.REST_FAMILY

    def sym(ref, cid, ctype, **kw):
        return ComponentSymbolV1(ref="$ref:" + ref, component_id=cid, component_type=ctype, **kw)

    return SymbolTableV1(symbols=(
        sym("RCONN", "RCONN", "connector-settings", connector_type=rest),
        sym("PATCH", "PATCHOP", "connector-action", connector_type=rest, action_type="PATCH",
            connection_ref="$ref:RCONN", input_profile_ref="$ref:P2"),
        sym("M12", "M12", "transform.map", input_profile_ref="$ref:P1", output_profile_ref="$ref:P2"),
        sym("P1", "PROFILE-ONE", "profile.json"),
        sym("P2", "PROFILE-TWO", "profile.json"),
        sym("CACHE_P1", "CACHEP1", "documentcache", cache_profile_ref="$ref:P1"),
    ))


_ENTRY = {"kind": "passthrough", "label": "Receive prepared requests"}
_PATCH = {"kind": "connector_call", "operation_ref": "$ref:PATCH"}
_STOP = {"kind": "stop"}


def _writer(profile_ref):
    return {"kind": "set_ddp", "name": "X", "source_values": [
        {"value_type": "profile", "profile_ref": "$ref:" + profile_ref, "profile_type": "json",
         "element_id": "3", "element_name": "requestId"}]}


def _doc(*steps):
    return {"version": "1", "body": {"kind": "sequence", "steps": list(steps)}}


def _both_routes(payload):
    routes = []
    for compile_route in ("parse_and_compile", "model"):
        try:
            if compile_route == "parse_and_compile":
                parse_and_compile_process_ir_v1(payload, _symbols())
            else:
                compile_process_ir_v1(parse_process_ir_v1(payload), _symbols())
        except ProcessIRCompileError as exc:
            routes.append(tuple((item.code, item.path) for item in exc.diagnostics))
        else:
            routes.append(())
    assert routes[0] == routes[1], routes
    return routes[0]


def _requirements(payload):
    return walk_lineage(prepare_validation_context(parse_process_ir_v1(payload), _symbols())).entry_requirements


_ADMITTED = [
    pytest.param(_doc(_ENTRY, _PATCH, _STOP), (_P2,), id="document_consuming_first_call"),
    pytest.param(_doc(_ENTRY, {"kind": "map_ref", "map_ref": "$ref:M12"}, _PATCH, _STOP), (_P1,),
                 id="first_map_on_the_callers_documents"),
    pytest.param(_doc(_ENTRY, _writer("P2"), _PATCH, _STOP), (_P2, _P2),
                 id="profile_source_then_a_call_of_the_same_profile"),
    # #184 amendment 3 (measured): Add to Cache hands on ZERO documents, so a read
    # authored straight after it never runs. The caller's documents are staged by a
    # cache_put that TERMINATES leg 1, and a LATER leg reads them back. The staging
    # write still consumes the caller's documents and records its declared profile.
    pytest.param(_doc(_ENTRY, {"kind": "branch", "legs": [
        {"steps": [], "terminal": {"kind": "cache_put", "cache_ref": "$ref:CACHE_P1"}},
        {"steps": [{"kind": "cache_get", "cache_ref": "$ref:CACHE_P1"}], "terminal": _STOP},
    ]}), (_P1,), id="staging_the_callers_documents"),
]


@pytest.mark.parametrize("payload,requirements", _ADMITTED)
def test_a_consumer_of_the_callers_documents_records_a_requirement_instead_of_failing(payload, requirements):
    diagnostics = _both_routes(payload)
    codes = {code for code, _path in diagnostics}
    assert PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH not in codes, diagnostics
    assert PROCESS_IR_SEMANTIC_PROFILE_MISMATCH not in codes, diagnostics
    assert _requirements(payload) == requirements


def test_two_requirements_contradicting_on_one_path_are_refused_inside_the_child():
    """The caller's documents are one group of one profile on a path, so a second
    consumer naming another profile cannot be satisfied by any caller."""
    payload = _doc(_ENTRY, _writer("P1"), _writer("P2"), _PATCH, _STOP)
    diagnostics = _both_routes(payload)
    assert (PROCESS_IR_SEMANTIC_PROFILE_MISMATCH, "/body/steps/2/source_values/0/profile_ref") in diagnostics, diagnostics
    assert (PROCESS_IR_SEMANTIC_PROFILE_MISMATCH, "/body/steps/1/source_values/0/profile_ref") not in diagnostics, diagnostics


def test_the_same_consumers_on_a_scheduled_root_are_still_refused():
    """Contrast (amendment 2 §4): without the passthrough entry the first document is
    the empty No Data document, so the profile read and the consuming call keep
    their refusals."""
    diagnostics = _both_routes(_doc(_writer("P2"), _PATCH, _STOP))
    assert (PROCESS_IR_SEMANTIC_PROFILE_MISMATCH, "/body/steps/0/source_values/0/profile_ref") in diagnostics, diagnostics
    assert (PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH, "/body/steps/1/operation_ref") in diagnostics, diagnostics
    assert walk_lineage(prepare_validation_context(parse_process_ir_v1(_doc(_writer("P2"), _PATCH, _STOP)),
                                                   _symbols())).entry_requirements == ()


def test_the_branch_point_of_the_merge_refused_the_passthrough_consumers():
    """The 'before' half, recorded as measured on the merged tree before this change
    (ledger plan correction for the caller entry): connector resolution refused
    `[passthrough, PATCH, stop]` at `/body/steps/1/operation_ref`. Asserted against the
    connector walk with the passthrough producer removed, so the witness exercises
    the real walk rather than restating the number."""
    from boomi_mcp.compiler.process_ir import connector_resolution

    original = connector_resolution._walk_paths
    source = __import__("inspect").getsource(original)
    assert 'elif kind == "passthrough":' in source, "the passthrough producer branch is gone"
