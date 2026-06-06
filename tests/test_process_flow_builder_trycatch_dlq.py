"""Golden + structural tests for the issue #51 M3.R1a Try/Catch + DLQ slice.

ProcessFlowBuilder un-gates ``reliability.retry_count == 0`` with
``reliability.dlq.mode`` in {``document_cache_ref``, ``error_subprocess_ref``}
and emits a verified Try/Catch (``catcherrors``) wrapper whose catch leg routes
caught documents to a DLQ.

The emitted shapes are transcribed verbatim from verified live ``work``-profile
exports (no XML invented from docs):

  * ``catcherrors`` / ``doccacheload`` — component
    ``dff0bf83-d525-4781-b572-c93d285bb788`` ("[Time 3E Submission]
    REST-Call 3E POST TimeCard or Pending"), shapes shape4 / shape80.
  * ``processcall`` — component ``7b19baeb-ed62-4fac-9962-44fc0ed87f07``
    ("[Time Submission] Auto Release"), shape34, on a catcherrors
    error branch.

Structure is asserted with ElementTree (matching test_process_flow_builder.py)
plus a committed golden fixture compared via XML canonicalization (robust to
attribute ordering — the repo deliberately commits no byte-exact fixtures).
"""

from __future__ import annotations

import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from src.boomi_mcp.categories.components.builders import (
    BuilderValidationError,
    ProcessFlowBuilder,
)

NS = {"bns": "http://api.platform.boomi.com/"}

_FIXTURE = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "golden_xml"
    / "try_catch_dlq_document_cache.xml"
)

_DB_CONN_ID = "11111111-1111-1111-1111-111111111111"
_DB_OP_ID = "22222222-2222-2222-2222-222222222222"
_REST_CONN_ID = "33333333-3333-3333-3333-333333333333"
_REST_OP_ID = "44444444-4444-4444-4444-444444444444"
_CACHE_ID = "55555555-5555-5555-5555-555555555555"
_PROC_ID = "66666666-6666-6666-6666-666666666666"


def _config(dlq, transform=None):
    cfg = {
        "process_kind": "database_to_api_sync",
        "source": {
            "connector_type": "database",
            "connection_id": _DB_CONN_ID,
            "operation_id": _DB_OP_ID,
            "action_type": "Get",
            "label": "DB extract",
        },
        "transform": transform or {"mode": "passthrough"},
        "target": {
            "connector_type": "rest",
            "connection_id": _REST_CONN_ID,
            "operation_id": _REST_OP_ID,
            "action_type": "POST",
            "label": "REST send",
        },
        "reliability": {"retry_count": 0, "dlq": dlq},
    }
    return cfg


def _parse_shapes(xml):
    root = ET.fromstring(xml)
    process = root.find("bns:object/process", NS)
    assert process is not None
    return root, list(process.find("shapes").findall("shape"))


def _by_type(shapes):
    return [s.attrib["shapetype"] for s in shapes]


# ---------------------------------------------------------------------------
# Golden fixture
# ---------------------------------------------------------------------------

def test_document_cache_matches_golden_fixture():
    """The canonical document_cache_ref build must match the committed golden.

    Compared via C14N canonicalization so attribute ordering is not brittle —
    if an emitter changes shape structure, the canonical forms diverge and
    this fails (regenerate the fixture deliberately, not accidentally)."""
    cfg = _config({"mode": "document_cache_ref", "document_cache_id": _CACHE_ID})
    emitted = ProcessFlowBuilder.build(
        cfg, name="TryCatch DLQ Golden", folder_name="Golden/Fixtures"
    )
    expected = _FIXTURE.read_text()
    assert ET.canonicalize(emitted) == ET.canonicalize(expected)


# ---------------------------------------------------------------------------
# catcherrors wrapper structure (verified live shape)
# ---------------------------------------------------------------------------

def test_document_cache_emits_catcherrors_wrapper():
    cfg = _config({"mode": "document_cache_ref", "document_cache_id": _CACHE_ID})
    root, shapes = _parse_shapes(ProcessFlowBuilder.build(cfg, name="N"))

    assert _by_type(shapes) == [
        "start", "catcherrors", "connectoraction", "connectoraction",
        "stop", "doccacheload",
    ]

    catcherrors = shapes[1]
    cfg_node = catcherrors.find("configuration/catcherrors")
    assert cfg_node is not None
    assert cfg_node.attrib["catchAll"] == "true"
    assert cfg_node.attrib["retryCount"] == "0"

    # Start now points at the catcherrors wrapper, not the source directly.
    start_dp = list(shapes[0].find("dragpoints"))
    assert len(start_dp) == 1
    assert start_dp[0].attrib["toShape"] == catcherrors.attrib["name"]


def test_catcherrors_try_and_catch_dragpoints():
    cfg = _config({"mode": "document_cache_ref", "document_cache_id": _CACHE_ID})
    _, shapes = _parse_shapes(ProcessFlowBuilder.build(cfg, name="N"))
    catcherrors = shapes[1]
    source = shapes[2]
    catch_leg = shapes[5]

    dps = {dp.attrib["identifier"]: dp for dp in catcherrors.find("dragpoints")}
    assert set(dps) == {"default", "error"}
    # Try path -> first normal shape (source); Catch path -> DLQ catch leg.
    assert dps["default"].attrib["text"] == "Try"
    assert dps["default"].attrib["toShape"] == source.attrib["name"]
    assert dps["error"].attrib["text"] == "Catch"
    assert dps["error"].attrib["toShape"] == catch_leg.attrib["name"]


def test_document_cache_catch_leg_is_terminal_doccacheload():
    cfg = _config({"mode": "document_cache_ref", "document_cache_id": _CACHE_ID})
    _, shapes = _parse_shapes(ProcessFlowBuilder.build(cfg, name="N"))
    catch_leg = shapes[5]
    assert catch_leg.attrib["shapetype"] == "doccacheload"
    assert catch_leg.find("configuration/doccacheload").attrib["docCache"] == _CACHE_ID
    # Verified live shape: catch leg is terminal (no outgoing edge / no Stop).
    assert list(catch_leg.find("dragpoints")) == []


def test_only_one_stop_on_try_path():
    cfg = _config({"mode": "document_cache_ref", "document_cache_id": _CACHE_ID})
    _, shapes = _parse_shapes(ProcessFlowBuilder.build(cfg, name="N"))
    stops = [s for s in shapes if s.attrib["shapetype"] == "stop"]
    assert len(stops) == 1
    assert stops[0].find("configuration/stop").attrib["continue"] == "true"


def test_error_subprocess_emits_terminal_processcall():
    cfg = _config({"mode": "error_subprocess_ref", "process_id": _PROC_ID})
    _, shapes = _parse_shapes(ProcessFlowBuilder.build(cfg, name="N"))
    assert _by_type(shapes) == [
        "start", "catcherrors", "connectoraction", "connectoraction",
        "stop", "processcall",
    ]
    call = shapes[5].find("configuration/processcall")
    assert call.attrib["processId"] == _PROC_ID
    # Verified live shape attributes (component 7b19baeb-... shape34).
    assert call.attrib["abort"] == "true"
    assert call.attrib["wait"] == "true"
    assert call.find("parameters") is not None
    assert call.find("returnpaths") is not None
    assert list(shapes[5].find("dragpoints")) == []


def test_transform_is_inside_try_path():
    cfg = _config(
        {"mode": "document_cache_ref", "document_cache_id": _CACHE_ID},
        transform={"mode": "message", "message_text": "'{\"k\":1}'"},
    )
    _, shapes = _parse_shapes(ProcessFlowBuilder.build(cfg, name="N"))
    # Try chain now: source -> message -> target -> stop, all inside Try.
    assert _by_type(shapes) == [
        "start", "catcherrors", "connectoraction", "message",
        "connectoraction", "stop", "doccacheload",
    ]


def test_every_dragpoint_target_resolves_in_trycatch():
    for dlq in (
        {"mode": "document_cache_ref", "document_cache_id": _CACHE_ID},
        {"mode": "error_subprocess_ref", "process_id": _PROC_ID},
    ):
        _, shapes = _parse_shapes(ProcessFlowBuilder.build(_config(dlq), name="N"))
        names = {s.attrib["name"] for s in shapes}
        for shape in shapes:
            for dp in shape.find("dragpoints"):
                assert dp.attrib["toShape"] in names


def test_trycatch_xml_round_trips():
    # The build()'s internal ET.fromstring guard already enforces this, but
    # assert it explicitly for the new shapes.
    xml = ProcessFlowBuilder.build(
        _config({"mode": "document_cache_ref", "document_cache_id": _CACHE_ID}),
        name="N",
    )
    ET.fromstring(xml)  # must not raise


# ---------------------------------------------------------------------------
# validate_config gating
# ---------------------------------------------------------------------------

class TestValidateGating:
    def test_accepts_zero_retry_document_cache_with_id(self):
        cfg = _config({"mode": "document_cache_ref", "document_cache_id": _CACHE_ID})
        assert ProcessFlowBuilder.validate_config(cfg, depends_on=[]) is None

    def test_accepts_zero_retry_error_subprocess_with_id(self):
        cfg = _config({"mode": "error_subprocess_ref", "process_id": _PROC_ID})
        assert ProcessFlowBuilder.validate_config(cfg, depends_on=[]) is None

    def test_accepts_ref_token_binding_in_depends_on(self):
        cfg = _config({"mode": "document_cache_ref", "document_cache_id": "$ref:my_cache"})
        assert ProcessFlowBuilder.validate_config(cfg, depends_on=["my_cache"]) is None

    def test_rejects_ref_token_binding_undeclared(self):
        cfg = _config({"mode": "document_cache_ref", "document_cache_id": "$ref:my_cache"})
        err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
        assert err.error_code == "MISSING_PROCESS_DEPENDENCY"

    def test_retry_count_positive_with_dlq_still_gated(self):
        cfg = _config({"mode": "document_cache_ref", "document_cache_id": _CACHE_ID})
        cfg["reliability"]["retry_count"] = 1
        err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
        assert err.error_code == "PROCESS_RETRY_UNVERIFIED"
        assert err.field == "reliability.retry_count"

    def test_rejects_document_cache_missing_binding(self):
        cfg = _config({"mode": "document_cache_ref"})
        err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
        assert err.error_code == "PROCESS_DLQ_BINDING_INVALID"
        assert err.field == "reliability.dlq.document_cache_id"

    def test_rejects_error_subprocess_missing_binding(self):
        cfg = _config({"mode": "error_subprocess_ref"})
        err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
        assert err.error_code == "PROCESS_DLQ_BINDING_INVALID"
        assert err.field == "reliability.dlq.process_id"

    def test_rejects_bare_ref_key_binding(self):
        # The dlq_writer primitive's bare *_ref_key is not resolvable on the
        # build path — reject with a clear PROCESS_DLQ_BINDING_INVALID.
        cfg = _config({"mode": "document_cache_ref", "document_cache_ref_key": "k"})
        err = ProcessFlowBuilder.validate_config(cfg, depends_on=["k"])
        assert err.error_code == "PROCESS_DLQ_BINDING_INVALID"
        assert err.field == "reliability.dlq.document_cache_ref_key"

    def test_rejects_both_id_and_ref_key(self):
        cfg = _config({
            "mode": "document_cache_ref",
            "document_cache_id": _CACHE_ID,
            "document_cache_ref_key": "k",
        })
        err = ProcessFlowBuilder.validate_config(cfg, depends_on=["k"])
        assert err.error_code == "PROCESS_DLQ_BINDING_INVALID"

    def test_should_emit_try_catch_guard(self):
        good = {"retry_count": 0, "dlq": {"mode": "document_cache_ref", "document_cache_id": _CACHE_ID}}
        assert ProcessFlowBuilder._should_emit_try_catch(good) is True
        assert ProcessFlowBuilder._should_emit_try_catch(
            {"retry_count": 1, "dlq": {"mode": "document_cache_ref"}}
        ) is False
        assert ProcessFlowBuilder._should_emit_try_catch(
            {"retry_count": 0, "dlq": {"mode": "disabled"}}
        ) is False
        assert ProcessFlowBuilder._should_emit_try_catch(None) is False


# ---------------------------------------------------------------------------
# Non-DLQ build is unchanged (guards the "existing XML unchanged" criterion)
# ---------------------------------------------------------------------------

def test_disabled_dlq_build_has_no_catcherrors():
    cfg = _config({"mode": "disabled"})
    _, shapes = _parse_shapes(ProcessFlowBuilder.build(cfg, name="N"))
    assert _by_type(shapes) == ["start", "connectoraction", "connectoraction", "stop"]


def test_no_reliability_build_has_no_catcherrors():
    cfg = _config({"mode": "disabled"})
    del cfg["reliability"]
    _, shapes = _parse_shapes(ProcessFlowBuilder.build(cfg, name="N"))
    assert _by_type(shapes) == ["start", "connectoraction", "connectoraction", "stop"]
