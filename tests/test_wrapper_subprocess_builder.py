"""Tests for WrapperSubprocessBuilder — the thin wrapper-parent ("facade")
process and standalone Process Call emission (issue #90 M4.5.5).

The standalone processcall shape is transcribed from the verified live ``work``
wrapper exemplar (component 6a432a0b-..., a processcall calling the main-logic
subprocess 57a5822c-...): ``abort="false"`` (the parent continues past a child
failure), ``wait="true"``, empty parameters/returnpaths. The DLQ catch-leg
processcall (abort="true") is unchanged — see test_process_flow_builder_trycatch_dlq.

Structure is asserted with ElementTree plus a committed golden fixture compared
via XML canonicalization (matching the repo's other process-builder goldens).
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
    WrapperSubprocessBuilder,
    PROCESS_FLOW_BUILDERS,
    get_process_flow_builder,
)

NS = {"bns": "http://api.platform.boomi.com/"}

_GOLDEN = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "golden_xml"
    / "processcall_standalone_parent.xml"
)

_CHILD_ID = "11111111-1111-1111-1111-111111111111"
_CHILD_ID_2 = "22222222-2222-2222-2222-222222222222"


def _parse_shapes(xml):
    root = ET.fromstring(xml)
    process = root.find("bns:object/process", NS)
    assert process is not None
    return root, list(process.find("shapes").findall("shape"))


def _by_type(shapes):
    return [s.attrib["shapetype"] for s in shapes]


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

def test_registry_exposes_wrapper_subprocess():
    assert "wrapper_subprocess" in PROCESS_FLOW_BUILDERS
    assert get_process_flow_builder("wrapper_subprocess") is WrapperSubprocessBuilder


# ---------------------------------------------------------------------------
# Golden fixture + structure
# ---------------------------------------------------------------------------

def test_standalone_processcall_matches_golden_fixture():
    """A single-child parent build matches the committed golden (C14N-compared)."""
    cfg = {
        "process_kind": "wrapper_subprocess",
        "process_calls": [
            {"subprocess_ref": _CHILD_ID, "wait": True, "abort_on_error": False,
             "label": "Run main-logic subprocess"},
        ],
    }
    emitted = WrapperSubprocessBuilder.build(
        cfg, name="Wrapper Parent Golden", folder_name="Golden/Fixtures"
    )
    assert ET.canonicalize(emitted) == ET.canonicalize(_GOLDEN.read_text())


def test_parent_shape_sequence_and_wiring():
    cfg = {
        "process_kind": "wrapper_subprocess",
        "process_calls": [{"subprocess_ref": _CHILD_ID}],
    }
    _, shapes = _parse_shapes(WrapperSubprocessBuilder.build(cfg, name="N"))
    assert _by_type(shapes) == ["start", "processcall", "stop"]
    start, call, stop = shapes
    # start -> processcall -> stop
    assert [dp.attrib["toShape"] for dp in start.find("dragpoints")] == [call.attrib["name"]]
    assert [dp.attrib["toShape"] for dp in call.find("dragpoints")] == [stop.attrib["name"]]
    assert list(stop.find("dragpoints")) == []


def test_standalone_processcall_is_verified_shape():
    cfg = {
        "process_kind": "wrapper_subprocess",
        "process_calls": [{"subprocess_ref": _CHILD_ID}],
    }
    _, shapes = _parse_shapes(WrapperSubprocessBuilder.build(cfg, name="N"))
    pc = shapes[1].find("configuration/processcall")
    # Live-verified main-flow values: abort="false" (parent continues), wait="true".
    assert pc.attrib["abort"] == "false"
    assert pc.attrib["wait"] == "true"
    assert pc.attrib["processId"] == _CHILD_ID
    assert pc.find("parameters") is not None
    assert pc.find("returnpaths") is not None
    # Main-flow geometry (not the catch row).
    assert shapes[1].attrib["y"] == "96.0"


def test_explicit_process_id_target():
    cfg = {
        "process_kind": "wrapper_subprocess",
        "process_calls": [{"process_id": _CHILD_ID}],
    }
    _, shapes = _parse_shapes(WrapperSubprocessBuilder.build(cfg, name="N"))
    assert shapes[1].find("configuration/processcall").attrib["processId"] == _CHILD_ID


def test_multi_child_parent_chains():
    cfg = {
        "process_kind": "wrapper_subprocess",
        "process_calls": [{"process_id": _CHILD_ID}, {"process_id": _CHILD_ID_2}],
    }
    _, shapes = _parse_shapes(WrapperSubprocessBuilder.build(cfg, name="N"))
    assert _by_type(shapes) == ["start", "processcall", "processcall", "stop"]
    # start -> pc1 -> pc2 -> stop
    assert [dp.attrib["toShape"] for dp in shapes[0].find("dragpoints")] == [shapes[1].attrib["name"]]
    assert [dp.attrib["toShape"] for dp in shapes[1].find("dragpoints")] == [shapes[2].attrib["name"]]
    assert [dp.attrib["toShape"] for dp in shapes[2].find("dragpoints")] == [shapes[3].attrib["name"]]
    assert shapes[1].find("configuration/processcall").attrib["processId"] == _CHILD_ID
    assert shapes[2].find("configuration/processcall").attrib["processId"] == _CHILD_ID_2


def test_abort_on_error_true_emits_abort_attr():
    cfg = {
        "process_kind": "wrapper_subprocess",
        "process_calls": [{"process_id": _CHILD_ID, "abort_on_error": True, "wait": False}],
    }
    _, shapes = _parse_shapes(WrapperSubprocessBuilder.build(cfg, name="N"))
    pc = shapes[1].find("configuration/processcall")
    assert pc.attrib["abort"] == "true"
    assert pc.attrib["wait"] == "false"


def test_xml_round_trips():
    cfg = {"process_kind": "wrapper_subprocess", "process_calls": [{"process_id": _CHILD_ID}]}
    ET.fromstring(WrapperSubprocessBuilder.build(cfg, name="N"))  # must not raise


# ---------------------------------------------------------------------------
# validate_config
# ---------------------------------------------------------------------------

class TestValidate:
    def test_accepts_subprocess_ref_with_or_without_depends_on(self):
        cfg = {"process_kind": "wrapper_subprocess",
               "process_calls": [{"subprocess_ref": "$ref:child"}]}
        # depends_on is optional — the implicit edge is synthesized at plan time.
        assert WrapperSubprocessBuilder.validate_config(cfg, depends_on=["child"]) is None
        assert WrapperSubprocessBuilder.validate_config(cfg, depends_on=[]) is None

    def test_accepts_literal_process_id(self):
        cfg = {"process_kind": "wrapper_subprocess",
               "process_calls": [{"process_id": _CHILD_ID}]}
        assert WrapperSubprocessBuilder.validate_config(cfg, depends_on=[]) is None

    def test_rejects_wrong_process_kind(self):
        cfg = {"process_kind": "database_to_api_sync", "process_calls": [{"process_id": _CHILD_ID}]}
        err = WrapperSubprocessBuilder.validate_config(cfg, depends_on=[])
        assert err.error_code == "PROCESS_KIND_UNSUPPORTED"

    def test_rejects_empty_process_calls(self):
        for calls in ([], None, "x"):
            cfg = {"process_kind": "wrapper_subprocess", "process_calls": calls}
            err = WrapperSubprocessBuilder.validate_config(cfg, depends_on=[])
            assert err.error_code == "PROCESS_REF_MISSING"
            assert err.field == "process_calls"

    def test_rejects_entry_without_target(self):
        cfg = {"process_kind": "wrapper_subprocess", "process_calls": [{"wait": True}]}
        err = WrapperSubprocessBuilder.validate_config(cfg, depends_on=[])
        assert err.error_code == "PROCESS_REF_MISSING"

    def test_rejects_entry_with_both_targets(self):
        cfg = {"process_kind": "wrapper_subprocess",
               "process_calls": [{"subprocess_ref": "$ref:child", "process_id": _CHILD_ID}]}
        err = WrapperSubprocessBuilder.validate_config(cfg, depends_on=["child"])
        assert err.error_code == "PROCESS_REF_AMBIGUOUS"

    def test_rejects_subprocess_ref_not_a_ref_token(self):
        cfg = {"process_kind": "wrapper_subprocess",
               "process_calls": [{"subprocess_ref": _CHILD_ID}]}
        err = WrapperSubprocessBuilder.validate_config(cfg, depends_on=[])
        assert err.error_code == "PROCESS_REF_MISSING"
        assert err.field == "process_calls[0].subprocess_ref"

    def test_accepts_undeclared_ref_resolved_at_plan(self):
        # An undeclared subprocess_ref is fine at the builder layer — the
        # implicit edge is synthesized and cross-spec resolution (NOT_FOUND)
        # runs at the integration_builder plan layer.
        cfg = {"process_kind": "wrapper_subprocess",
               "process_calls": [{"subprocess_ref": "$ref:child"}]}
        assert WrapperSubprocessBuilder.validate_config(cfg, depends_on=[]) is None

    def test_rejects_padded_ref(self):
        # A padded ref is not resolvable by _resolve_dependency_tokens — rejected
        # by the exact-$ref-token shape check.
        cfg = {"process_kind": "wrapper_subprocess",
               "process_calls": [{"subprocess_ref": " $ref:child "}]}
        err = WrapperSubprocessBuilder.validate_config(cfg, depends_on=["child"])
        assert err.error_code == "PROCESS_REF_MISSING"
        assert err.field == "process_calls[0].subprocess_ref"

    def test_rejects_plaintext_secret(self):
        cfg = {"process_kind": "wrapper_subprocess",
               "process_calls": [{"process_id": _CHILD_ID}], "password": "hunter2"}
        err = WrapperSubprocessBuilder.validate_config(cfg, depends_on=[])
        assert err.error_code == "PLAINTEXT_SECRET_REJECTED"

    def test_rejects_empty_ref_token(self):
        # "$ref:" passes a naive startswith check but would emit processId="$ref:".
        cfg = {"process_kind": "wrapper_subprocess",
               "process_calls": [{"subprocess_ref": "$ref:"}]}
        err = WrapperSubprocessBuilder.validate_config(cfg, depends_on=[])
        assert err.error_code == "PROCESS_REF_MISSING"
        assert err.field == "process_calls[0].subprocess_ref"

    def test_rejects_ref_token_in_process_id(self):
        # A $ref in process_id bypasses the implicit edge + ref-type checks
        # (which only inspect subprocess_ref) — reject it.
        cfg = {"process_kind": "wrapper_subprocess",
               "process_calls": [{"process_id": "$ref:child"}]}
        err = WrapperSubprocessBuilder.validate_config(cfg, depends_on=["child"])
        assert err.error_code == "PROCESS_CALL_CONFIG_INVALID"
        assert err.field == "process_calls[0].process_id"

    def test_rejects_non_bool_flags(self):
        for flag in ("wait", "abort_on_error"):
            cfg = {"process_kind": "wrapper_subprocess",
                   "process_calls": [{"process_id": _CHILD_ID, flag: "false"}]}
            err = WrapperSubprocessBuilder.validate_config(cfg, depends_on=[])
            assert err.error_code == "PROCESS_CALL_CONFIG_INVALID", flag
            assert err.field == f"process_calls[0].{flag}"
        # A real boolean is accepted.
        ok = {"process_kind": "wrapper_subprocess",
              "process_calls": [{"process_id": _CHILD_ID, "wait": False, "abort_on_error": True}]}
        assert WrapperSubprocessBuilder.validate_config(ok, depends_on=[]) is None


def test_build_bypass_raises_on_missing_target():
    # Direct build() with neither subprocess_ref nor process_id must RAISE,
    # never emit <processcall processId="">.
    cfg = {"process_kind": "wrapper_subprocess", "process_calls": [{"wait": True}]}
    with pytest.raises(BuilderValidationError) as exc:
        WrapperSubprocessBuilder.build(cfg, name="N")
    assert exc.value.error_code == "PROCESS_REF_MISSING"


# ---------------------------------------------------------------------------
# Issue #99 G3 — wrapper carries process_extensions (env-extension overrides)
#
# So a wrapper-deployed package surfaces the child connection override points
# through get_extensions. The block is validated + emitted exactly like
# ProcessFlowBuilder's, reusing _emit_process_overrides; integration_builder
# HOISTS it from a called child (see test_integration_builder).
# ---------------------------------------------------------------------------

_CONN_ID = "33333333-3333-3333-3333-333333333333"

_PE_BLOCK = {
    "connections": [
        {
            "connection_id": _CONN_ID,
            "fields": [
                {"id": "host", "label": "Host", "xpath": "DatabaseConnectionSettings/@host"},
                {"id": "password", "label": "Password", "xpath": "DatabaseConnectionSettings/@password"},
            ],
        }
    ]
}


def test_wrapper_without_process_extensions_emits_empty_overrides():
    """The pre-#99 wrapper output is unchanged: an absent block emits the empty
    <bns:processOverrides/> element."""
    cfg = {"process_kind": "wrapper_subprocess", "process_calls": [{"process_id": _CHILD_ID}]}
    xml = WrapperSubprocessBuilder.build(cfg, name="N")
    root = ET.fromstring(xml)
    overrides = root.find("bns:object/process/bns:processOverrides", NS)
    # Empty element: no ConnectionOverride children.
    assert overrides is None or len(list(overrides)) == 0


def test_wrapper_emits_process_extension_overrides():
    cfg = {
        "process_kind": "wrapper_subprocess",
        "process_calls": [{"process_id": _CHILD_ID}],
        "process_extensions": _PE_BLOCK,
    }
    assert WrapperSubprocessBuilder.validate_config(cfg, depends_on=[]) is None
    xml = WrapperSubprocessBuilder.build(cfg, name="N")
    assert "processOverrides" in xml and "ConnectionOverride" in xml
    assert _CONN_ID in xml
    assert "@password" in xml and "@host" in xml


def test_wrapper_rejects_malformed_process_extensions():
    cfg = {
        "process_kind": "wrapper_subprocess",
        "process_calls": [{"process_id": _CHILD_ID}],
        # connections must be a list of objects with a connection_id.
        "process_extensions": {"connections": [{"fields": []}]},
    }
    err = WrapperSubprocessBuilder.validate_config(cfg, depends_on=[])
    assert err is not None
    assert err.error_code == "PROCESS_EXTENSIONS_INVALID"


def test_wrapper_build_bypass_raises_on_malformed_process_extensions():
    cfg = {
        "process_kind": "wrapper_subprocess",
        "process_calls": [{"process_id": _CHILD_ID}],
        "process_extensions": {"connections": "not-a-list"},
    }
    with pytest.raises(BuilderValidationError) as exc:
        WrapperSubprocessBuilder.build(cfg, name="N")
    assert exc.value.error_code == "PROCESS_EXTENSIONS_INVALID"
