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
from src.boomi_mcp.categories.integration_builder import _resolve_dependency_tokens

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


_NOTIFY_FIXTURE = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "golden_xml"
    / "try_catch_notify_dlq_document_cache.xml"
)

# Issue #89: placeholder Notify config (references the caught-error property by
# its token; the builder substitutes it for the {1} placeholder + track param).
_NOTIFY_TOKEN = "meta.base.catcherrorsmessage"
_NOTIFY_TEMPLATE = f"Integration catch path failed. Caught error: {_NOTIFY_TOKEN}"
_CATCH_NOTIFY = {"level": "ERROR", "message_template": _NOTIFY_TEMPLATE}


def _config(dlq, transform=None, catch_notify=None):
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
    if catch_notify is not None:
        cfg["reliability"]["catch_notify"] = catch_notify
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


_FIXTURE_RETRY2 = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "golden_xml"
    / "try_catch_dlq_retry_count_2.xml"
)


def test_document_cache_retry_count_2_matches_golden_fixture():
    """Issue #88: a retry_count=2 build emits the verified Try/Catch with the
    bounded retry attribute. Builder-emitted golden (no vendor XML)."""
    cfg = _config({"mode": "document_cache_ref", "document_cache_id": _CACHE_ID})
    cfg["reliability"]["retry_count"] = 2
    emitted = ProcessFlowBuilder.build(
        cfg, name="TryCatch DLQ Retry2 Golden", folder_name="Golden/Fixtures"
    )
    assert ET.canonicalize(emitted) == ET.canonicalize(_FIXTURE_RETRY2.read_text())


@pytest.mark.parametrize("retry_count", [1, 2, 5])
def test_retry_count_emits_bounded_retry_attribute(retry_count):
    # Issue #88: the full un-gated range 1..5 emits the matching Retry Count.
    cfg = _config({"mode": "document_cache_ref", "document_cache_id": _CACHE_ID})
    cfg["reliability"]["retry_count"] = retry_count
    _, shapes = _parse_shapes(ProcessFlowBuilder.build(cfg, name="N"))
    catcherrors = shapes[1]
    cfg_node = catcherrors.find("configuration/catcherrors")
    assert cfg_node.attrib["retryCount"] == str(retry_count)
    assert cfg_node.attrib["catchAll"] == "true"
    # Catch leg still present + terminal (unchanged by the retry count).
    assert shapes[-1].attrib["shapetype"] == "doccacheload"


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
# $ref DLQ binding: resolution -> emitted id (full pipeline invariant)
# ---------------------------------------------------------------------------

def test_ref_token_binding_resolves_into_emitted_doccache():
    # Exercises the whole $ref -> resolve -> emit path that validate_config-only
    # tests miss: a $ref:KEY binding must be substituted by
    # _resolve_dependency_tokens (as integration_builder does before build())
    # and the RESOLVED id — not the literal "$ref:my_cache" — must reach docCache.
    resolved_cache = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    cfg = _config({"mode": "document_cache_ref", "document_cache_id": "$ref:my_cache"})
    resolved_cfg = _resolve_dependency_tokens(cfg, {"my_cache": resolved_cache})
    _, shapes = _parse_shapes(ProcessFlowBuilder.build(resolved_cfg, name="N"))
    catch_leg = shapes[5]
    assert catch_leg.attrib["shapetype"] == "doccacheload"
    doccache = catch_leg.find("configuration/doccacheload").attrib["docCache"]
    assert doccache == resolved_cache
    assert "$ref" not in doccache


def test_ref_token_binding_resolves_into_emitted_processcall():
    resolved_proc = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    cfg = _config({"mode": "error_subprocess_ref", "process_id": "$ref:my_proc"})
    resolved_cfg = _resolve_dependency_tokens(cfg, {"my_proc": resolved_proc})
    _, shapes = _parse_shapes(ProcessFlowBuilder.build(resolved_cfg, name="N"))
    call = shapes[5].find("configuration/processcall")
    assert call.attrib["processId"] == resolved_proc
    assert "$ref" not in call.attrib["processId"]


# ---------------------------------------------------------------------------
# build() stays total on the validate_config-bypass path (issue #51 fix)
# ---------------------------------------------------------------------------

def test_build_raises_on_missing_document_cache_binding():
    # Direct build() (bypassing validate_config) with a DLQ mode but no binding
    # must RAISE, not emit <doccacheload docCache=""/>.
    cfg = _config({"mode": "document_cache_ref"})  # no document_cache_id
    with pytest.raises(BuilderValidationError) as exc:
        ProcessFlowBuilder.build(cfg, name="N")
    assert exc.value.error_code == "PROCESS_DLQ_BINDING_INVALID"
    assert exc.value.field == "reliability.dlq.document_cache_id"


def test_build_raises_on_missing_error_subprocess_binding():
    cfg = _config({"mode": "error_subprocess_ref"})  # no process_id
    with pytest.raises(BuilderValidationError) as exc:
        ProcessFlowBuilder.build(cfg, name="N")
    assert exc.value.error_code == "PROCESS_DLQ_BINDING_INVALID"
    assert exc.value.field == "reliability.dlq.process_id"


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

    def test_retry_count_positive_with_dlq_now_accepted(self):
        # Issue #88: retry_count 1..5 with a wired DLQ catch path is un-gated.
        cfg = _config({"mode": "document_cache_ref", "document_cache_id": _CACHE_ID})
        cfg["reliability"]["retry_count"] = 1
        assert ProcessFlowBuilder.validate_config(cfg, depends_on=[]) is None
        cfg["reliability"]["retry_count"] = 5
        assert ProcessFlowBuilder.validate_config(cfg, depends_on=[]) is None
        # Out-of-range retry stays gated.
        cfg["reliability"]["retry_count"] = 6
        err6 = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
        assert err6.error_code == "PROCESS_RETRY_UNVERIFIED"

    def test_retry_count_positive_without_dlq_still_gated(self):
        # Positive retry has no Try/Catch catch leg without a wired DLQ mode.
        cfg = _config({"mode": "disabled"})
        cfg["reliability"]["retry_count"] = 2
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
        # Issue #88: retry_count 1..5 with a supported DLQ mode now emits.
        assert ProcessFlowBuilder._should_emit_try_catch(
            {"retry_count": 1, "dlq": {"mode": "document_cache_ref"}}
        ) is True
        assert ProcessFlowBuilder._should_emit_try_catch(
            {"retry_count": 5, "dlq": {"mode": "error_subprocess_ref"}}
        ) is True
        # Out of range / disabled / None → no Try/Catch.
        assert ProcessFlowBuilder._should_emit_try_catch(
            {"retry_count": 6, "dlq": {"mode": "document_cache_ref"}}
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


# ---------------------------------------------------------------------------
# Issue #89 — Notify step on the catch leg
# ---------------------------------------------------------------------------

def test_notify_document_cache_matches_golden_fixture():
    """The canonical document_cache_ref + catch_notify build must match the
    committed golden (C14N-compared, like the no-notify golden)."""
    cfg = _config(
        {"mode": "document_cache_ref", "document_cache_id": _CACHE_ID},
        catch_notify=_CATCH_NOTIFY,
    )
    emitted = ProcessFlowBuilder.build(
        cfg, name="TryCatch Notify DLQ Golden", folder_name="Golden/Fixtures"
    )
    assert ET.canonicalize(emitted) == ET.canonicalize(_NOTIFY_FIXTURE.read_text())


def test_notify_document_cache_shape_sequence():
    cfg = _config(
        {"mode": "document_cache_ref", "document_cache_id": _CACHE_ID},
        catch_notify=_CATCH_NOTIFY,
    )
    _, shapes = _parse_shapes(ProcessFlowBuilder.build(cfg, name="N"))
    # Catch leg becomes notify -> dlq route -> catch stop, appended after the
    # Try-path stop.
    assert _by_type(shapes) == [
        "start", "catcherrors", "connectoraction", "connectoraction",
        "stop", "notify", "doccacheload", "stop",
    ]


def test_notify_error_subprocess_shape_sequence():
    cfg = _config(
        {"mode": "error_subprocess_ref", "process_id": _PROC_ID},
        catch_notify=_CATCH_NOTIFY,
    )
    _, shapes = _parse_shapes(ProcessFlowBuilder.build(cfg, name="N"))
    assert _by_type(shapes) == [
        "start", "catcherrors", "connectoraction", "connectoraction",
        "stop", "notify", "processcall", "stop",
    ]


def test_notify_catch_leg_wiring_resolves():
    for dlq in (
        {"mode": "document_cache_ref", "document_cache_id": _CACHE_ID},
        {"mode": "error_subprocess_ref", "process_id": _PROC_ID},
    ):
        _, shapes = _parse_shapes(
            ProcessFlowBuilder.build(_config(dlq, catch_notify=_CATCH_NOTIFY), name="N")
        )
        by_name = {s.attrib["name"]: s for s in shapes}
        catcherrors = shapes[1]
        notify = shapes[5]
        dlq_route = shapes[6]
        catch_stop = shapes[7]
        # catcherrors Catch dragpoint targets the Notify (not the DLQ route).
        catch_dp = {dp.attrib["identifier"]: dp for dp in catcherrors.find("dragpoints")}
        assert catch_dp["error"].attrib["toShape"] == notify.attrib["name"]
        # Notify -> DLQ route -> catch Stop.
        assert notify.attrib["shapetype"] == "notify"
        notify_dps = list(notify.find("dragpoints"))
        assert len(notify_dps) == 1
        assert notify_dps[0].attrib["toShape"] == dlq_route.attrib["name"]
        dlq_dps = list(dlq_route.find("dragpoints"))
        assert len(dlq_dps) == 1
        assert dlq_dps[0].attrib["toShape"] == catch_stop.attrib["name"]
        # Catch Stop is terminal and on the catch row.
        assert catch_stop.attrib["shapetype"] == "stop"
        assert catch_stop.attrib["y"] == "456.0"
        assert list(catch_stop.find("dragpoints")) == []
        # Every dragpoint target resolves.
        for shape in shapes:
            for dp in shape.find("dragpoints"):
                assert dp.attrib["toShape"] in by_name


def test_notify_config_is_verified_shape():
    cfg = _config(
        {"mode": "document_cache_ref", "document_cache_id": _CACHE_ID},
        catch_notify={"level": "warning", "message_template": _NOTIFY_TEMPLATE},
    )
    _, shapes = _parse_shapes(ProcessFlowBuilder.build(cfg, name="N"))
    notify = shapes[5].find("configuration/notify")
    # Log-only Notify (no platform email event → email/SMS stays out of scope).
    assert notify.attrib["disableEvent"] == "true"
    assert notify.attrib["enableUserLog"] == "false"
    assert notify.attrib["perExecution"] == "false"
    # level is normalized to the canonical uppercase token.
    assert notify.find("notifyMessageLevel").text == "WARNING"
    # The caught-error property token is substituted for the {1} placeholder...
    msg = notify.find("notifyMessage").text
    assert "{1}" in msg
    assert _NOTIFY_TOKEN not in msg
    # ...and bound as the single notify track parameter.
    tp = notify.find("notifyParameters/parametervalue/trackparameter")
    assert tp.attrib["propertyId"] == _NOTIFY_TOKEN


def test_notify_message_doubles_apostrophes_for_messageformat():
    # Boomi Notify text uses MessageFormat quoting: an unmatched apostrophe would
    # quote the {1} placeholder and stop the caught-error from expanding. The
    # builder doubles apostrophes so they render literally and {1} still binds.
    cfg = _config(
        {"mode": "document_cache_ref", "document_cache_id": _CACHE_ID},
        catch_notify={
            "level": "ERROR",
            "message_template": f"couldn't sync: {_NOTIFY_TOKEN}",
        },
    )
    _, shapes = _parse_shapes(ProcessFlowBuilder.build(cfg, name="N"))
    msg = shapes[5].find("configuration/notify/notifyMessage").text
    # XML decodes &apos;&apos; back to '' — the MessageFormat literal-quote escape.
    assert msg == "couldn''t sync: {1}"
    assert "{1}" in msg


def test_notify_xml_round_trips():
    xml = ProcessFlowBuilder.build(
        _config(
            {"mode": "document_cache_ref", "document_cache_id": _CACHE_ID},
            catch_notify=_CATCH_NOTIFY,
        ),
        name="N",
    )
    ET.fromstring(xml)  # must not raise


def test_notify_with_retry_still_emits_bounded_retry():
    cfg = _config(
        {"mode": "document_cache_ref", "document_cache_id": _CACHE_ID},
        catch_notify=_CATCH_NOTIFY,
    )
    cfg["reliability"]["retry_count"] = 3
    _, shapes = _parse_shapes(ProcessFlowBuilder.build(cfg, name="N"))
    assert shapes[1].find("configuration/catcherrors").attrib["retryCount"] == "3"
    assert _by_type(shapes)[5:] == ["notify", "doccacheload", "stop"]


class TestNotifyValidation:
    def _ok_dlq(self):
        return {"mode": "document_cache_ref", "document_cache_id": _CACHE_ID}

    def test_accepts_valid_catch_notify(self):
        cfg = _config(self._ok_dlq(), catch_notify=_CATCH_NOTIFY)
        assert ProcessFlowBuilder.validate_config(cfg, depends_on=[]) is None

    def test_rejects_non_dict_catch_notify(self):
        cfg = _config(self._ok_dlq(), catch_notify="nope")
        err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
        assert err.error_code == "PROCESS_NOTIFY_CONFIG_INVALID"
        assert err.field == "reliability.catch_notify"

    def test_rejects_extra_channel_keys(self):
        for extra in ({"email_to": "x"}, {"channel": "slack"}, {"sms": "+1"}):
            cn = dict(_CATCH_NOTIFY, **extra)
            err = ProcessFlowBuilder.validate_config(
                _config(self._ok_dlq(), catch_notify=cn), depends_on=[]
            )
            assert err.error_code == "PROCESS_NOTIFY_CONFIG_INVALID"
            assert err.field == "reliability.catch_notify"

    def test_rejects_missing_template(self):
        err = ProcessFlowBuilder.validate_config(
            _config(self._ok_dlq(), catch_notify={"level": "ERROR"}), depends_on=[]
        )
        assert err.error_code == "PROCESS_NOTIFY_CONFIG_INVALID"
        assert err.field == "reliability.catch_notify.message_template"

    def test_rejects_blank_template(self):
        err = ProcessFlowBuilder.validate_config(
            _config(self._ok_dlq(), catch_notify={"level": "ERROR", "message_template": "  "}),
            depends_on=[],
        )
        assert err.error_code == "PROCESS_NOTIFY_CONFIG_INVALID"
        assert err.field == "reliability.catch_notify.message_template"

    def test_rejects_template_without_caught_error_token(self):
        err = ProcessFlowBuilder.validate_config(
            _config(self._ok_dlq(), catch_notify={"level": "ERROR", "message_template": "static text"}),
            depends_on=[],
        )
        assert err.error_code == "PROCESS_NOTIFY_CONFIG_INVALID"
        assert err.field == "reliability.catch_notify.message_template"

    def test_rejects_unsupported_level(self):
        for bad in ("SEVERE", "debug", "", 5):
            err = ProcessFlowBuilder.validate_config(
                _config(self._ok_dlq(), catch_notify={"level": bad, "message_template": _NOTIFY_TEMPLATE}),
                depends_on=[],
            )
            assert err.error_code == "PROCESS_NOTIFY_CONFIG_INVALID"
            assert err.field == "reliability.catch_notify.level"

    def test_rejects_notify_without_wired_dlq(self):
        cfg = _config({"mode": "disabled"}, catch_notify=_CATCH_NOTIFY)
        err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
        assert err.error_code == "PROCESS_NOTIFY_CONFIG_INVALID"
        assert err.field == "reliability.catch_notify"

    def test_build_bypass_raises_on_invalid_notify(self):
        # Direct build() (bypassing validate_config) with a wired DLQ but a
        # malformed catch_notify must RAISE, not emit broken XML.
        cfg = _config(self._ok_dlq(), catch_notify={"level": "ERROR", "message_template": "no token"})
        with pytest.raises(BuilderValidationError) as exc:
            ProcessFlowBuilder.build(cfg, name="N")
        assert exc.value.error_code == "PROCESS_NOTIFY_CONFIG_INVALID"

    def test_build_bypass_raises_when_notify_present_without_wired_dlq(self):
        # Direct build() with catch_notify but a disabled DLQ would skip the
        # Try/Catch path (no catch leg) — build() must RAISE rather than silently
        # drop the notify (the linear-fallback branch stays total). Codex §6.
        cfg = _config({"mode": "disabled"}, catch_notify=_CATCH_NOTIFY)
        with pytest.raises(BuilderValidationError) as exc:
            ProcessFlowBuilder.build(cfg, name="N")
        assert exc.value.error_code == "PROCESS_NOTIFY_CONFIG_INVALID"
        assert exc.value.field == "reliability.catch_notify"
