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
plus a committed golden fixture compared byte-for-byte (raw ``==``): the M12.3
(#138) emitter-registry extraction makes byte parity the hard gate, so these
builder-generated goldens are frozen as exact bytes, not canonicalized.
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

# The golden-case definitions live in the corpus (#165); this module CONSUMES
# them. The aliases keep every existing call site and assertion unchanged.
import _wave_gate_golden_corpus as _corpus

_CACHE_ID = _corpus.DLQ_CACHE_ID
_PROC_ID = _corpus.DLQ_PROC_ID


_NOTIFY_FIXTURE = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "golden_xml"
    / "try_catch_notify_dlq_document_cache.xml"
)

_NOTIFY_TOKEN = _corpus.DLQ_NOTIFY_TOKEN
_NOTIFY_TEMPLATE = _corpus.DLQ_NOTIFY_TEMPLATE
_CATCH_NOTIFY = _corpus.DLQ_CATCH_NOTIFY

_config = _corpus.dlq_config


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

    Compared byte-for-byte (raw ``==``): the builder emission is deterministic
    (fixed attribute order), so any byte change fails this and must be a
    deliberate fixture regeneration, not an accidental drift (#138 byte gate)."""
    cfg = _config({"mode": "document_cache_ref", "document_cache_id": _CACHE_ID})
    emitted = ProcessFlowBuilder.build(
        cfg, name="TryCatch DLQ Golden", folder_name="Golden/Fixtures"
    )
    expected = _FIXTURE.read_text()
    assert emitted == expected


_FIXTURE_RETRY2 = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "golden_xml"
    / "try_catch_dlq_retry_count_2.xml"
)


def test_document_cache_retry_count_2_matches_golden_fixture():
    """Issue #88: a retry_count=2 build emits the verified Try/Catch with the
    bounded retry attribute. Builder-emitted golden (no vendor XML)."""
    cfg = _corpus.dlq_retry2_case_config()
    emitted = ProcessFlowBuilder.build(
        cfg, name="TryCatch DLQ Retry2 Golden", folder_name="Golden/Fixtures"
    )
    assert emitted == _FIXTURE_RETRY2.read_text()


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


_ERROR_SUBPROCESS_FIXTURE = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "golden_xml"
    / "try_catch_dlq_error_subprocess.xml"
)


def test_error_subprocess_matches_golden_fixture():
    """Byte golden (#138): the catch-row error-subprocess ProcessCall variant had
    only structural coverage; freeze its bytes so the extracted processcall emitter
    is proven byte-identical."""
    from boomi_mcp.categories.components.process_graph_verifier import verify_process_graph

    cfg = _config({"mode": "error_subprocess_ref", "process_id": _PROC_ID})
    emitted = ProcessFlowBuilder.build(
        cfg, name="TryCatch Error Subprocess Golden", folder_name="Golden/Fixtures"
    )
    assert emitted == _ERROR_SUBPROCESS_FIXTURE.read_text()
    assert verify_process_graph(emitted)["errors"] == []


def test_transform_is_inside_try_path():
    cfg = _config(
        {"mode": "document_cache_ref", "document_cache_id": _CACHE_ID},
        transform={"mode": "message", "message_text": "{\"k\":1}"},
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
    assert emitted == _NOTIFY_FIXTURE.read_text()


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
    # #175: no trailing catch-row Stop after the process call. The call ends its
    # path, so the Stop this used to emit was never connected to it — measured at
    # baseline, this exact composition produced the orphan the issue reports.
    # The Stop carried no intent of its own (it existed only because the pre-#108
    # notify path needed *a* terminal), so nothing is lost by dropping it.
    assert _by_type(shapes) == [
        "start", "catcherrors", "connectoraction", "connectoraction",
        "stop", "notify", "processcall",
    ]
    assert list(shapes[6].find("dragpoints")) == []


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
        # catcherrors Catch dragpoint targets the Notify (not the DLQ route).
        catch_dp = {dp.attrib["identifier"]: dp for dp in catcherrors.find("dragpoints")}
        assert catch_dp["error"].attrib["toShape"] == notify.attrib["name"]
        # Notify -> DLQ route.
        assert notify.attrib["shapetype"] == "notify"
        notify_dps = list(notify.find("dragpoints"))
        assert len(notify_dps) == 1
        assert notify_dps[0].attrib["toShape"] == dlq_route.attrib["name"]
        # #175: what follows the DLQ route depends on WHICH route it is, and the
        # discriminator is the platform's own rule, not a preference. A Document
        # Cache load legitimately continues downstream, so it keeps its catch-row
        # Stop; a process call ends its path, so it has no outgoing edge and no
        # Stop is emitted after it.
        dlq_dps = list(dlq_route.find("dragpoints"))
        if dlq_route.attrib["shapetype"] == "processcall":
            assert dlq_dps == []
            assert len(shapes) == 7
        else:
            assert dlq_route.attrib["shapetype"] == "doccacheload"
            catch_stop = shapes[7]
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


# ---------------------------------------------------------------------------
# Issue #99 G1 — connector-scoped Try/Catch (one Try/Catch per connector)
#
# The whole-process scope (above) wraps the entire chain in ONE catcherrors, so
# a target (REST) retry re-runs the source (DB) read — live-proven a problem in
# #91 Scenario 2. Connector scope emits a Try/Catch per connector (source retry
# 0, target retry N) SEPARATED by the source connector, so each scopes its own
# failures independently (Boomi docs: "two Try/Catch steps separated by other
# steps — each behaves according to its own Failure Trigger") and the target
# retry no longer re-executes the source read.
# ---------------------------------------------------------------------------

_CONNECTOR_SCOPE_FIXTURE = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "golden_xml"
    / "connector_scoped_trycatch_notify_dlq_document_cache.xml"
)

_connector_config = _corpus.dlq_connector_config


def test_connector_scope_matches_golden_fixture():
    """The canonical connector-scoped build (map + document_cache + retry 2 +
    Notify — the #91 production pattern) must match the committed golden."""
    cfg = _connector_config(retry_count=2, catch_notify=_CATCH_NOTIFY)
    emitted = ProcessFlowBuilder.build(
        cfg, name="Connector Scope DLQ Golden", folder_name="Golden/Fixtures"
    )
    assert emitted == _CONNECTOR_SCOPE_FIXTURE.read_text()


def test_connector_scope_emits_two_try_catch_with_retry_placement():
    """Source connector gets its own Try/Catch (retry 0); target connector gets
    its own Try/Catch (the configured retry)."""
    cfg = _connector_config(retry_count=2)
    _root, shapes = _parse_shapes(ProcessFlowBuilder.build(cfg, name="N"))
    assert _by_type(shapes) == [
        "start", "catcherrors", "connectoraction", "map", "catcherrors",
        "connectoraction", "stop", "doccacheload", "doccacheload",
    ]
    catcherrors = [s for s in shapes if s.attrib["shapetype"] == "catcherrors"]
    retries = [c.find("configuration/catcherrors").attrib["retryCount"] for c in catcherrors]
    # Source Try/Catch retry 0; target Try/Catch retry 2.
    assert retries == ["0", "2"]


def test_connector_scope_target_retry_does_not_re_run_source():
    """The target Try/Catch's Try branch wraps ONLY the target connector; the
    source connector is UPSTREAM of it, so a target retry cannot re-execute the
    source read (the #99 G1 isolation guarantee)."""
    cfg = _connector_config(retry_count=3)
    root, shapes = _parse_shapes(ProcessFlowBuilder.build(cfg, name="N"))
    by_name = {s.attrib["name"]: s for s in shapes}

    def _drag(shape, identifier):
        for dp in shape.findall("dragpoints/dragpoint"):
            if dp.attrib.get("identifier") == identifier:
                return dp.attrib["toShape"]
        return None

    def _forward(shape):
        # A non-catcherrors flow shape has at most one (unnamed-identifier) edge.
        dps = shape.findall("dragpoints/dragpoint")
        return dps[0].attrib["toShape"] if dps else None

    catcherrors = [s for s in shapes if s.attrib["shapetype"] == "catcherrors"]
    src_ce, tgt_ce = catcherrors[0], catcherrors[1]
    # Source Try -> the source connector; target Try -> the target connector.
    src_try = by_name[_drag(src_ce, "default")]
    tgt_try = by_name[_drag(tgt_ce, "default")]
    assert src_try.attrib["shapetype"] == "connectoraction"
    assert tgt_try.attrib["shapetype"] == "connectoraction"
    assert src_try is not tgt_try
    # The source connector flows FORWARD into the target Try/Catch (via the map),
    # i.e. the source is upstream of the target catcherrors and therefore outside
    # the target's retry unit (the target Try wraps only the REST connector).
    nxt = _forward(src_try)
    hops = 0
    while nxt is not None and by_name[nxt].attrib["shapetype"] != "catcherrors" and hops < 5:
        nxt = _forward(by_name[nxt])
        hops += 1
    assert nxt == tgt_ce.attrib["name"], "source connector must flow into the target Try/Catch"
    # And the target Try branch (the retry unit) is exactly the target connector,
    # which terminates at the Try-row stop — the source is not on that branch.
    assert _forward(tgt_try) is not None
    assert by_name[_forward(tgt_try)].attrib["shapetype"] == "stop"


def test_connector_scoped_trycatch_keeps_dataprocess_outside_target_retry():
    """Issue #106 M10.2: a dataprocess transform is a middle (non-setproperties)
    shape, so under connector-scope it sits OUTSIDE the target retry unit — as a
    separator between the two Try/Catch shapes, exactly like map/message. A target
    retry must not re-run the Data Process step."""
    cfg = _connector_config(
        retry_count=2,
        transform={
            "mode": "dataprocess",
            "label": "Tag",
            "steps": [
                {"operation": "custom_scripting", "script": "dataContext.storeStream(is, props);"}
            ],
        },
    )
    _root, shapes = _parse_shapes(ProcessFlowBuilder.build(cfg, name="N"))
    # dataprocess takes the same slot map occupies in the canonical layout.
    assert _by_type(shapes) == [
        "start", "catcherrors", "connectoraction", "dataprocess", "catcherrors",
        "connectoraction", "stop", "doccacheload", "doccacheload",
    ]
    by_name = {s.attrib["name"]: s for s in shapes}
    catcherrors = [s for s in shapes if s.attrib["shapetype"] == "catcherrors"]
    tgt_ce = catcherrors[1]
    # The target Try branch enters directly at the target connector — the
    # dataprocess shape is NOT on the target retry branch.
    def _drag(shape, identifier):
        for dp in shape.findall("dragpoints/dragpoint"):
            if dp.attrib.get("identifier") == identifier:
                return dp.attrib["toShape"]
        return None

    tgt_try = by_name[_drag(tgt_ce, "default")]
    assert tgt_try.attrib["shapetype"] == "connectoraction"
    # And the dataprocess shape flows forward INTO the target Try/Catch (upstream
    # of it), so it is outside the retry unit.
    dp = next(s for s in shapes if s.attrib["shapetype"] == "dataprocess")
    fwd = dp.findall("dragpoints/dragpoint")[0].attrib["toShape"]
    assert by_name[fwd].attrib["shapetype"] == "catcherrors"


def test_connector_scope_passthrough_no_transform():
    cfg = _connector_config(retry_count=1, transform={"mode": "passthrough"})
    _root, shapes = _parse_shapes(ProcessFlowBuilder.build(cfg, name="N"))
    assert _by_type(shapes) == [
        "start", "catcherrors", "connectoraction", "catcherrors",
        "connectoraction", "stop", "doccacheload", "doccacheload",
    ]


def test_process_scope_explicit_equals_default():
    """try_catch_scope='process' must be byte-identical to omitting the key —
    the legacy whole-process wrapper is preserved unchanged."""
    base = _config({"mode": "document_cache_ref", "document_cache_id": _CACHE_ID})
    explicit = _config({"mode": "document_cache_ref", "document_cache_id": _CACHE_ID})
    explicit["reliability"]["try_catch_scope"] = "process"
    assert ProcessFlowBuilder.build(base, name="X") == ProcessFlowBuilder.build(
        explicit, name="X"
    )


def test_connector_scope_each_leg_routes_to_dlq_cache():
    cfg = _connector_config(retry_count=2)
    _root, shapes = _parse_shapes(ProcessFlowBuilder.build(cfg, name="N"))
    legs = [s for s in shapes if s.attrib["shapetype"] == "doccacheload"]
    assert len(legs) == 2
    for leg in legs:
        assert leg.find("configuration/doccacheload").attrib["docCache"] == _CACHE_ID


def test_invalid_try_catch_scope_rejected():
    cfg = _config({"mode": "document_cache_ref", "document_cache_id": _CACHE_ID})
    cfg["reliability"]["try_catch_scope"] = "bogus"
    err = ProcessFlowBuilder.validate_config(cfg, depends_on=[])
    assert err is not None
    assert err.error_code == "PROCESS_RETRY_UNVERIFIED"
    assert err.field == "reliability.try_catch_scope"


# ---------------------------------------------------------------------------
# Issue #108 M10.4 — Exception (Throw) catch-leg terminal
# ---------------------------------------------------------------------------

_EXCEPTION_FIXTURE = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "golden_xml"
    / "exception_catch_path.xml"
)


_exc_config = _corpus.dlq_exc_config


def test_exception_catch_path_matches_golden_fixture():
    """The canonical bare catch -> exception build must match the committed golden
    (compared via C14N canonicalization — attribute ordering is not brittle)."""
    cfg = _exc_config(dict(_corpus.DLQ_EXCEPTION_CATCH))
    emitted = ProcessFlowBuilder.build(
        cfg, name="Exception Catch Path", folder_name="Golden/Fixtures"
    )
    expected = _EXCEPTION_FIXTURE.read_text()
    assert emitted == expected


def test_exception_terminal_after_dlq_route():
    cfg = _exc_config(
        {"message_template": "halt {1}", "parameter_source": "current_document"},
        dlq={"mode": "document_cache_ref", "document_cache_id": _CACHE_ID},
    )
    _root, shapes = _parse_shapes(ProcessFlowBuilder.build(cfg, name="P"))
    types = _by_type(shapes)
    # catch leg: doccacheload -> exception (exception is the leg terminal).
    assert "doccacheload" in types and "exception" in types
    dlq_shape = next(s for s in shapes if s.attrib["shapetype"] == "doccacheload")
    ex = next(s for s in shapes if s.attrib["shapetype"] == "exception")
    dp = dlq_shape.find("dragpoints/dragpoint")
    assert dp is not None and dp.attrib["toShape"] == ex.attrib["name"]
    # Only the normal Try-path Stop remains; the catch leg throws (no catch Stop).
    assert types.count("stop") == 1


def test_exception_connector_scope_throws_on_both_legs():
    cfg = _exc_config(
        {"message_template": "boom", "parameter_source": "none"},
        scope="connector",
    )
    _root, shapes = _parse_shapes(ProcessFlowBuilder.build(cfg, name="P"))
    types = _by_type(shapes)
    # Two catcherrors (source + target), each catch leg ends in its own Exception.
    assert types.count("catcherrors") == 2
    assert types.count("exception") == 2


def test_exception_leg_is_not_a_bare_stop_branch():
    # The catcherrors Catch dragpoint must target the Exception, never a Stop —
    # this is what keeps the catch leg CONTROL_BRANCH_BARE_STOP-clean (#108).
    cfg = _exc_config({"message_template": "halt {1}", "parameter_source": "caught_error"})
    _root, shapes = _parse_shapes(ProcessFlowBuilder.build(cfg, name="P"))
    by_name = {s.attrib["name"]: s for s in shapes}
    ce = next(s for s in shapes if s.attrib["shapetype"] == "catcherrors")
    catch_dp = next(d for d in ce.find("dragpoints") if d.attrib.get("identifier") == "error")
    target = by_name[catch_dp.attrib["toShape"]]
    assert target.attrib["shapetype"] == "exception"


# ---------------------------------------------------------------------------
# #175 — the SECOND instance of the defect class, in the legacy catch leg.
#
# An `error_subprocess_ref` DLQ route emits a process call, and the leg used to
# wire that call to whatever trailing terminal the composition produced. Measured
# at the pre-fix baseline, three compositions carried the defect and one did not:
#
#     bare error_subprocess_ref              -> clean (already terminal)
#     catch_notify + error_subprocess_ref    -> notify -> pc -> stop      DEFECT
#     error_subprocess_ref + catch_exception -> pc -> exception           DEFECT
#     notify + err_sub + catch_exception     -> notify -> pc -> exception DEFECT
#
# NONE of them was covered by a test that ran the graph verifier, which is why
# the suite stayed green while every affected build emitted the orphan. Closing
# that gap is what these tests do.
# ---------------------------------------------------------------------------

from boomi_mcp.categories.components.process_graph_verifier import verify_process_graph

_CATCH_EXCEPTION = {"message_template": "{1}", "parameter_source": "caught_error"}


def _dlq_cfg(dlq, *, notify=False, exception=False):
    cfg = _config(dlq, catch_notify=_CATCH_NOTIFY if notify else None)
    if exception:
        cfg["reliability"]["catch_exception"] = dict(_CATCH_EXCEPTION)
    return cfg


def _orphan_shapes(xml):
    return sorted(
        i["shape"] for i in verify_process_graph(xml)["errors"]
        if i["code"] == "PROCESS_CALL_ORPHAN_CONTINUATION"
    )


@pytest.mark.parametrize("notify,label", [(False, "bare"), (True, "notify")])
def test_an_error_subprocess_catch_leg_emits_no_orphan_continuation(notify, label):
    xml = ProcessFlowBuilder.build(
        _dlq_cfg({"mode": "error_subprocess_ref", "process_id": _PROC_ID}, notify=notify),
        name="N",
    )
    assert _orphan_shapes(xml) == [], label
    # ...and the call really is the last shape, so this does not pass because the
    # process call vanished along with its edge.
    _, shapes = _parse_shapes(xml)
    assert shapes[-1].attrib["shapetype"] == "processcall", label
    assert list(shapes[-1].find("dragpoints")) == [], label


@pytest.mark.parametrize("notify,label", [(False, "exception"), (True, "notify+exception")])
def test_an_exception_after_an_error_subprocess_call_is_refused(notify, label):
    """Refused rather than silently dropped.

    A `catch_exception` is authored INTENT — "hand off to the subprocess, then
    throw" — and that cannot be expressed while the call is terminal. Emitting
    the leg without the Exception would lose the intent silently, so the
    composition fails typed, and before anything is created.
    """
    cfg = _dlq_cfg(
        {"mode": "error_subprocess_ref", "process_id": _PROC_ID},
        notify=notify, exception=True,
    )
    with pytest.raises(BuilderValidationError) as excinfo:
        ProcessFlowBuilder.build(cfg, name="N")
    assert excinfo.value.error_code == "PROCESS_CALL_CONFIG_INVALID", label
    assert excinfo.value.field == "reliability.catch_exception", label


@pytest.mark.parametrize(
    "notify,exception,label",
    [(False, False, "bare"), (True, False, "notify"), (False, True, "exception")],
)
def test_the_document_cache_dlq_route_is_untouched(notify, exception, label):
    """The over-firing control, and the discriminator for the rule itself.

    A Document Cache load legitimately continues downstream — it is deliberately
    excluded from the always-terminal set — so the SAME trailing shapes that make
    the process-call compositions fail must leave this route exactly as it was.
    A guard keyed on "something followed a shape", rather than on the declared
    return path, would break all three of these.
    """
    xml = ProcessFlowBuilder.build(
        _dlq_cfg({"mode": "document_cache_ref", "document_cache_id": _CACHE_ID},
                 notify=notify, exception=exception),
        name="N",
    )
    assert _orphan_shapes(xml) == [], label
    types = _by_type(_parse_shapes(xml)[1])
    assert "doccacheload" in types, label
    # The cache route keeps whatever terminal its composition implies: none at
    # all when it is itself the end of the leg, else the Stop or the Exception.
    expected_last = "doccacheload" if not (notify or exception) else ("exception" if exception else "stop")
    assert types[-1] == expected_last, (label, types)


# ---------------------------------------------------------------------------
# #175 QA-175-r1-01 — the two enforcement sites must agree, over the WHOLE
# composition matrix rather than over the one pair that was reported.
#
# The first version of the rule lived only in `_emit_catch_leg`, so
# `validate_config` accepted `error_subprocess_ref` + `catch_exception`, `plan`
# reported success, and apply created the preceding components before the build
# refused. Live QA measured that: inventory 26 -> 27 before the refusal.
#
# The matrix is DERIVED from the authority's own case set — every supported DLQ
# mode crossed with notify/exception presence — not from a hand-listed pair, so
# a mode added later is covered without editing this test.
# ---------------------------------------------------------------------------

def test_both_enforcement_sites_agree_on_every_catch_composition():
    from src.boomi_mcp.categories.components.builders import process_flow_builder as _pfb

    modes = {
        "disabled": None,
        "document_cache_ref": {"mode": "document_cache_ref", "document_cache_id": _CACHE_ID},
        "error_subprocess_ref": {"mode": "error_subprocess_ref", "process_id": _PROC_ID},
    }
    # Every mode the module supports must appear above, or the matrix silently
    # stops covering one.
    assert set(modes) - {"disabled"} == set(_pfb._TRY_CATCH_DLQ_MODES), sorted(
        set(_pfb._TRY_CATCH_DLQ_MODES) ^ (set(modes) - {"disabled"})
    )

    refused_at_plan, refused_at_build = set(), set()
    for mode_name, dlq in sorted(modes.items()):
        for notify in (False, True):
            for exception in (False, True):
                if mode_name == "disabled" and not exception:
                    continue  # no catch path at all — not a composition
                cell = (mode_name, notify, exception)
                cfg = _config(dlq or {"mode": "disabled"},
                              catch_notify=_CATCH_NOTIFY if notify else None)
                if exception:
                    cfg["reliability"]["catch_exception"] = dict(_CATCH_EXCEPTION)

                err = ProcessFlowBuilder.validate_config(cfg)
                if err is not None and err.error_code == "PROCESS_CALL_CONFIG_INVALID":
                    refused_at_plan.add(cell)
                try:
                    ProcessFlowBuilder.build(cfg, name="N")
                except BuilderValidationError as exc:
                    if exc.error_code == "PROCESS_CALL_CONFIG_INVALID":
                        refused_at_build.add(cell)

    # The agreement property — the whole point of the shared definition.
    assert refused_at_plan == refused_at_build, {
        "plan-only (apply would mutate first)": sorted(refused_at_plan ^ refused_at_build
                                                       & refused_at_plan),
        "build-only": sorted(refused_at_build - refused_at_plan),
    }
    # ...and NON-VACUITY: the matrix must actually contain refusals, or the
    # equality above holds trivially with two empty sets.
    assert refused_at_plan == {
        ("error_subprocess_ref", False, True),
        ("error_subprocess_ref", True, True),
    }, sorted(refused_at_plan)


def test_the_composition_rule_has_exactly_one_definition():
    """Both sites route through `_process_call_catch_composition_error`.

    Asserted by MUTATION rather than by reading the source: neutralise the one
    function and BOTH enforcement sites must stop refusing. If either kept
    refusing, it would be carrying its own copy of the rule — which is the
    defect this structure exists to make impossible.
    """
    from src.boomi_mcp.categories.components.builders import process_flow_builder as _pfb

    cfg = _dlq_cfg({"mode": "error_subprocess_ref", "process_id": _PROC_ID}, exception=True)
    assert ProcessFlowBuilder.validate_config(cfg) is not None
    with pytest.raises(BuilderValidationError):
        ProcessFlowBuilder.build(cfg, name="N")

    original = _pfb._process_call_catch_composition_error
    try:
        _pfb._process_call_catch_composition_error = lambda *a, **k: None
        assert ProcessFlowBuilder.validate_config(cfg) is None, (
            "plan-time site kept refusing with the shared rule neutralised — it "
            "carries its own copy"
        )
        # The build stops refusing on the COMPOSITION rule too — but it does not
        # sail through, because a third and independent boundary catches the
        # same shape one layer down: the shared renderer refuses to wire a
        # process call to a following shape at all. That is defence in depth,
        # not a duplicated composition rule, and the two are told apart by WHICH
        # field the refusal names.
        with pytest.raises(BuilderValidationError) as deeper:
            ProcessFlowBuilder.build(cfg, name="N")
        assert deeper.value.field == "reliability.dlq.mode", deeper.value.field
        assert "wired to a following shape" in str(deeper.value)
    finally:
        _pfb._process_call_catch_composition_error = original

    # ...and the restore is real, not just the finally running.
    assert ProcessFlowBuilder.validate_config(cfg) is not None
