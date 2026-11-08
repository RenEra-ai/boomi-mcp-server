"""Round-8 correction: an observation must be CORRELATED to the connector under test.

The defect class this file pins had three instances in one slice, each looking like
a fix for the last: an artifact chosen by sort order, a flag read from the first
record, and then a flag aggregated over every record in the execution. All three
agreed with the archive, because every archived capture runs its connector
downstream of a `nodata` start shape — so the archive could not tell a correlated
read from an uncorrelated one, and three rounds of review passed on that agreement.

These tests supply the case the archive lacks.
"""

from __future__ import annotations

_TESTS_ROOT_FOR_CLIENT = __import__('pathlib').Path(__file__).resolve().parent


def _evidenced_client():
    """A client that REPORTS the capture's account — see `evidenced_account_client`.

    Every witness here used a bare `MagicMock()`, whose account attribute is a
    Mock rather than a string, so the account reader found none and the scope
    check was skipped rather than failed. That is the fail-open arm the
    issue-level architect gate found, and these witnesses are why it stayed
    invisible: they proved the evidenced path in the one configuration where the
    check did not run.
    """
    import sys
    sys.path.insert(0, str(_TESTS_ROOT_FOR_CLIENT))
    from _m12_11_support import evidenced_account_client

    return evidenced_account_client()


import ast
import contextlib
import json
import os
import shutil
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

from boomi_mcp.connector_replay.capture import (
    _connector_rows_under_test,
    _platform_connector_records,
    _start_shape_of_the_exercised_connector,
    summarize,
)
from boomi_mcp.connector_replay.models import EXECUTION_SENTINELS

_CAPTURES = Path(__file__).resolve().parents[1] / "tests/fixtures/boomi/connector_replay/captures"
_SOURCE = "cap155-e5-patch-attested"


def _entry_connector_capture(tmp_path: Path) -> Path:
    """A capture whose CONNECTOR UNDER TEST is the process entry.

    Every archived capture runs its verb downstream, so this shape does not exist in
    the archive and has to be constructed. Only the flag on the PATCH rows moves; the
    source GET stays downstream, which is exactly the mixed-placement execution the
    uncorrelated reads could not describe.
    """
    dst = tmp_path / "entry-connector"
    shutil.copytree(_CAPTURES / _SOURCE, dst)
    patch_step = None
    for xml in dst.glob("*.xml"):
        text = xml.read_text()
        if 'customOperationType="PATCH"' in text:
            patch_step = text.split('name="', 1)[1].split('"', 1)[0]
    assert patch_step, "fixture precondition: the source capture declares a PATCH component"

    for path in dst.glob("*.json"):
        payload = json.loads(path.read_text())
        touched = False
        for record in _platform_connector_records(payload):
            if record.get("executionConnector") == patch_step:
                record["isStartShape"] = True
                touched = True
        if touched:
            path.write_text(json.dumps(payload))
    return dst


def test_placement_is_read_from_the_connector_under_test_not_the_execution(tmp_path):
    """THE WITNESS: a mixed-placement execution resolves to the tested connector."""
    capture = _entry_connector_capture(tmp_path)
    assert summarize(capture, "PATCH").is_start_shape is True
    # And the SOURCE connector in the same capture still reads downstream — the two
    # answers differ, which is the whole point: one execution, two placements.
    assert summarize(capture, "GET").is_start_shape is False


def test_the_witness_is_not_vacuous_the_uncorrelated_read_cannot_answer_it(tmp_path):
    """CONTROL: the superseded aggregation is ambiguous on the very same capture.

    Without this the witness above would pass for a fix that changed nothing. The
    mutant is the previous implementation, spelled out rather than imported: every
    non-sentinel row in the execution, aggregated.
    """
    capture = _entry_connector_capture(tmp_path)
    summary = summarize(capture, "PATCH")
    execution_ids = frozenset(summary.execution_ids)

    mutant_flags = set()
    for path in sorted(capture.glob("*.json")):
        for record in _platform_connector_records(json.loads(path.read_text())):
            if record.get("executionId") not in execution_ids:
                continue
            if record.get("connectorType") in EXECUTION_SENTINELS:
                continue
            value = record.get("isStartShape")
            if isinstance(value, bool):
                mutant_flags.add(value)

    assert mutant_flags == {True, False}, "the fixture must be genuinely mixed"
    # The mutant returns None here — which `_capture_reference` published as
    # DOWNSTREAM, the exact false machine-served claim this round removed.
    assert _start_shape_of_the_exercised_connector(
        sorted(capture.iterdir()), execution_ids, "PATCH"
    ) is True


def test_an_uncorrelatable_action_refuses_rather_than_defaulting(tmp_path):
    """A verb no component declares correlates to nothing, and is refused."""
    capture = _entry_connector_capture(tmp_path)
    files = sorted(capture.iterdir())
    execution_ids = frozenset(summarize(capture, "PATCH").execution_ids)

    assert _connector_rows_under_test(files, execution_ids, "PUT") is None
    assert _start_shape_of_the_exercised_connector(files, execution_ids, "PUT") is None
    # No hint at all is equally unresolved — it must not fall back to scanning.
    assert _connector_rows_under_test(files, execution_ids, None) is None


def test_unresolved_placement_never_reaches_a_served_row(tmp_path):
    """The fail-open half: None must refuse, not publish DOWNSTREAM."""
    from boomi_mcp.connector_replay.ingest import IngestRefused, _capture_reference
    from boomi_mcp.connector_replay.models import SideEffectV1

    summary = summarize(_CAPTURES / _SOURCE, "PATCH").model_copy(
        update={"is_start_shape": None}
    )
    with pytest.raises(IngestRefused, match="placement is unresolved"):
        _capture_reference(summary, SideEffectV1.WRITE)


@pytest.mark.parametrize(
    "scenario,action",
    [
        ("cap155-e5-patch-attested", "PATCH"),
        ("cap155-e5-delete-attested", "DELETE"),
        ("cap155-e3b-patch-canonical", "PATCH"),
        ("cap155-e2-post", "POST"),
        ("cap155-e4-head-status", "HEAD"),
    ],
)
def test_the_correlation_still_resolves_every_archived_capture(scenario, action):
    """NOT A DENIAL: correlating must not refuse the evidence it protects.

    Three of this slice's defects were introduced by corrections that over-refused,
    including one that rejected the archived PATCH captures outright.
    """
    rows = _connector_rows_under_test(
        sorted((_CAPTURES / scenario).iterdir()),
        frozenset(summarize(_CAPTURES / scenario, action).execution_ids),
        action,
    )
    assert rows, f"{scenario}: the connector under test resolved to no platform row"
    assert summarize(_CAPTURES / scenario, action).is_start_shape is False


# --------------------------------------------------------------------------
# The sweep the structural fix above owed, and did not pay on its first pass:
# `capture.py` was swept, `ingest.py` was not, and the same class was sitting in
# it. These pin the corrected siblings.
# --------------------------------------------------------------------------


def test_connector_counts_come_from_the_connector_not_the_execution(tmp_path):
    """WITNESS for the served input/output claims.

    Zeroing the connector under test must change what is published about it. Under
    the process-level read it did not: the execution's counts are sums over every
    connector that ran, so the source read's document kept the claim alive.
    """
    from boomi_mcp.connector_replay.ingest import _input_observation
    from boomi_mcp.connector_replay.models import InputObservationV1

    dst = tmp_path / "zeroed"
    shutil.copytree(_CAPTURES / _SOURCE, dst)
    patch_step = None
    for xml in dst.glob("*.xml"):
        text = xml.read_text()
        if 'customOperationType="PATCH"' in text:
            patch_step = text.split('name="', 1)[1].split('"', 1)[0]

    for path in dst.glob("*.json"):
        payload = json.loads(path.read_text())
        touched = False
        for record in _platform_connector_records(payload):
            if record.get("executionConnector") == patch_step:
                record["successCount"] = 0
                record["errorCount"] = 0
                touched = True
        if touched:
            path.write_text(json.dumps(payload))

    zeroed = summarize(dst, "PATCH")
    assert zeroed.connector_documents == 0
    assert zeroed.connector_successful_documents == 0
    assert _input_observation(zeroed) is InputObservationV1.NO_INBOUND_DOCUMENTS

    # CONTROL: the execution's own counts are untouched and still non-zero, which
    # is exactly why the process-level read could not see this.
    assert any((r.inbound_documents or 0) > 0 for r in zeroed.runs)

    # CONTROL, the other way: unmodified, the same capture still reports consumption.
    intact = summarize(_CAPTURES / _SOURCE, "PATCH")
    assert _input_observation(intact) is InputObservationV1.DOCUMENTS_CONSUMED


def test_an_entry_connector_consumes_nothing_by_construction(tmp_path):
    """Placement decides the input claim where the graph decides the fact."""
    from boomi_mcp.connector_replay.ingest import _input_observation
    from boomi_mcp.connector_replay.models import InputObservationV1

    entry = summarize(_entry_connector_capture(tmp_path), "PATCH")
    assert entry.is_start_shape is True
    assert _input_observation(entry) is InputObservationV1.NO_INBOUND_DOCUMENTS

    downstream = summarize(_CAPTURES / _SOURCE, "PATCH")
    assert _input_observation(downstream) is InputObservationV1.DOCUMENTS_CONSUMED


def test_the_selector_returns_each_platform_row_once(tmp_path):
    """The archive queries twice; a counting consumer must not see double."""
    files = sorted((_CAPTURES / _SOURCE).iterdir())
    summary = summarize(_CAPTURES / _SOURCE, "PATCH")
    rows = _connector_rows_under_test(files, frozenset(summary.execution_ids), "PATCH")

    ids = [r["id"] for r in rows]
    assert len(ids) == len(set(ids)), "the same platform row was returned twice"
    # One row per execution, not one per archived query — the source capture
    # archives its connector rows in two files.
    assert len(rows) == len(summary.execution_ids) == 2
    assert summary.connector_documents == 2


def test_two_components_sharing_a_name_are_ambiguous_not_unique(tmp_path):
    """Resolution counts COMPONENTS, because the platform lets names collide.

    Measured live: component CREATE uniquifies a duplicate name, component UPDATE
    does not — so two ids can carry one name, and deduping by name would call that
    uniquely resolved.
    """
    from boomi_mcp.connector_replay.capture import _component_name_under_test

    dst = tmp_path / "name-collision"
    shutil.copytree(_CAPTURES / _SOURCE, dst)
    original = next(p for p in dst.glob("*.xml") if 'customOperationType="PATCH"' in p.read_text())
    text = original.read_text()
    component_id = text.split('componentId="', 1)[1].split('"', 1)[0]
    twin = component_id[:-1] + ("0" if component_id[-1] != "0" else "1")
    (dst / "component_op_twin.xml").write_text(text.replace(component_id, twin, 1))

    files = sorted(dst.iterdir())
    assert _component_name_under_test(files, "PATCH") is None
    # CONTROL: the untouched capture still resolves, so the refusal is about the
    # collision and not about the extra file.
    assert _component_name_under_test(sorted((_CAPTURES / _SOURCE).iterdir()), "PATCH")


# --------------------------------------------------------------------------
# Round 9. Four of these five are the SAME class again, one level in: a guard
# that protects the resolution and leaves the join open, a predicate that reads
# bytes where the vocabulary counts documents, a missing value read as an
# absence, and a duplicate resolved by filename order.
# --------------------------------------------------------------------------


def _rename_source_onto_target(tmp_path: Path) -> Path:
    """A capture where the SOURCE component carries the TARGET's name.

    The platform permits it: component CREATE uniquifies a duplicate name, component
    UPDATE does not. The two components keep different verbs and different ids.
    """
    dst = tmp_path / "name-shared-across-verbs"
    shutil.copytree(_CAPTURES / _SOURCE, dst)
    target = next(p for p in dst.glob("*.xml") if 'customOperationType="PATCH"' in p.read_text())
    source = next(p for p in dst.glob("*.xml") if 'customOperationType="GET"' in p.read_text())
    target_name = target.read_text().split('name="', 1)[1].split('"', 1)[0]
    source_text = source.read_text()
    source_name = source_text.split('name="', 1)[1].split('"', 1)[0]
    source.write_text(source_text.replace(f'name="{source_name}"', f'name="{target_name}"', 1))
    return dst


def test_a_name_shared_across_verbs_is_refused_not_joined(tmp_path):
    """The JOIN must be as narrow as the resolution claims to be.

    Guarding only the components that declare the requested verb left this open:
    exactly one component declares PATCH, so resolution succeeded, and the row filter
    then admitted the source's rows too — publishing the source's documents as the
    target's observations. Placement would not have caught it, because both run
    downstream.
    """
    from boomi_mcp.connector_replay.capture import _component_name_under_test

    dst = _rename_source_onto_target(tmp_path)
    files = sorted(dst.iterdir())
    assert _component_name_under_test(files, "PATCH") is None
    assert summarize(dst, "PATCH").connector_documents is None

    # CONTROL: one component still declares PATCH, so the refusal is about the
    # shared name and not about the capture having become malformed.
    patch_components = [
        p for p in dst.glob("*.xml") if 'customOperationType="PATCH"' in p.read_text()
    ]
    assert len(patch_components) == 1


def test_counts_are_documents_not_bytes(tmp_path):
    """A successful call that returns an empty body still handled a document.

    The counts must not be byte figures: the archived HEAD capture records a
    successful connector document of zero bytes, so a byte test would report that the
    connector handled nothing. What that document then went on to reach is a
    different question, answered by the receiver — see the return-delivery test.
    """
    head = summarize(_CAPTURES / "cap155-e4-head-status", "HEAD")
    assert head.connector_successful_documents == 1

    documents = json.loads(
        (_CAPTURES / "cap155-e4-head-status" / "connector_documents.json").read_text()
    )
    assert any(d["resp"]["size_bytes"] == 0 and d["resp"]["_success"] for d in documents), (
        "the fixture must actually contain a successful zero-byte document, or this "
        "test would pass for the wrong reason"
    )


def test_a_missing_count_is_unknown_and_refused_not_zero(tmp_path):
    """An incomplete capture must not assert that nothing was consumed."""
    from boomi_mcp.connector_replay.capture import CaptureRefused

    dst = tmp_path / "missing-counts"
    shutil.copytree(_CAPTURES / _SOURCE, dst)
    patch_step = None
    for xml in dst.glob("*.xml"):
        text = xml.read_text()
        if 'customOperationType="PATCH"' in text:
            patch_step = text.split('name="', 1)[1].split('"', 1)[0]
    for path in dst.glob("*.json"):
        payload = json.loads(path.read_text())
        touched = False
        for record in _platform_connector_records(payload):
            if record.get("executionConnector") == patch_step:
                record.pop("successCount", None)
                touched = True
        if touched:
            path.write_text(json.dumps(payload))

    with pytest.raises(CaptureRefused, match="successCount"):
        summarize(dst, "PATCH")


def test_a_negative_count_is_refused(tmp_path):
    """These feed `> 0` absence decisions, so a negative must not sum back under."""
    from boomi_mcp.connector_replay.capture import CaptureRefused

    dst = tmp_path / "negative-count"
    shutil.copytree(_CAPTURES / _SOURCE, dst)
    patch_step = None
    for xml in dst.glob("*.xml"):
        text = xml.read_text()
        if 'customOperationType="PATCH"' in text:
            patch_step = text.split('name="', 1)[1].split('"', 1)[0]
    for path in dst.glob("*.json"):
        payload = json.loads(path.read_text())
        touched = False
        for record in _platform_connector_records(payload):
            if record.get("executionConnector") == patch_step:
                record["successCount"] = -1
                touched = True
        if touched:
            path.write_text(json.dumps(payload))

    with pytest.raises(CaptureRefused, match="negative"):
        summarize(dst, "PATCH")


def test_two_copies_of_one_row_that_disagree_are_refused(tmp_path):
    """A served observation must not depend on which file sorted first."""
    from boomi_mcp.connector_replay.capture import CaptureRefused

    dst = tmp_path / "disagreeing-copies"
    shutil.copytree(_CAPTURES / _SOURCE, dst)
    patch_step = None
    for xml in dst.glob("*.xml"):
        text = xml.read_text()
        if 'customOperationType="PATCH"' in text:
            patch_step = text.split('name="', 1)[1].split('"', 1)[0]

    edited = 0
    for path in sorted(dst.glob("*.json"), reverse=True):
        payload = json.loads(path.read_text())
        touched = False
        for record in _platform_connector_records(payload):
            if record.get("executionConnector") == patch_step:
                record["successCount"] = (record.get("successCount") or 0) + 5
                touched = True
        if touched:
            path.write_text(json.dumps(payload))
            edited += 1
            break
    assert edited == 1, "the source capture must archive its rows in more than one file"

    with pytest.raises(CaptureRefused, match="disagree"):
        summarize(dst, "PATCH")


def test_the_archive_itself_has_no_disagreeing_copies():
    """CONTROL for the refusal above: it must not fire on the real archive.

    Measured: 34 duplicate row copies across the archive, none disagreeing.
    """
    from boomi_mcp.connector_replay.capture import CaptureRefused

    for scenario, action in [
        ("cap155-e5-patch-attested", "PATCH"),
        ("cap155-e5-delete-attested", "DELETE"),
        ("cap155-e3b-patch-canonical", "PATCH"),
    ]:
        try:
            assert summarize(_CAPTURES / scenario, action).connector_documents is not None
        except CaptureRefused as exc:  # pragma: no cover - a real regression
            pytest.fail(f"{scenario}: the reconciliation refused live evidence: {exc}")


# --------------------------------------------------------------------------
# Round 31 QA. The reconciliation field set was a hand-model of what the readers
# consume, and it had already drifted twice — one member unreachable, one reader
# omitted. The guard below derives the truth from the module instead.
# --------------------------------------------------------------------------


def test_the_reconciled_field_set_equals_what_the_module_actually_reads():
    """STRUCTURAL: the set is pinned to OBSERVED reads, not to a parse of read forms.

    The first version of this guard parsed the module for `<name>.get("literal")` over
    a hand-listed set of receiver names — which is the very class it was written to
    close, one level up. It was blind to a subscript, to a receiver name not on the
    list, to a comprehension variable and to a module-level key constant; and the
    module already reads rows by subscript, so two fields were reconciled only by the
    accident of also being spelled `.get()` somewhere else.

    This records the reads as they HAPPEN instead. A proxy stands in for every
    platform row and notes each key asked of it, so no read form can hide: whatever
    syntax reaches a row, the row itself sees the key.

    But observing every read includes the RECONCILER'S OWN, and the reconciler asks
    each row for every member of the set under test — so a stale member recorded
    itself and the equality assertion passed for it. Measured: a planted
    `totallyUnusedField` survived. The sweep therefore runs twice, once with the set
    emptied so the reconciler reads nothing, and the CONSUMER reads are what the
    second run saw.
    """
    from boomi_mcp.connector_replay import capture as module

    observed: set[str] = set()

    class _RecordingRow(dict):
        def __getitem__(self, key):
            observed.add(key)
            return super().__getitem__(key)

        def get(self, key, default=None):
            observed.add(key)
            return super().get(key, default)

    real_records = module._platform_connector_records
    real_fields = module._CORRELATED_ROW_FIELDS

    def recording_records(payload):
        return [_RecordingRow(row) for row in real_records(payload)]

    module._platform_connector_records = recording_records
    # Emptied for the duration: with nothing to reconcile, every recorded read is a
    # read some observation actually wanted.
    module._CORRELATED_ROW_FIELDS = ()
    try:
        for directory in sorted(_CAPTURES.iterdir()):
            if not directory.is_dir():
                continue
            verbs = set()
            for xml in directory.glob("*.xml"):
                try:
                    root = ET.parse(xml).getroot()
                except ET.ParseError:
                    continue
                verbs |= {
                    e.get("customOperationType")
                    for e in root.iter()
                    if e.get("customOperationType")
                }
            for verb in sorted(verbs) or [None]:
                try:
                    summarize(directory, verb)
                except Exception:
                    # A refused capture still exercised its reads on the way to the
                    # refusal, which is exactly what is being collected here.
                    pass
    finally:
        module._platform_connector_records = real_records
        module._CORRELATED_ROW_FIELDS = real_fields

    # `id` is the reconciliation KEY, not a reconciled value: two copies that
    # disagree on it are two different rows, which is not a conflict.
    observed.discard("id")

    assert observed, "no reads were recorded, so the comparison would be vacuous"
    assert "connectorType" in observed and "successCount" in observed, (
        "the isolation removed too much: these are consumer reads and must survive it"
    )
    assert set(module._CORRELATED_ROW_FIELDS) == observed, (
        "the reconciled field set and the module's observed reads have drifted: "
        f"only reconciled {sorted(set(module._CORRELATED_ROW_FIELDS) - observed)}, "
        f"only read {sorted(observed - set(module._CORRELATED_ROW_FIELDS))}"
    )


def test_copies_disagreeing_on_the_join_field_are_refused(tmp_path):
    """The clause that could not fire before the reconciler moved ahead of the join.

    Two copies of one row disagreeing about WHICH step it belongs to never met while
    reconciliation ran after filtering — they were filtered apart first — so the
    disagreement resolved silently toward the connector under test.
    """
    from boomi_mcp.connector_replay.capture import CaptureRefused

    dst = tmp_path / "join-field-disagreement"
    shutil.copytree(_CAPTURES / _SOURCE, dst)
    patch_step = None
    for xml in dst.glob("*.xml"):
        text = xml.read_text()
        if 'customOperationType="PATCH"' in text:
            patch_step = text.split('name="', 1)[1].split('"', 1)[0]

    for path in sorted(dst.glob("*.json"), reverse=True):
        payload = json.loads(path.read_text())
        touched = False
        for record in _platform_connector_records(payload):
            if record.get("executionConnector") == patch_step:
                record["executionConnector"] = "_SOME_OTHER_STEP"
                touched = True
        if touched:
            path.write_text(json.dumps(payload))
            break

    with pytest.raises(CaptureRefused, match="executionConnector"):
        summarize(dst, "PATCH")


def test_a_non_operation_component_does_not_contest_the_name(tmp_path):
    """The ownership gate is scoped to components that can produce a connector row."""
    from boomi_mcp.connector_replay.capture import _component_name_under_test

    dst = tmp_path / "process-shares-the-name"
    shutil.copytree(_CAPTURES / _SOURCE, dst)
    target = next(p for p in dst.glob("*.xml") if 'customOperationType="PATCH"' in p.read_text())
    name = target.read_text().split('name="', 1)[1].split('"', 1)[0]
    (dst / "component_process.xml").write_text(
        '<Component xmlns="http://api.platform.boomi.com/" '
        f'componentId="00000000-0000-4000-8000-000000000001" name="{name}" '
        'type="process"><object/></Component>'
    )

    assert _component_name_under_test(sorted(dst.iterdir()), "PATCH") == name

    # CONTROL: a second OPERATION sharing the name still refuses, so the narrowing
    # did not reopen the collision it was added to close.
    twin = target.read_text()
    component_id = twin.split('componentId="', 1)[1].split('"', 1)[0]
    other = component_id[:-1] + ("0" if component_id[-1] != "0" else "1")
    (dst / "component_op_twin.xml").write_text(twin.replace(component_id, other, 1))
    assert _component_name_under_test(sorted(dst.iterdir()), "PATCH") is None


def test_the_missing_count_refusal_survives_a_brace_bearing_row_id(tmp_path):
    """The message must not be re-formatted after interpolation."""
    from boomi_mcp.connector_replay.capture import CaptureRefused, _connector_document_counts

    dst = tmp_path / "brace-id"
    shutil.copytree(_CAPTURES / _SOURCE, dst)
    patch_step = None
    for xml in dst.glob("*.xml"):
        text = xml.read_text()
        if 'customOperationType="PATCH"' in text:
            patch_step = text.split('name="', 1)[1].split('"', 1)[0]
    for path in dst.glob("*.json"):
        payload = json.loads(path.read_text())
        touched = False
        for record in _platform_connector_records(payload):
            if record.get("executionConnector") == patch_step:
                record["id"] = "row-{0}-{name}"
                record.pop("successCount", None)
                touched = True
        if touched:
            path.write_text(json.dumps(payload))

    with pytest.raises(CaptureRefused) as raised:
        summarize(dst, "PATCH")
    # The id reaches the operator intact, and no KeyError is raised in its place.
    assert "row-{0}-{name}" in str(raised.value)


def test_return_delivery_is_read_from_the_receiver_not_the_connector():
    """WITNESS: the two subjects disagree on the real archive.

    `return_documents_received` names a Return Documents shape receiving output. The
    connector's own success count says only that the connector finished — and for
    every archived read-verb capture those two answers differ, because the platform
    records a receiver row for the write captures and none for the read ones.
    """
    from boomi_mcp.connector_replay.ingest import _output_observation
    from boomi_mcp.connector_replay.models import OutputObservationV1

    delivered = summarize(_CAPTURES / _SOURCE, "PATCH")
    assert delivered.return_documents > 0
    assert _output_observation(delivered) is OutputObservationV1.RETURN_DOCUMENTS_RECEIVED

    for scenario, action in [
        ("cap155-e4-head-status", "HEAD"),
        ("cap155-e4-options-status", "OPTIONS"),
        ("cap155-e4-trace-status", "TRACE"),
    ]:
        summary = summarize(_CAPTURES / scenario, action)
        # The connector DID finish documents successfully — which is exactly why
        # reading its count published a receiver claim for a capture with no receiver.
        assert summary.connector_successful_documents > 0
        assert summary.return_documents == 0
        assert _output_observation(summary) is OutputObservationV1.NO_OUTPUT_OBSERVED


def test_a_receiver_row_that_received_nothing_is_not_a_delivery(tmp_path):
    """Presence of the receiver is not delivery; its own count decides."""
    from boomi_mcp.connector_replay.ingest import _output_observation
    from boomi_mcp.connector_replay.models import OutputObservationV1

    dst = tmp_path / "empty-receiver"
    shutil.copytree(_CAPTURES / _SOURCE, dst)
    for path in dst.glob("*.json"):
        payload = json.loads(path.read_text())
        touched = False
        for record in _platform_connector_records(payload):
            if record.get("connectorType") == "return":
                record["successCount"] = 0
                touched = True
        if touched:
            path.write_text(json.dumps(payload))

    summary = summarize(dst, "PATCH")
    assert summary.return_documents == 0
    assert summary.connector_successful_documents > 0
    assert _output_observation(summary) is OutputObservationV1.NO_OUTPUT_OBSERVED


def test_a_receiver_on_another_branch_is_not_this_operations_delivery(tmp_path):
    """Co-occurrence in one execution is not attribution.

    The archived processes are linear, so the receiver is always downstream and the
    archive cannot tell an attributed claim from an unattributed one. This builds the
    case it lacks: the operation under test runs on a branch that never reaches the
    Return Documents shape, while a receiver runs on the other one.
    """
    from boomi_mcp.connector_replay.capture import _first_across, _reachable_receivers
    from boomi_mcp.connector_replay.ingest import _output_observation
    from boomi_mcp.connector_replay.models import OutputObservationV1

    dst = tmp_path / "receiver-on-another-branch"
    shutil.copytree(_CAPTURES / _SOURCE, dst)
    process = dst / "component_process.xml"
    text = process.read_text()

    # shape3 is the PATCH; cut its edge to the receiver so nothing downstream of it
    # reaches shape4, while shape4 itself still exists and still ran.
    assert '<dragpoint name="shape3.dragpoint1" toShape="shape4"' in text or 'toShape="shape4"' in text
    process.write_text(text.replace('toShape="shape4"', 'toShape="shape3"', 1))

    files = sorted(dst.iterdir())
    assert _reachable_receivers(files, "PATCH", _first_across(files, "processId", str)) == frozenset()

    # The receiver still RAN and still reported — the platform rows are untouched —
    # but the graph now says this operation produces for no receiver, so the count is
    # not borrowed from it. A measured zero, not an unknown and not a delivery.
    summary = summarize(dst, "PATCH")
    assert summary.receiver_is_downstream is False
    assert summary.return_documents == 0
    assert _output_observation(summary) is OutputObservationV1.NO_OUTPUT_OBSERVED

    untouched = [
        row for path in dst.glob("*.json")
        for row in _platform_connector_records(json.loads(path.read_text()))
        if row.get("connectorType") == "return"
        and isinstance(row.get("successCount"), int)
        and row["successCount"] > 0
    ]
    assert untouched, "the receiver rows must still report documents, or this is vacuous"

    # CONTROL: unmodified, the same capture attributes the delivery.
    intact = sorted((_CAPTURES / _SOURCE).iterdir())
    assert _reachable_receivers(intact, "PATCH", _first_across(intact, "processId", str))


def test_a_receiver_with_no_countable_row_is_unknown_not_zero(tmp_path):
    """Half the archive's receiver rows never carry counts; absence must not read as zero."""
    from boomi_mcp.connector_replay.ingest import IngestRefused, _output_observation

    # The premise, measured rather than assumed: document-record receiver rows exist
    # in the real archive with no count on them at all.
    uncounted = 0
    for path in sorted((_CAPTURES / _SOURCE).glob("*.json")):
        for record in _platform_connector_records(json.loads(path.read_text())):
            if record.get("connectorType") == "return" and not isinstance(
                record.get("successCount"), int
            ):
                uncounted += 1
    assert uncounted, "the fixture must contain a countless receiver row"

    dst = tmp_path / "receiver-without-counts"
    shutil.copytree(_CAPTURES / _SOURCE, dst)
    for path in dst.glob("*.json"):
        payload = json.loads(path.read_text())
        touched = False
        for record in _platform_connector_records(payload):
            if record.get("connectorType") == "return":
                record.pop("successCount", None)
                touched = True
        if touched:
            path.write_text(json.dumps(payload))

    summary = summarize(dst, "PATCH")
    assert summary.return_documents is None, "an absent count must not become zero"
    with pytest.raises(IngestRefused, match="unknown rather than nothing"):
        _output_observation(summary)

    # CONTROL: a capture with NO receiver at all is a measured absence, not unknown.
    absent = summarize(_CAPTURES / "cap155-e4-head-status", "HEAD")
    assert absent.return_documents == 0


def test_delivery_is_attributed_to_the_producer_not_every_ancestor():
    """WITNESS from the real archive: reaching the receiver is not producing for it.

    No mutation needed — the archived captures already contain the discriminating
    case, and the previous rule got it wrong on live evidence: with the SOURCE verb
    selected, a linear process makes the source reach the receiver exactly as the
    operation under test does.
    """
    from boomi_mcp.connector_replay.capture import _first_across, _reachable_receivers

    for scenario, produced, merely_reaches in [
        ("cap155-e5-delete-attested", "DELETE", "GET"),
        ("cap155-e5-patch-attested", "PATCH", "GET"),
        ("cap155-e3b-patch-canonical", "PATCH", "GET"),
    ]:
        files = sorted((_CAPTURES / scenario).iterdir())
        process_id = _first_across(files, "processId", str)
        assert _reachable_receivers(files, produced, process_id), "the producer delivers"
        assert _reachable_receivers(files, merely_reaches, process_id) == frozenset(), (
            "an ancestor that merely reaches the receiver produces nothing for it"
        )


def test_the_archive_corroborates_which_operation_produced_the_delivery():
    """CONTROL for the rule above, from an independent field the rule never reads.

    The topological rule is deliberately not built on document sizes — equal payloads
    would break that the moment two steps moved the same bytes. But the sizes are in
    the archive, and they agree with the topology, which is what makes the topological
    answer checkable rather than merely self-consistent.
    """
    from boomi_mcp.connector_replay.capture import (
        _component_name_under_test,
        _reconciled_platform_rows,
    )

    directory = _CAPTURES / "cap155-e5-delete-attested"
    files = sorted(directory.iterdir())
    summary = summarize(directory, "DELETE")
    execution_ids = frozenset(summary.execution_ids)
    rows = _reconciled_platform_rows(files, execution_ids)

    target = _component_name_under_test(files, "DELETE")
    source = _component_name_under_test(files, "GET")

    def sizes(step):
        return sorted(
            r.get("size") for r in rows
            if r.get("executionConnector") == step and isinstance(r.get("size"), int)
        )

    receiver = sizes("shape4") or sorted(
        r.get("size") for r in rows
        if r.get("connectorType") == "return" and isinstance(r.get("size"), int)
    )
    assert receiver, "the receiver rows must carry sizes for this control to mean anything"
    assert receiver == sizes(target), "the receiver counted the target's documents"
    assert receiver != sizes(source), "and not the source's"


def test_the_graph_is_taken_from_the_process_that_actually_RAN(tmp_path):
    """The graph is selected by the executed process id, not by sort order.

    The first version of this test proved nothing: it used a capture with no operation
    component at all, so resolution returned None before the process id was ever read
    and the assertion held even if selection were ignored entirely. This one uses a
    capture that DOES resolve, and plants a decoy graph that sorts first and carries
    the same operation — so sort order and the execution record disagree, and only one
    of them gives the right answer.
    """
    from boomi_mcp.connector_replay.capture import _first, _load_json, _reachable_receivers

    dst = tmp_path / "decoy-graph"
    shutil.copytree(_CAPTURES / _SOURCE, dst)
    real = (dst / "component_process.xml").read_text()
    process_id = real.split('componentId="', 1)[1].split('"', 1)[0]

    executed = {
        _first(_load_json(p), "processId")
        for p in dst.glob("*execution_record*.json")
    }
    assert executed == {process_id}, "the execution records must name the archived graph"

    # A decoy that sorts BEFORE `component_process.xml`, carries the same operation,
    # and reaches no receiver. Under sort-order selection this is what gets read.
    decoy_id = "00000000-0000-4000-8000-000000000002"
    decoy = real.replace(process_id, decoy_id, 1)
    for shape in ("shape4",):
        decoy = decoy.replace(f'toShape="{shape}"', 'toShape="shape3"')
    (dst / "aaa_decoy_process.xml").write_text(decoy)
    assert sorted(p.name for p in dst.glob("*.xml"))[0] == "aaa_decoy_process.xml", (
        "the decoy must sort first, or this test cannot tell the two rules apart"
    )

    files = sorted(dst.iterdir())
    # Selected by the executed id: the real graph, which reaches the receiver.
    assert _reachable_receivers(files, "PATCH", process_id) == frozenset({"shape4"})
    # Selected by the decoy's id: the decoy graph, which reaches none. Different
    # answers from the same directory prove the id is what chooses.
    assert _reachable_receivers(files, "PATCH", decoy_id) == frozenset()
    # And the whole pipeline picks the executed one.
    assert summarize(dst, "PATCH").receiver_is_downstream is True


def test_executions_naming_different_processes_yield_no_attribution(tmp_path):
    """Two executions, two processes: no single topology to attribute through."""
    dst = tmp_path / "two-processes"
    shutil.copytree(_CAPTURES / _SOURCE, dst)
    records = sorted(dst.glob("*execution_record*.json"))
    assert len(records) > 1, "the fixture must carry more than one execution"

    payload = json.loads(records[0].read_text())

    def repoint(node):
        if isinstance(node, dict):
            if "processId" in node:
                node["processId"] = "00000000-0000-4000-8000-000000000003"
            for value in node.values():
                repoint(value)
        elif isinstance(node, list):
            for value in node:
                repoint(value)

    repoint(payload)
    records[0].write_text(json.dumps(payload))

    summary = summarize(dst, "PATCH")
    assert summary.receiver_is_downstream is None, (
        "disagreeing executions must leave the attribution unestablished"
    )
    assert summary.return_documents is None, "and the count unknown, not zero"


def test_a_process_id_from_an_unrelated_artifact_is_not_used(tmp_path):
    """The id comes from this capture's execution records, not from the directory.

    The archive holds a capture whose earliest `processId`-bearing file is a by-atom
    listing of other executions entirely, which a directory-wide search would read.
    """
    from boomi_mcp.connector_replay.capture import _first_across

    dst = tmp_path / "foreign-process-id"
    shutil.copytree(_CAPTURES / _SOURCE, dst)
    real = (dst / "component_process.xml").read_text()
    process_id = real.split('componentId="', 1)[1].split('"', 1)[0]

    (dst / "aaa_other_executions.json").write_text(
        json.dumps({"result": [{"processId": "00000000-0000-4000-8000-000000000004"}]})
    )
    files = sorted(dst.iterdir())
    assert _first_across(files, "processId", str) != process_id, (
        "a directory-wide search must actually pick the foreign id, or this is vacuous"
    )
    # The pipeline still attributes correctly, because it reads the execution records.
    assert summarize(dst, "PATCH").receiver_is_downstream is True


def test_a_receiver_on_an_unrelated_branch_does_not_supply_the_count(tmp_path):
    """Counting must bind to the receivers the walk found, not to every receiver."""
    from boomi_mcp.connector_replay.capture import _first_across, _reachable_receivers

    dst = tmp_path / "two-receivers"
    shutil.copytree(_CAPTURES / _SOURCE, dst)
    process = dst / "component_process.xml"
    text = process.read_text()

    # Give the process a SECOND Return Documents shape that the operation under test
    # does not produce for: hang it off the start shape, ahead of every connector.
    text = text.replace(
        '<shape name="shape4"',
        '<shape name="shape9" shapetype="returndocuments" userlabel="">'
        '<configuration><returndocuments/></configuration></shape>'
        '<shape name="shape4"',
        1,
    )
    process.write_text(text)

    # A SHAPE ALONE PROVES NOTHING. Without a platform row for it, a sum-over-every-
    # receiver implementation returns the same number and this test stays green under
    # the exact defect it exists to catch — which is what the previous version did.
    added = 0
    for path in sorted(dst.glob("*.json")):
        payload = json.loads(path.read_text())
        rows = _platform_connector_records(payload)
        unrelated = [
            {**row, "id": f"{row['id']}-shape9", "executionConnector": "shape9",
             "successCount": 7}
            for row in rows
            if row.get("connectorType") == "return"
            and isinstance(row.get("successCount"), int)
        ]
        if not unrelated:
            continue
        container = payload
        for key in ("raw", "data", "result"):
            if isinstance(container, dict) and key in container:
                container = container[key]
        if isinstance(container, list):
            container.extend(unrelated)
            path.write_text(json.dumps(payload))
            added += len(unrelated)
    assert added, "the unrelated receiver must actually report documents"

    files = sorted(dst.iterdir())
    process_id = _first_across(files, "processId", str)
    receivers = _reachable_receivers(files, "PATCH", process_id)
    assert receivers == frozenset({"shape4"}), (
        "the unrelated receiver must not be attributed to this operation"
    )

    summary = summarize(dst, "PATCH")
    # Bound to the attributed receiver. A sum over every receiver would be 2 + 7 per
    # execution, so the two rules give different numbers and the test discriminates.
    assert summary.return_documents == 2
    every_receiver = sum(
        row["successCount"]
        for path in dst.glob("*.json")
        for row in _platform_connector_records(json.loads(path.read_text()))
        if row.get("connectorType") == "return"
        and isinstance(row.get("successCount"), int)
    )
    assert every_receiver > summary.return_documents, (
        "summing every receiver must give a DIFFERENT answer, or the assertion above "
        "passes under the defect"
    )


def test_a_positive_receiver_count_always_comes_from_an_attributed_receiver():
    """The implication the ingest layer relies on, pinned where it is decidable.

    `_output_observation` deliberately writes no guard for this: the count is bound to
    the graph-attributed receivers upstream, so the failing case is unconstructible
    there and a guard would be one more clause that cannot fire. It is checkable here,
    across every capture and verb, which is where an unreachable invariant belongs.
    """
    import xml.etree.ElementTree as ET

    checked = positive = 0
    for directory in sorted(_CAPTURES.iterdir()):
        if not directory.is_dir():
            continue
        verbs = set()
        for xml in directory.glob("*.xml"):
            try:
                root = ET.parse(xml).getroot()
            except ET.ParseError:
                continue
            verbs |= {
                e.get("customOperationType")
                for e in root.iter()
                if e.get("customOperationType")
            }
        for verb in sorted(verbs) or [None]:
            try:
                summary = summarize(directory, verb)
            except Exception:
                continue
            checked += 1
            if (summary.return_documents or 0) > 0:
                positive += 1
                assert summary.receiver_is_downstream is True, (
                    f"{directory.name}::{verb} counts documents from a receiver the "
                    "graph does not attribute to the operation under test"
                )
    assert checked > 20, "the sweep must actually cover the archive"
    # THE FLOOR THAT MATTERS. Counting cells that merely summarize let a mutant
    # returning zero documents keep this green — the implication held vacuously over
    # a set with no positive member. Only cells that actually count a delivery
    # exercise it, and the archive has four.
    assert positive >= 4, (
        f"only {positive} cells counted a delivery, so the implication was not "
        "exercised; a reader that always returns zero would satisfy it"
    )


def test_served_provenance_comes_from_this_captures_executions(tmp_path):
    """The recorded time and account are read from the executions, not the directory.

    The same sweep the process id got, owed at the same moment and not paid then.
    Witness uses the archive's OWN by-atom listing, which names executions from other
    captures a month earlier — a directory-wide search reads it.
    """
    from boomi_mcp.connector_replay.capture import _first_across

    intact = summarize(_CAPTURES / _SOURCE, "PATCH")
    assert intact.captured_at and intact.account_id

    dst = tmp_path / "foreign-provenance"
    shutil.copytree(_CAPTURES / _SOURCE, dst)
    foreign = _CAPTURES / "cap155-e1-id-grammar" / "execution_records_by_atom.json"
    assert foreign.exists(), "the archive must still carry the foreign listing"
    shutil.copy(foreign, dst / "aa_execution_records_by_atom.json")

    files = sorted(dst.iterdir())
    borrowed = _first_across(files, "executionTime", str)
    assert borrowed != intact.captured_at, (
        "a directory-wide search must actually read the foreign time, or this is vacuous"
    )

    moved = summarize(dst, "PATCH")
    assert moved.captured_at == intact.captured_at, "the served time must not move"
    assert moved.account_id == intact.account_id, "nor the account the scope hash uses"


def test_runs_are_ordered_by_when_they_ran(tmp_path):
    """Counterparty outcomes are paired against the log in chronological order.

    The pairing used the filename LABEL on the stated grounds that label order and
    execution order agree. They disagree in most multi-run captures; with exactly two
    runs the inversions cancel, which is why no served value ever moved.
    """
    dst = tmp_path / "reversed-labels"
    shutil.copytree(_CAPTURES / _SOURCE, dst)

    records = sorted(dst.glob("*execution_record*.json"))
    assert len(records) == 2

    times = []
    for record in records:
        payload = json.loads(record.read_text())
        times.append(_first_time(payload))
    assert times[0] and times[1] and times[0] != times[1]

    # Swap the two records' times so label order and run order now disagree.
    for record, other in zip(records, reversed(times)):
        text = record.read_text()
        payload = json.loads(text)
        _set_time(payload, other)
        record.write_text(json.dumps(payload))

    summary = summarize(dst, "PATCH")
    ordered = [r.label for r in summary.runs]
    assert ordered == ["run2", "run1"], (
        f"runs must follow execution time, got {ordered}"
    )


def _first_time(node):
    if isinstance(node, dict):
        if "executionTime" in node:
            return node["executionTime"]
        for value in node.values():
            found = _first_time(value)
            if found:
                return found
    elif isinstance(node, list):
        for value in node:
            found = _first_time(value)
            if found:
                return found
    return None


def _set_time(node, value):
    if isinstance(node, dict):
        if "executionTime" in node:
            node["executionTime"] = value
        for inner in node.values():
            _set_time(inner, value)
    elif isinstance(node, list):
        for inner in node:
            _set_time(inner, value)


def test_two_disagreeing_copies_of_the_executed_process_are_refused(tmp_path):
    """The graph reader reconciles, like the row reader beside it."""
    from boomi_mcp.connector_replay.capture import CaptureRefused

    dst = tmp_path / "disagreeing-graphs"
    shutil.copytree(_CAPTURES / _SOURCE, dst)
    real = (dst / "component_process.xml").read_text()
    # Same component id, different graph, sorting FIRST.
    (dst / "aaa_same_process.xml").write_text(real.replace('toShape="shape4"', 'toShape="shape3"'))

    with pytest.raises(CaptureRefused, match="two archived copies of process"):
        summarize(dst, "PATCH")

    # CONTROL: a byte-identical second copy is not a disagreement.
    other = tmp_path / "identical-graphs"
    shutil.copytree(_CAPTURES / _SOURCE, other)
    (other / "aaa_same_process.xml").write_text((other / "component_process.xml").read_text())
    assert summarize(other, "PATCH").receiver_is_downstream is True


def test_a_partial_chronology_drops_attribution_rather_than_guessing(tmp_path):
    """One missing timestamp is not a chronology, and must not become one.

    The first version of the ordering fix sorted by timestamp with a fallback for
    missing ones, which put an untimed run AHEAD of every timed one: removing a single
    timestamp from the archived delete capture moved the first call's 204 onto the
    second execution and served that execution's id as the capture's.

    Falling back to LABEL order for the pairing is equally wrong — that is the
    unestablished key the ordering fix replaced. So an incomplete chronology drops the
    per-run attribution.

    What happens NEXT was stated wrongly here twice: the evidence row is REFUSED
    outright, not "degraded to unverified" — and `unverified` is a retry-safety value,
    not a replay observation at all. This test now asserts the consumer it gates
    rather than stopping at the classifier, which is the whole point of the finding
    that produced it.

    Both timestamps are stripped, because the platform records two and the
    establishment key reads either.
    """
    from boomi_mcp.connector_replay.ingest import IngestRefused, _capture_reference, classify
    from boomi_mcp.connector_replay.models import RetrySafetyV1

    source = _CAPTURES / "cap155-e5-delete-attested"
    intact = summarize(source, "DELETE")
    assert [(r.label, r.counterparty_status) for r in intact.runs] == [
        ("run1", 204), ("run2", 404)
    ]
    assert classify(intact, "DELETE")[1] is RetrySafetyV1.CONDITIONALLY_IDEMPOTENT

    dst = tmp_path / "partial-chronology"
    shutil.copytree(source, dst)

    def strip_times(node):
        if isinstance(node, dict):
            node.pop("executionTime", None)
            node.pop("recordedDate", None)
            for value in node.values():
                strip_times(value)
        elif isinstance(node, list):
            for value in node:
                strip_times(value)

    record = dst / "run2_execution_record.json"
    payload = json.loads(record.read_text())
    strip_times(payload)
    record.write_text(json.dumps(payload))

    partial = summarize(dst, "DELETE")
    # The counterparty log is untouched and still carries both outcomes...
    assert all(run.counterparty_status is None for run in partial.runs), (
        "an unestablished order must not pair outcomes to executions"
    )
    # ...so the verdict is not minted from a guessed order, and the CONSUMER refuses
    # rather than serving a row built on it.
    side_effect, safety = classify(partial, "DELETE")
    assert safety is RetrySafetyV1.UNVERIFIED
    with pytest.raises(IngestRefused):
        _capture_reference(partial, side_effect)
    # And the untimed run must not have been promoted to first, which is what the
    # superseded ordering did and what made the served execution id wrong.
    assert [r.label for r in partial.runs] == ["run1", "run2"]


def test_two_runs_sharing_one_timestamp_are_not_an_order(tmp_path):
    """A tie establishes no sequence either."""
    from boomi_mcp.connector_replay.ingest import IngestRefused, _capture_reference, classify
    from boomi_mcp.connector_replay.models import RetrySafetyV1

    dst = tmp_path / "tied-chronology"
    shutil.copytree(_CAPTURES / "cap155-e5-delete-attested", dst)
    records = sorted(dst.glob("*execution_record*.json"))
    shared = _first_time(json.loads(records[0].read_text()))
    assert shared

    for record in records:
        payload = json.loads(record.read_text())
        _set_time(payload, shared)
        _set_recorded(payload, shared)
        record.write_text(json.dumps(payload))

    tied = summarize(dst, "DELETE")
    assert all(run.counterparty_status is None for run in tied.runs)
    side_effect, safety = classify(tied, "DELETE")
    assert safety is RetrySafetyV1.UNVERIFIED
    with pytest.raises(IngestRefused):
        _capture_reference(tied, side_effect)


def _set_recorded(node, value):
    if isinstance(node, dict):
        if "recordedDate" in node:
            node["recordedDate"] = value
        for inner in node.values():
            _set_recorded(inner, value)
    elif isinstance(node, list):
        for inner in node:
            _set_recorded(inner, value)


def test_the_second_platform_timestamp_establishes_the_order(tmp_path):
    """The platform stamps each record twice, and both are recorded evidence.

    Refusing to order runs because ONE of two recorded times is absent discards an
    order the artifact does record. Measured: the second stamp is present and
    distinct in all thirteen archived multi-run captures.
    """
    dst = tmp_path / "second-stamp-only"
    shutil.copytree(_CAPTURES / "cap155-e5-delete-attested", dst)
    for record in dst.glob("*execution_record*.json"):
        payload = json.loads(record.read_text())

        def drop(node):
            if isinstance(node, dict):
                node.pop("executionTime", None)
                for value in node.values():
                    drop(value)
            elif isinstance(node, list):
                for value in node:
                    drop(value)

        drop(payload)
        record.write_text(json.dumps(payload))

    summary = summarize(dst, "DELETE")
    assert [(r.label, r.counterparty_status) for r in summary.runs] == [
        ("run1", 204), ("run2", 404)
    ], "the second stamp must still establish the order"


def test_a_shared_readback_delta_is_read_per_run(tmp_path):
    """Each entry names the run it describes; entry zero is not everyone's verdict.

    Not hypothetical — the archived POST capture served its replay a state change
    taken from the first call's entry while its own second entry says otherwise.
    """
    summary = summarize(_CAPTURES / "cap155-e2-post", "POST")
    by_label = {r.label: r.state_changed for r in summary.runs}
    assert by_label == {"run1": True, "replay": False}, (
        f"each run must take its own entry, got {by_label}"
    )

    entries = json.loads(
        (_CAPTURES / "cap155-e2-post" / "readback_delta.json").read_text()
    )
    assert isinstance(entries, list) and len(entries) > 1
    recorded = {e["label"]: e["raw_changed"] for e in entries}
    assert recorded != {k: entries[0]["raw_changed"] for k in recorded}, (
        "the fixture's entries must actually disagree, or this proves nothing"
    )
    assert by_label == recorded



























def test_a_conflicting_execution_id_is_not_overridden_by_a_matching_label(tmp_path):
    """The stronger key decides. A label that agrees cannot rescue an id that does not."""
    dst = tmp_path / "conflicting-keys"
    shutil.copytree(_CAPTURES / "cap155-e2-post", dst)
    path = dst / "readback_delta.json"
    entries = json.loads(path.read_text())
    assert len(entries) > 1

    # run1's entry keeps its LABEL but points its id at an execution in neither run,
    # and the verdicts are made to disagree so binding the wrong one would show.
    entries[0]["execution_id"] = "execution-00000000-0000-4000-8000-000000000009-2026.01.01"
    entries[0]["raw_changed"] = False
    entries[1]["raw_changed"] = True
    path.write_text(json.dumps(entries))

    summary = summarize(dst, "POST")
    by_label = {r.label: r.state_changed for r in summary.runs}
    # run1's own id matches no entry, so it gets no verdict rather than the one a
    # coincidental label offered — which is what the superseded rule handed it.
    assert by_label["run1"] is None, (
        f"run1 must not take an entry whose id names another execution, got {by_label}"
    )
    # The other run is still named by exactly one entry, so it binds cleanly: the
    # narrowing refuses the conflict without refusing the evidence beside it.
    assert by_label["replay"] is True, (
        f"the unambiguously keyed run must still bind, got {by_label}"
    )


def test_two_delta_entries_naming_one_execution_are_ambiguous(tmp_path):
    """Two entries for one id is a conflict, not a choice."""
    dst = tmp_path / "duplicate-keys"
    shutil.copytree(_CAPTURES / "cap155-e2-post", dst)
    path = dst / "readback_delta.json"
    entries = json.loads(path.read_text())
    ids = [e["execution_id"] for e in entries]
    entries[0]["execution_id"] = ids[1]
    path.write_text(json.dumps(entries))

    by_label = {r.label: r.state_changed for r in summarize(dst, "POST").runs}
    assert all(v is None for v in by_label.values()), (
        f"an id claimed by two entries must bind to neither, got {by_label}"
    )


def test_a_convergence_subject_with_a_missing_stage_is_refused(tmp_path):
    """Dropping the subject silently narrowed the set the verdict quantifies over.

    The archive records each readback's moment in its filename AND in its own payload
    label, and the reader used neither: it admitted any subject with three or more
    files and compared by ordinal position. A capture archived with one readback fewer
    lost that subject entirely — and with one MORE, both comparison windows slid past
    the effect. Either way an archive that recorded a second effect served the clean
    verdict, and the served row carries no stage count to notice with.
    """
    from boomi_mcp.connector_replay.capture import CaptureRefused

    source = _CAPTURES / "cap155-e5-delete-attested"
    intact = summarize(source, "DELETE")
    assert {c.subject for c in intact.convergence} == {"control", "target", "template"}

    dropped = tmp_path / "missing-stage"
    shutil.copytree(source, dropped)
    (dropped / "readback_R1_between_target.json").unlink()
    with pytest.raises(CaptureRefused, match="staged readbacks cover"):
        summarize(dropped, "DELETE")

    extra = tmp_path / "extra-stage"
    shutil.copytree(source, extra)
    shutil.copy(
        extra / "readback_R2_after_target.json",
        extra / "readback_R3_after_target.json",
    )
    with pytest.raises(CaptureRefused, match="more than one staged readback claims"):
        summarize(extra, "DELETE")


def test_an_unkeyed_delta_entry_still_binds_beside_a_keyed_stranger(tmp_path):
    """Precedence is per ENTRY: a keyed neighbour must not deny an unkeyed one."""
    dst = tmp_path / "mixed-keys"
    shutil.copytree(_CAPTURES / "cap155-e2-post", dst)
    path = dst / "readback_delta.json"
    entries = json.loads(path.read_text())

    # run1's entry keeps its label and loses its id; a stranger keeps an id.
    entries[0].pop("execution_id", None)
    entries[0]["raw_changed"] = True
    entries.append({
        "label": "someone-else",
        "execution_id": "execution-00000000-0000-4000-8000-00000000000a-2026.01.01",
        "raw_changed": False,
    })
    path.write_text(json.dumps(entries))

    by_label = {r.label: r.state_changed for r in summarize(dst, "POST").runs}
    assert by_label["run1"] is True, (
        f"an unkeyed entry must bind by its label regardless of a keyed stranger, "
        f"got {by_label}"
    )


def test_a_capture_stating_two_different_replay_orders_is_refused(tmp_path):
    """A complete moment set can still contradict its own stage numbers.

    Renaming one readback's moment leaves the set complete and reverses the sequence,
    so the two comparison windows get placed on a replay that ran the other way round.
    Whether that lands on a safe or unsafe verdict depends on the data — measured on
    the archived delete capture it flipped to `non_idempotent`, the fail-closed side —
    which is exactly why the refusal is on the CONTRADICTION and not on the outcome.
    """
    from boomi_mcp.connector_replay.capture import CaptureRefused

    dst = tmp_path / "two-orders"
    shutil.copytree(_CAPTURES / "cap155-e5-delete-attested", dst)
    (dst / "readback_R0_before_target.json").rename(dst / "readback_R0_between_target.json")
    (dst / "readback_R1_between_target.json").rename(dst / "readback_R1_before_target.json")

    with pytest.raises(CaptureRefused, match="do not increase"):
        summarize(dst, "DELETE")


def test_a_readback_whose_own_label_contradicts_its_filename_is_refused(tmp_path):
    """The place a readback claims for itself must match the place it is filed under."""
    from boomi_mcp.connector_replay.capture import CaptureRefused

    dst = tmp_path / "label-contradiction"
    shutil.copytree(_CAPTURES / "cap155-e5-delete-attested", dst)
    path = dst / "readback_R1_between_target.json"
    payload = json.loads(path.read_text())
    assert payload["label"] == "target R1_between", "the fixture must state its moment"
    payload["label"] = "target R1_after"
    path.write_text(json.dumps(payload))

    with pytest.raises(CaptureRefused, match="its own label states"):
        summarize(dst, "DELETE")

    # CONTROL: the older label generation states subject and stage only, and must
    # still be accepted — four archived captures write it that way.
    older = summarize(_CAPTURES / "cap155-e3b-patch-canonical", "PATCH")
    assert {c.subject for c in older.convergence} == {"target", "template"}


def test_a_label_a_keyed_entry_also_claims_is_not_a_fallback(tmp_path):
    """A keyed row proves the label is non-unique, so it identifies nothing."""
    dst = tmp_path / "reused-label"
    shutil.copytree(_CAPTURES / "cap155-e2-post", dst)
    path = dst / "readback_delta.json"
    entries = json.loads(path.read_text())
    entries[0].pop("execution_id", None)
    entries[0]["raw_changed"] = True
    entries.append({
        "label": "run1",
        "execution_id": "execution-00000000-0000-4000-8000-00000000000b-2026.01.01",
        "raw_changed": False,
    })
    path.write_text(json.dumps(entries))

    by_label = {r.label: r.state_changed for r in summarize(dst, "POST").runs}
    assert by_label["run1"] is None, (
        f"a label another keyed entry claims must not bind, got {by_label}"
    )


def test_a_comparison_across_two_resources_is_refused(tmp_path):
    """The reader established WHEN each observation was taken, never WHAT of.

    The subject token comes from a filename our own capture harness chose, while every
    readback records the counterparty's own name for the resource it read. Substituting
    one staged readback with an observation of a DIFFERENT resource — filename, stage,
    moment and payload label all untouched — manufactured the positive control the
    verdict calls essential.
    """
    from boomi_mcp.connector_replay.capture import CaptureRefused

    source = _CAPTURES / "cap155-e5-delete-attested"
    target_before = json.loads((source / "readback_R0_before_target.json").read_text())
    control_before = json.loads((source / "readback_R0_before_control.json").read_text())
    assert target_before["path"] != control_before["path"], (
        "the fixture's subjects must observe different resources"
    )

    dst = tmp_path / "swapped-subject"
    shutil.copytree(source, dst)
    swapped = dict(control_before)
    swapped["label"] = target_before["label"]  # every stated key left truthful-looking
    (dst / "readback_R0_before_target.json").write_text(json.dumps(swapped))

    with pytest.raises(CaptureRefused, match="rather than one resource"):
        summarize(dst, "DELETE")


def test_two_subjects_observing_one_resource_are_refused(tmp_path):
    """A negative control only controls for something if it is somewhere else."""
    from boomi_mcp.connector_replay.capture import CaptureRefused

    dst = tmp_path / "same-resource"
    shutil.copytree(_CAPTURES / "cap155-e5-delete-attested", dst)
    for moment, stage in (("before", "R0"), ("between", "R1"), ("after", "R2")):
        control = dst / f"readback_{stage}_{moment}_control.json"
        target = dst / f"readback_{stage}_{moment}_target.json"
        body = json.loads(control.read_text())
        body["path"] = json.loads(target.read_text())["path"]
        control.write_text(json.dumps(body))

    with pytest.raises(CaptureRefused, match="observed the same resource"):
        summarize(dst, "DELETE")


def test_repeated_stage_numbers_are_not_a_sequence(tmp_path):
    """Three readbacks all filed as one stage record no order at all.

    The first version of this pin compared a stable sort against the original, which
    is vacuous under ties — and the older payload-label form states subject and stage
    only, so it had no moment to contradict. A capture with no sequence drove the
    comparison that decides retry safety.
    """
    from boomi_mcp.connector_replay.capture import CaptureRefused

    dst = tmp_path / "one-stage"
    shutil.copytree(_CAPTURES / "cap155-e3b-patch-canonical", dst)
    for subject in ("target", "template"):
        for moment, stage in (("before", "R0"), ("between", "R1"), ("after", "R2")):
            path = dst / f"readback_{stage}_{moment}_{subject}.json"
            body = json.loads(path.read_text())
            assert body["label"] == f"{subject} {stage}", "this generation states no moment"
            body["label"] = f"{subject} R0"
            path.write_text(json.dumps(body))
            path.rename(dst / f"readback_R0_{moment}_{subject}.json")

    with pytest.raises(CaptureRefused, match="do not increase"):
        summarize(dst, "PATCH")




















def _report(w, n1, run="cdx-review.aaaaaa"):
    return (
        "## Slice B — closing report\n\n"
        f"| `{w}` (**W**) | wave |\n| `{n1}` (**N−1**) | review `{run}` |\n"
    )








































_ROW = ("| L4 composite wave gate, slice B | 1 / 1 | x | `CLOSE-CLEAN` | W = `{sha}`{cite}. Arms: the non-KB "
        "suite green at 11,037 passed and 18 skipped; 74 active goldens rendered twice and byte-exact; the "
        "plan-fingerprint seam across 2 cases |")






























#: This slice's literal baseline. The manifest check derives its scope from the
#: files changed since it, so the scope follows the work instead of naming one.
_SLICE_BASELINE = "d04a2482d9b43c84765395ec8bbc73495de29fd4"






































def test_every_collected_node_is_pinned_by_the_manifest():
    """The floor is a MINIMUM, so an unpinned test never fails anything.

    The required-node manifest is checked in one direction — every required node must
    still collect — which catches a deletion and misses an addition. Three times in
    this slice a batch added tests and left them unpinned: 217 of them once, the
    closure guard once, and its five rule cases once. Each time the gate stayed green
    while the new work was protected by nothing, because collection merely exceeded
    the floor.

    This asserts the other direction over EVERY test file this slice touched, derived
    from the files changed since the slice baseline rather than from a filename written
    here. It was scoped to one file, and replacing that enumeration is what the SECOND
    instance of the regenerated-from-a-non-final-tree class requires: a manifest
    rebuilt every round instead of once from the final tree moved 254 identifiers
    between commits, and a check watching a single file could not see it. The scope
    stays inside this slice's own changes — the manifest is another issue's artifact,
    so this covers what the slice added and not what it inherited.
    """
    import json
    import subprocess

    root = Path(__file__).resolve().parents[1]
    manifest = root / "tests/fixtures/wave_gate/test_nodes.jsonl"
    rows = [json.loads(l) for l in manifest.read_text().splitlines() if l.strip()]
    pinned = {r["node_id"] for r in rows[1:]}

    def paths(*args):
        # `-z`, always. Concatenating display-formatted output and calling `.split()`
        # corrupts any path containing whitespace or a quoted non-ASCII name, after
        # which `is_file()` drops it silently — the exact mistake this file already
        # fixed once for `git status` and I reintroduced here.
        out = subprocess.run(["git", *args, "-z", "--", "tests"],
                             capture_output=True, cwd=root)
        assert out.returncode == 0, out.stderr[-300:]
        return [q for q in out.stdout.decode("utf-8", "surrogateescape").split("\0") if q]

    # STAGED CONTENT IS WHAT THE NEXT COMMIT HOLDS. Collecting the worktree checks a
    # tree that may never be committed: a test staged and then reverted in the
    # worktree is absent from the collection while a plain commit still adds it.
    # Rather than materialize the index, divergence is refused outright — the guard
    # then says plainly that it cannot speak for this tree.
    staged_only = paths("diff", "--name-only", "--cached")
    unstaged = set(paths("diff", "--name-only"))
    diverged = sorted(f for f in staged_only
                      if f in unstaged and f.endswith(".py") and f.startswith("tests/"))
    assert not diverged, (
        "these test files differ between the index and the worktree, so what a commit "
        f"would contain is not what pytest would collect: {diverged}")
    # WORKTREE-INCLUSIVE, and untracked too. `git diff A B` lists only committed
    # paths, so during the ordinary uncommitted-fix workflow a freshly touched or
    # freshly created test file never reached the collection and its unpinned nodes
    # passed — which is the state this guard exists for.
    candidates = (paths("diff", "--name-only", _SLICE_BASELINE, "HEAD")
                  + paths("diff", "--name-only", _SLICE_BASELINE)
                  + paths("ls-files", "--others", "--exclude-standard"))
    # tests/fixtures/ holds FROZEN inputs, including captured Python scripts that were
    # never meant to be imported (some open sockets or provision accounts at import
    # time); naming them to pytest explicitly would collect them. Only test code counts.
    files = sorted({f for f in candidates
                    if f.startswith("tests/") and f.endswith(".py")
                    and not f.startswith("tests/fixtures/") and (root / f).is_file()})
    assert files, "the slice changed no test file; this check would be vacuous"

    collected = subprocess.run(
        [sys.executable, "-m", "pytest", *files, "--collect-only", "-q",
         "-p", "no:cacheprovider"],
        capture_output=True, text=True, cwd=root,
        env={**os.environ, "PYTHONPATH": "src", "PYTHONDONTWRITEBYTECODE": "1"},
    )
    assert collected.returncode == 0, f"collection failed: {collected.stderr[-400:]}"
    prefixes = tuple(f + "::" for f in files)
    nodes = [ln.strip() for ln in collected.stdout.splitlines()
             if ln.startswith(prefixes)]
    assert len(nodes) > 40, f"only {len(nodes)} nodes collected; the check would be thin"

    unpinned = sorted(n for n in nodes if n not in pinned)
    assert not unpinned, (
        "these tests collect but are not required by the manifest, so deleting them "
        f"would leave every gate green: {unpinned}"
    )




def _node_manifest_gate():
    """The wave gate itself, loaded as a module. The authority, not a copy of it."""
    import importlib.util

    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "_node_manifest_authority", root / "scripts" / "wave_gate.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, root


def _canonical_collector(candidates, want):
    """The first candidate that IS Python 3.11 carrying pytest ``want``.

    Extracted so the miss cases can be driven. A `Path` is always truthy, so the
    first version launched a repository-local interpreter that a fresh checkout
    does not have and raised `FileNotFoundError` instead of trying the one on the
    path — measured, not reasoned: the exception, not the fallback, is what a
    clean clone got.
    """
    import subprocess as _sub

    for path in candidates:
        if not path:
            continue
        try:
            probe = _sub.run(
                [str(path), "-c",
                 "import sys, pytest; print(sys.version_info[:2], pytest.__version__)"],
                capture_output=True, text=True)
        except OSError:
            # Absent, not executable, or a broken symlink — all of them mean
            # "not this one", never "stop looking".
            continue
        if probe.returncode != 0:
            continue
        head, _, version = probe.stdout.strip().rpartition(" ")
        if head.startswith("(3, 11)") and version == want:
            return str(path)
    return None


def _assert_legal_manifest_successor(gate, at_head, current, landing_of, ranks_of):
    """The successor rule, plus the one waiver the wave gate forces.

    ``landing_of`` and ``ranks_of`` are THUNKS, resolved only on the waiver path.
    Resolving the first up front made a clone without that ref go red on a tree
    that needed no waiver at all — a guard failing for a reason unrelated to what
    it checks — and the second costs a full collection, which no tree taking the
    strict path should pay.

    Extracted from the test so every arm can be driven with synthetic manifests.
    Inline, the arms could only be reasoned about, and this slice's record is a
    list of what happens when I reason about a branch instead of executing it.
    """
    try:
        gate.validate_transition(at_head, current, "pytest-nodes")
        return "strict"
    except gate.GateFailure:
        pass

    at_landing = landing_of()
    assert at_landing is not None, (
        "the manifest could not be read at the landing base, so the closing "
        "regeneration cannot be told apart from an arbitrary repoint"
    )
    # The repair must be REAL: legal at the base the wave gate will judge, and
    # repairing a committed manifest that is NOT legal there.
    gate.validate_transition(at_landing, current, "pytest-nodes")
    try:
        gate.validate_transition(at_landing, at_head, "pytest-nodes")
    except gate.GateFailure:
        pass
    else:
        raise AssertionError(
            "the committed manifest is already a legal successor of the landing "
            "base, so this regeneration repairs nothing and its deletions and "
            "repoints are refused"
        )
    # AND THE REPAIR MUST BE ONLY THAT REPAIR. Without this the waiver excused
    # any deletion that happened to accompany a born-tombstone: a regeneration
    # dropping an unrelated ACTIVE identity is still legal at the landing base
    # and still repairs the tombstone, so it was waived and a required test
    # simply disappeared. The removable set is DERIVED — the rows appended after
    # the landing base that HEAD carries as tombstones — never asserted.
    born_tombstoned = {
        row["node_id"] for row in at_head.rows[len(at_landing.rows):]
        if row["state"] == "tombstone"
    }
    removed = ({row["node_id"] for row in at_head.rows}
               - {row["node_id"] for row in current.rows})
    # SUBSET, not equality. A born-tombstoned identity the tree brings BACK is
    # removed from nothing — the wave gate accepts it as an ordinary active
    # append — and requiring equality refused that legal regeneration. What must
    # hold is only that nothing OUTSIDE the removable set disappears, which is
    # also what keeps an active identity from vanishing: a born-tombstone is by
    # construction not active at HEAD.
    if not removed <= born_tombstoned:
        raise AssertionError(
            "the regeneration removed identities the wave gate does not require "
            "it to remove, so this is not the repair it claims to be: removed "
            "{0}, removable {1}".format(sorted(removed), sorted(born_tombstoned))
        )
    # AND THE BLOCK IT REWRITES MUST BE IN COLLECTION ORDER. Everything above is
    # decided from three manifests, and three manifests cannot tell a legal
    # reordering from a repoint: the closing regeneration re-derives the appended
    # block in collection order while the committed one accumulated it in the
    # order tests were added, so the two genuinely differ and every rule keyed on
    # the committed order refuses the legal case — measured, 47 mismatches
    # against the real regeneration. The authority that DOES separate them is the
    # collection itself, and it is the one thing the earlier version never asked.
    ranks = ranks_of()
    appended = [row["node_id"] for row in current.rows[len(at_landing.rows):]]
    unknown = [node for node in appended if node not in ranks]
    if unknown:
        raise AssertionError(
            "the regeneration appended identities this tree does not collect, so "
            "their order cannot be checked against anything: {0}".format(unknown)
        )
    order = [ranks[node] for node in appended]
    inversions = [(appended[i], appended[i + 1])
                  for i in range(len(order) - 1) if order[i] >= order[i + 1]]
    if inversions:
        raise AssertionError(
            "the appended block is not in collection order, so at least one "
            "identifier was repointed rather than re-derived: {0}".format(
                inversions[:3])
        )
    return "waived"


def test_the_node_manifest_is_a_legal_successor_of_the_one_it_replaces():
    """A regeneration may APPEND. It may never repoint an id that already exists.

    The procedure invites exactly this mistake: it says restore the slice baseline
    and re-append, which silently reassigns every id an intermediate commit already
    published. Measured when this guard was written — a regeneration from the slice
    baseline moved `pytest-011173` off
    `test_every_closing_checkpoint_agrees_with_its_own_window` onto a parameter case
    and shifted 300-odd rows behind it, and the bases the gate actually uses
    (`origin/dev`, the slice baseline) both accepted it.

    WHAT THIS GUARD DOES NOT DO, stated because a review found it claiming
    otherwise: it does not audit branch history. An id appended after `origin/dev`
    and repointed by a later commit is invisible to a `origin/dev` comparison — the
    row simply looks new — so that arm gave coverage it did not have and is gone.
    Auditing adjacent committed pairs is not the fix either: measured on this branch,
    ELEVEN of 88 pairs are already illegal, because following the documented
    regeneration produces exactly that, and the eleven cannot be repaired forward
    (restoring an id is itself a repoint) nor rewritten (archived attestations cite
    those commits). The gate is scoped to the base transition ON PURPOSE; the
    intra-branch chain is not an invariant this repository holds.

    So this asks the one question it can answer, against the one manifest the
    regeneration never consults: is the working tree a legal successor of the
    manifest it REPLACES? That fires between regenerating and committing, which is
    the only moment the mistake is cheap — and it asks `validate_transition`, the
    gate's own rule, rather than modelling it a second time.
    """
    import subprocess

    gate, root = _node_manifest_gate()
    rel = "tests/fixtures/wave_gate/test_nodes.jsonl"

    def at(ref):
        out = subprocess.run(
            ["git", "show", f"{ref}:{rel}"], capture_output=True, cwd=root
        )
        return out.stdout if out.returncode == 0 else None

    current = gate.parse_manifest((root / rel).read_bytes(), "pytest-nodes")

    previous = at("HEAD")
    assert previous is not None, (
        "the manifest committed at HEAD could not be read, so this guard checked "
        "nothing"
    )
    at_head = gate.parse_manifest(previous, "pytest-nodes")

    def landing_of():
        raw = at("origin/dev")
        return gate.parse_manifest(raw, "pytest-nodes") if raw is not None else None

    def ranks_of():
        """The tree's own collection order — the authority the manifests lack.

        Run in the CI ENVIRONMENT, not in whatever environment happens to be
        invoking pytest: the endgame gate fixes `PYTHONPATH`, no bytecode, plugin
        autoload OFF and the three platform variables, and a rank derived under a
        different configuration is a different authority. Autoload in particular
        is what keeps a randomising plugin arriving through a transitive
        dependency from reordering the very thing being ranked. Measured on this
        tree when the divergence was found: with autoload on and off the order is
        IDENTICAL, 11,432 nodes in the same sequence — so this alignment closes a
        divergence rather than a live defect, and the measurement is recorded so
        the next reader need not redo it.

        INTERPRETER: FAIL CLOSED. The canonical collection runs on Python 3.11
        with the pinned pytest, because the node-id grammar and the summary line
        are part of the gate's parsing contract and an unpinned pytest can change
        either. Ranking under anything else is ranking under a different
        authority, and the preflight cannot repair the mistake afterwards: the
        gate's collection parser returns a SET, so order is never compared there
        — verified in `scripts/wave_gate.py`. So this refuses to guess. Measured
        while closing this: 3.11 and 3.12 with the pinned pytest and autoload off
        produce the IDENTICAL order over 11,432 nodes, which is why the earlier
        fallback did no harm in practice and is still not a licence to keep it.

        A full collection, which is why it is a thunk: only a regeneration that
        already failed the strict transition pays for it.
        """
        import os
        import re as _re
        import subprocess as _sub

        pinned = _re.search(
            r"^pytest==(\S+)$",
            (root / "requirements-dev.txt").read_text(encoding="utf-8"),
            _re.MULTILINE,
        )
        assert pinned, "requirements-dev.txt does not pin pytest; the rank has no contract"
        want = pinned.group(1)

        interpreter = _canonical_collector(
            [root / ".venv311" / "bin" / "python", shutil.which("python3.11")], want)
        assert interpreter, (
            "no canonical collection environment: this rank must come from Python "
            "3.11 with pytest=={0}, and neither .venv311 nor python3.11 on PATH "
            "provides it. Provision one — `python3.11 -m venv .venv311 && "
            ".venv311/bin/python -m pip install -r requirements-dev.txt` — rather "
            "than ranking the manifest under a different authority".format(want)
        )

        out = subprocess.run(
            [interpreter, "-m", "pytest", "tests", "--ignore=tests/kb",
             "--collect-only", "-q", "-p", "no:cacheprovider"],
            capture_output=True, text=True, cwd=root,
            env={**os.environ, "PYTHONPATH": "src", "PYTHONDONTWRITEBYTECODE": "1",
                 "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1", "BOOMI_LOCAL": "true",
                 "BOOMI_DOCS_ENABLED": "false", "BOOMI_GOTCHAS_ENABLED": "false"},
        )
        assert out.returncode == 0, f"collection failed: {out.stderr[-400:]}"
        nodes = [ln.strip() for ln in out.stdout.splitlines() if "::" in ln.strip()]
        assert len(nodes) > 1000, f"only {len(nodes)} nodes collected; too thin to rank"
        return {node: position for position, node in enumerate(nodes)}

    # A test created and retired inside one slice leaves a row that was APPENDED
    # ALREADY TOMBSTONED at the landing base, and the wave gate refuses exactly
    # that — "a row added and removed within the same range needs no row at all".
    # Removing it renumbers every row behind it, which is a deletion plus a
    # repoint and is what this guard otherwise exists to stop. The two rules meet
    # head-on at the close, and the gate's base transition is the one that will
    # actually judge the landing.
    _assert_legal_manifest_successor(
        gate, at_head, current, landing_of, ranks_of
    )

    # NON-VACUITY, against this very manifest: repoint one live row exactly as the
    # baseline-restore regeneration did, and the authority must refuse it. Without
    # this, a `validate_transition` that stopped checking node ids would leave the
    # guard green and silent.
    rows = [dict(r) for r in current.rows]
    victim = next(i for i, r in enumerate(rows) if r["state"] == "active")
    rows[victim]["node_id"] = rows[victim]["node_id"] + "-repointed"
    mutant = gate.Manifest("pytest-nodes", dict(current.header), rows)
    try:
        gate.validate_transition(current, mutant, "pytest-nodes")
    except gate.GateFailure as exc:
        # The CODE, not the message text. The gate carries its stable diagnostic on
        # the exception; matching prose would pass the day someone rewords it.
        assert exc.code == "MANIFEST_TRANSITION_ILLEGAL", (
            f"a repointed id was refused, but not as an illegal transition: {exc.code}"
        )
    else:
        raise AssertionError(
            "the authority accepted a repointed node id, so this guard proves nothing"
        )

    # The waiver's own arms are witnessed on SYNTHETIC manifests in the test
    # below. They used to be checked here against `origin/dev`, which reintroduced
    # exactly the defect the thunk removed: a clone without that ref went red on
    # the strict path, where no landing base is needed at all.


def test_the_collector_search_skips_a_miss_instead_of_dying_on_it(tmp_path):
    """Each way a candidate can fail to be the canonical collector.

    The case that mattered: a repository-local interpreter a fresh checkout does
    not have. A `Path` is truthy whether or not it exists, so the first version
    launched it and raised instead of trying the next candidate — measured on a
    parked directory, and the exception is what a clean clone would have got.
    """
    import re as _re
    import shutil as _shutil
    import sys as _sys

    pinned = _re.search(
        r"^pytest==(\S+)$",
        (Path(__file__).resolve().parents[1] / "requirements-dev.txt").read_text(),
        _re.MULTILINE,
    )
    assert pinned, "requirements-dev.txt no longer pins pytest"

    # ABSENT path, then None, then the running interpreter — the first two may
    # neither raise nor end the search, whatever the third turns out to be.
    #
    # WHAT THE THIRD IS gets DETERMINED, not assumed. The first version of this
    # assertion assumed the runner was 3.12 and so a miss; on the required 3.11
    # job the runner is a match and the search rightly returns it, and the
    # assertion failed — which would have broken the required check. Asserting an
    # environment fact instead of deriving it is the defect class this slice has
    # recorded twenty-two times, committed inside the witness written to police a
    # different instance of it.
    import pytest as _pytest

    want = pinned.group(1)
    runner_is_canonical = (
        _sys.version_info[:2] == (3, 11) and _pytest.__version__ == want
    )
    missing = tmp_path / "nowhere" / "bin" / "python"
    assert not missing.exists()
    found = _canonical_collector([missing, None, _sys.executable], want)
    assert found == (_sys.executable if runner_is_canonical else None), (
        f"runner canonical={runner_is_canonical} ({_sys.version_info[:2]}, "
        f"pytest {_pytest.__version__}); search returned {found!r}"
    )

    # LAUNCHABLE BUT NOT CANONICAL, both halves, and both DETERMINISTIC rather
    # than borrowed from whatever interpreter happens to be running. The previous
    # version leaned on the runner being 3.12; on the required 3.11 job it is
    # canonical, so no mismatch was exercised at all and a collector that dropped
    # its version checks entirely passed — measured by running that mutant under
    # a real 3.11.
    def _shim(name, line):
        path = tmp_path / name
        path.write_text("#!/bin/sh\necho '{0}'\n".format(line))
        path.chmod(0o755)
        return path

    # Right pytest, wrong Python.
    assert _canonical_collector([_shim("old_py", "(3, 10) " + want)], want) is None
    # Right Python, wrong pytest.
    assert _canonical_collector([_shim("old_pytest", "(3, 11) 0.0.0")], want) is None
    # Both right — the control, without which the two above would pass on a
    # collector that simply refused everything.
    ok = _shim("canonical", "(3, 11) " + want)
    assert _canonical_collector([ok], want) == str(ok)

    # CONTINUATION PAST A LAUNCHABLE MISMATCH, which is the production shape: a
    # repository-local environment that runs but carries the wrong versions, with
    # a usable interpreter behind it on the path. Every case above used a
    # SINGLETON list, so a collector that gave up after the first launchable
    # mismatch passed all of them — measured by writing exactly that regression
    # and watching this test stay green. A mismatch means "not this one", and
    # only a list proves it does not also mean "stop".
    assert _canonical_collector(
        [_shim("first_old_py", "(3, 10) " + want), ok], want) == str(ok)
    assert _canonical_collector(
        [_shim("first_old_pytest", "(3, 11) 0.0.0"), ok], want) == str(ok)
    # ...and a mismatch that cannot even launch must not stop the search either.
    assert _canonical_collector(
        [tmp_path / "gone" / "python", _shim("junk_first", "nonsense"), ok],
        want) == str(ok)

    # A candidate that exists but is not an interpreter at all.
    junk = tmp_path / "junk"
    junk.write_text("not an interpreter")
    junk.chmod(0o755)
    assert _canonical_collector([junk], want) is None

    # And nothing at all is None rather than an exception.
    assert _canonical_collector([], want) is None


def test_the_successor_waiver_only_excuses_a_repair_that_is_real():
    """Every arm of the waiver, driven — not reasoned about.

    Arm one: the committed manifest is illegal at the landing base and the tree
    fixes it. That is the closing regeneration and it is waived. Arm two: the
    committed manifest is ALREADY legal there, so a tree that deletes a row is
    repairing nothing. Arm three: the tree is itself illegal at the landing base,
    so a broken committed manifest must not excuse it. Arm four: the tree drops
    an unrelated ACTIVE identity alongside the tombstone, which is legal at the
    landing base and repairs the tombstone and is still not this repair.
    """
    gate, _root = _node_manifest_gate()

    def manifest(rows):
        active = sum(1 for r in rows if r["state"] == "active")
        header = {
            "kind": "manifest", "schema_version": 1, "manifest": "pytest-nodes",
            "minimum_active": active, "minimum_collected": active,
            "maximum_skipped": 30, "bootstrap_base": "0" * 40,
        }
        return gate.Manifest("pytest-nodes", header, [dict(r) for r in rows])

    def row(n, node, state="active"):
        return {"kind": "test", "id": "pytest-%06d" % n, "node_id": node,
                "state": state}

    landing = manifest([row(1, "a"), row(2, "b")])
    of_landing = lambda: landing
    # The collection order these synthetic nodes would have. It is the authority
    # that separates a legal re-derivation of the appended block from a repoint,
    # and passing it explicitly is what lets both be driven here.
    of_ranks = lambda: {"a": 0, "b": 1, "c": 2, "d": 3}

    # ARM ONE — the closing regeneration.
    head_illegal = manifest([row(1, "a"), row(2, "b"), row(3, "c", "tombstone")])
    repaired = manifest([row(1, "a"), row(2, "b")])
    assert _assert_legal_manifest_successor(
        gate, head_illegal, repaired, of_landing, of_ranks
    ) == "waived"

    # ARM TWO — nothing to repair, so nothing is excused.
    head_legal = manifest([row(1, "a"), row(2, "b"), row(3, "c")])
    try:
        _assert_legal_manifest_successor(
            gate, head_legal, repaired, of_landing, of_ranks)
    except AssertionError as exc:
        assert "repairs nothing" in str(exc), exc
    else:
        raise AssertionError(
            "a deletion was waived against a committed manifest that needed no "
            "repair, so the waiver excuses anything"
        )

    # ARM THREE — a broken record must not excuse a broken tree. Measured:
    # without this case, deleting the landing-base check left every other arm
    # green.
    tree_also_illegal = manifest([
        row(1, "a"), row(2, "b"), row(3, "c", "tombstone"), row(4, "d", "tombstone"),
    ])
    try:
        _assert_legal_manifest_successor(
            gate, head_illegal, tree_also_illegal, of_landing, of_ranks
        )
    except gate.GateFailure as exc:
        assert exc.code == "MANIFEST_TRANSITION_ILLEGAL", exc.code
    else:
        raise AssertionError(
            "a tree that is itself illegal at the landing base was waived because "
            "the committed manifest was broken too"
        )

    # ARM FOUR — the hole the review found. HEAD carries a born-tombstone AND an
    # unrelated active row; a regeneration that drops both is legal at the
    # landing base and does repair the tombstone, and every earlier version of
    # this waiver accepted it while a required identity vanished.
    head_mixed = manifest([
        row(1, "a"), row(2, "b"), row(3, "c", "tombstone"), row(4, "d"),
    ])
    drops_both = manifest([row(1, "a"), row(2, "b")])
    try:
        _assert_legal_manifest_successor(
            gate, head_mixed, drops_both, of_landing, of_ranks)
    except AssertionError as exc:
        assert "does not require it to remove" in str(exc), exc
    else:
        raise AssertionError(
            "an active identity was dropped under cover of the tombstone repair"
        )
    # ...while dropping ONLY the tombstone, keeping the active row, is the repair.
    keeps_active = manifest([row(1, "a"), row(2, "b"), row(3, "d")])
    assert _assert_legal_manifest_successor(
        gate, head_mixed, keeps_active, of_landing, of_ranks
    ) == "waived"

    # ARM FIVE — a born-tombstoned identity the tree BRINGS BACK. The wave gate
    # takes it as an ordinary active append and nothing is lost, so the waiver
    # must permit it. An earlier version required the removed set to EQUAL the
    # removable one and refused this outright, which would have blocked a legal
    # regeneration the day a deleted test came back under its own name.
    reactivated = manifest([row(1, "a"), row(2, "b"), row(3, "c")])
    assert _assert_legal_manifest_successor(
        gate, head_illegal, reactivated, of_landing, of_ranks
    ) == "waived"

    # ARM SIX — a repoint is still refused on the waiver path, so the waiver's
    # own landing-base check is not decorative. Checked here on synthetic
    # manifests rather than against the real landing ref, because resolving that
    # ref outside the waiver path is the fresh-clone defect itself.
    repointed = manifest([row(1, "a"), row(2, "zz")])
    try:
        _assert_legal_manifest_successor(
            gate, head_illegal, repointed, of_landing, of_ranks)
    except gate.GateFailure as exc:
        assert exc.code == "MANIFEST_TRANSITION_ILLEGAL", exc.code
    else:
        raise AssertionError(
            "the waiver accepted a repointed identity, so its landing-base check "
            "proves nothing"
        )

    # ARM SEVEN — THE REVIEWER'S CASE, and the one that had no disposition until
    # the collection order was brought in. HEAD appends a tombstone and an active
    # row; the tree swaps those two positions. Nothing is removed, so the subset
    # rule passes, and the landing-base transition is legal, so the wave gate
    # cannot see it either — yet both identifiers now name different tests. Only
    # the collection order separates it from the reordering the closing
    # regeneration legitimately performs.
    head_swappable = manifest([
        row(1, "a"), row(2, "b"), row(3, "c", "tombstone"), row(4, "d"),
    ])
    swapped = manifest([row(1, "a"), row(2, "b"), row(3, "d"), row(4, "c")])
    try:
        _assert_legal_manifest_successor(
            gate, head_swappable, swapped, of_landing, of_ranks)
    except AssertionError as exc:
        assert "not in collection order" in str(exc), exc
    else:
        raise AssertionError(
            "a swap inside the appended block was waived, so two identifiers "
            "silently changed which test they name"
        )
    # ...and the same block IN collection order is the legal re-derivation.
    in_order = manifest([row(1, "a"), row(2, "b"), row(3, "c"), row(4, "d")])
    assert _assert_legal_manifest_successor(
        gate, head_swappable, in_order, of_landing, of_ranks
    ) == "waived"

    # ARM EIGHT — an appended identity this tree does not collect. Its order
    # cannot be checked against anything, so it is refused rather than skipped:
    # an unrankable row would otherwise slip past the order rule silently, which
    # is how a check quietly stops covering the row it was written for.
    # Nothing removed — `ghost` is APPENDED beside the survivors, so the subset
    # rule passes and only the rank lookup can object. The first version of this
    # arm replaced `d` instead of adding to it, which made the subset rule fire
    # first and left the check it was written for ungraded.
    ghost = manifest([row(1, "a"), row(2, "b"), row(3, "c"), row(4, "d"),
                      row(5, "ghost")])
    try:
        _assert_legal_manifest_successor(
            gate, head_swappable, ghost, of_landing, of_ranks)
    except AssertionError as exc:
        assert "does not collect" in str(exc), exc
    else:
        raise AssertionError(
            "an identity absent from the collection was waived, so the order "
            "rule silently skipped a row"
        )

    # ARM NINE — the ordinary case takes the strict path and never resolves the
    # landing base or the collection at all, which is what keeps a clone lacking
    # that ref green and what keeps the collection off the common path.
    def _must_not_be_called():
        raise AssertionError("the landing base was resolved on the strict path")

    assert _assert_legal_manifest_successor(
        gate, landing, manifest([row(1, "a"), row(2, "b"), row(3, "c")]),
        _must_not_be_called, _must_not_be_called,
    ) == "strict"
















# --- A boundary witness asserts unconditionally -------------------------------
#
# DC-155-L, fourth consecutive round on one pin. The class's standing invariant —
# a witness must EXECUTE the production path it names — was satisfied by the third
# attempt and it still graded nothing: it drove the boundary on a route where the
# case cannot arise, and carried an `if` whose false branch asserted something
# weaker. The escape is what makes that survivable, so the escape is what this
# refuses. The scope is DERIVED, not listed: a test is in scope exactly when it
# calls the public apply entry point, which is the property that makes an
# unreached case indistinguishable from a passing one.
_BOUNDARY_ENTRY = "build_integration_action"

#: The census root, resolved from THIS FILE rather than the working directory.
#: Measured: with a relative `Path("tests")` and pytest launched from `tests/`,
#: the sweep looked for `tests/tests`, found no files, and passed in 0.12s having
#: checked nothing — a guard whose universe collapses silently is the class this
#: file exists to refuse, committed by the guard itself.
_TESTS_ROOT = Path(__file__).resolve().parent

#: The SERVED wrappers that delegate to the apply entry, derived from the tool
#: module rather than listed. A test that calls `server.build_integration(...)`
#: drives the same boundary without naming the entry, and #180's lesson is that
#: the served path is the one whose coverage claims are worth least when
#: assumed: six such tests existed and the first two versions of this census
#: could not see any of them.
def _served_boundary_wrappers():
    module = ast.parse(
        (_TESTS_ROOT.parent / "server.py").read_text(encoding="utf-8")
    )
    return {
        node.name for node in ast.walk(module)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and _BOUNDARY_ENTRY in set(_call_names(node))
    }



def _call_names(node):
    """Every callable name in *node*, in BOTH forms a call site can take.

    `build_integration_action(...)` is an `ast.Name`; the equally common
    `integration_builder.build_integration_action(...)` is an `ast.Attribute`
    and was invisible to the first version of this sweep, so a witness written
    that way escaped the rule entirely.
    """
    for inner in ast.walk(node):
        if not isinstance(inner, ast.Call):
            continue
        func = inner.func
        if isinstance(func, ast.Name):
            yield func.id
        elif isinstance(func, ast.Attribute):
            yield func.attr


def _boundary_callers(tree, served=frozenset()):
    """Names that reach the apply entry: the entry itself, plus module-local
    wrappers, to a fixed point.

    This suite's own `_apply` helper is exactly such a wrapper, and a test that
    calls it drives the boundary without ever naming it. One level of resolution
    would still miss a wrapper around a wrapper, so this iterates until the set
    stops growing rather than assuming a depth.
    """
    module_functions = {
        node.name: node for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    reaching = {_BOUNDARY_ENTRY} | served
    while True:
        grown = {
            name for name, node in module_functions.items()
            if name not in reaching and reaching & set(_call_names(node))
        }
        if not grown:
            return reaching
        reaching |= grown


def _tests_driving_the_apply_boundary(tree, served=frozenset()):
    """Every test function in *tree* that reaches the public apply entry.

    Deliberately action-AGNOSTIC. One function serves `plan`, `compile` and
    `apply`, and an assertion escape is no more acceptable on a plan witness
    than on an apply one; selecting on the action string would also mean reading
    an argument that is frequently a variable. Over-inclusion cannot leave the
    sibling sweep incomplete — under-inclusion is what did.
    """
    reaching = _boundary_callers(tree, served)
    for fn in ast.walk(tree):
        if not isinstance(fn, ast.FunctionDef) or not fn.name.startswith("test_"):
            continue
        if reaching & set(_call_names(fn)):
            yield fn


def _conditional_assertions(fn):
    """`(name, lineno)` for every assertion this function guards behind an `if`."""
    for node in ast.walk(fn):
        if not isinstance(node, ast.If):
            continue
        for inner in ast.walk(node):
            if isinstance(inner, ast.Assert):
                yield fn.name, node.lineno
                break


def test_a_witness_at_the_apply_boundary_asserts_without_an_escape():
    """Sweep of the mechanism across every artifact that has one.

    Two instances existed when this was written: the pin the fourth round
    replaced, and a #153 witness that read `build_id` and asserted the build
    registry's status only `if build_id` — so the day the envelope stopped
    carrying one, the assertion would have vanished rather than failed. Measured
    before changing it: the id is always present, so the guard was never doing
    anything except concealing its own absence. Both are unconditional now.
    """
    served = _served_boundary_wrappers()
    scanned, in_scope, unseeded, offenders = [], [], [], []
    for path in sorted(_TESTS_ROOT.rglob("test_*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError:  # pragma: no cover - a parse failure is another test's
            continue
        scanned.append(path)
        # The SAME census run twice, once without the served seed. The
        # difference is the population that enters only through the tool entry,
        # and comparing the two is the only thing that proves the seed is wired
        # into this sweep rather than merely computed beside it: dropping the
        # argument at this call site left every assertion green.
        unseeded += [fn.name for fn in _tests_driving_the_apply_boundary(tree)]
        for fn in _tests_driving_the_apply_boundary(tree, served):
            in_scope.append((path.name, fn.name))
            offenders += [(path.name, name, line)
                          for name, line in _conditional_assertions(fn)]

    # THE CENSUS PROVES ITSELF FIRST. Both floors are read off the run rather
    # than typed: this module must be among the files walked, and the selector
    # must have matched something. A sweep that silently sees nothing reports
    # exactly the same empty `offenders` as a clean tree.
    assert Path(__file__).resolve() in scanned, (
        "the census did not include its own file, so the root did not resolve"
    )
    assert in_scope, "the selector matched no test at all; the sweep is vacuous"
    assert served, (
        "no served wrapper was derived from the tool module, so every test that "
        "enters through one is silently outside this sweep"
    )
    assert len(in_scope) > len(unseeded), (
        "seeding the census with the served wrappers added no test, so either "
        "the seed never reached the selector or nothing enters through the tool "
        "entry any more: {0} seeded vs {1} unseeded".format(
            len(in_scope), len(unseeded)
        )
    )
    assert offenders == [], (
        "a test reaching the apply boundary guards an assertion behind a "
        "condition; an unreached case is then indistinguishable from a passing "
        "one: {0}".format(offenders)
    )


def test_the_escape_rule_refuses_the_shape_that_cost_four_rounds():
    """Non-vacuity: every call form the rule must see, and the ones it must not.

    Without this the rule above is a sweep over a set that happens to be empty,
    which is how a guard passes forever while modelling nothing.
    """
    def _hits(source, served=frozenset()):
        tree = ast.parse(source)
        return [list(_conditional_assertions(fn))
                for fn in _tests_driving_the_apply_boundary(tree, served)]

    # The shape that cost four rounds, named directly.
    assert _hits(
        "def test_x():\n"
        "    result = build_integration_action(c, p, 'apply', config={})\n"
        "    if result.get('write_attempted') is False:\n"
        "        assert result['error_code'] == 'A'\n"
    ) == [[("test_x", 3)]]

    # The same shape reached through the module-qualified form, which the first
    # version of this sweep did not see at all.
    assert _hits(
        "def test_x():\n"
        "    result = integration_builder.build_integration_action(c, p, 'apply')\n"
        "    if result.get('build_id'):\n"
        "        assert result['ok']\n"
    ) == [[("test_x", 3)]]

    # And through a module-local wrapper, which is how this suite's own
    # boundary tests are written.
    assert _hits(
        "def _apply(payload):\n"
        "    return build_integration_action(c, p, 'apply', config=payload)\n"
        "def test_x():\n"
        "    result = _apply({})\n"
        "    if result.get('build_id'):\n"
        "        assert result['ok']\n"
    ) == [[("test_x", 5)]]

    # And through a wrapper defined in ANOTHER module — the served tool entry,
    # which delegates to the apply function. Six existing tests enter this way
    # and no earlier version of the census could see one of them.
    assert _hits(
        "def test_x():\n"
        "    result = server.build_integration(profile='p', action='apply', config=c)\n"
        "    if result.get('build_id'):\n"
        "        assert result['ok']\n",
        served=frozenset({"build_integration"}),
    ) == [[("test_x", 3)]]

    # The SAME source with no served set is out of scope, which is what makes
    # the seeding load-bearing rather than decorative.
    assert _hits(
        "def test_x():\n"
        "    result = server.build_integration(profile='p', action='apply', config=c)\n"
        "    if result.get('build_id'):\n"
        "        assert result['ok']\n"
    ) == []

    # CONTROLS. An unconditional witness passes, and a test that never reaches
    # the boundary is out of scope by construction — the rule is derived from
    # the entry point, not from a file list, and a parametrized helper may
    # legitimately assert per case.
    assert _hits(
        "def test_x():\n"
        "    result = build_integration_action(c, p, 'apply', config={})\n"
        "    assert result['error_code'] == 'A'\n"
    ) == [[]]
    assert _hits(
        "def test_x():\n"
        "    for reason, names_it in CASES:\n"
        "        if not names_it:\n"
        "            assert 'not at fault' in detail\n"
    ) == []


# --- A recompiling consumer receives the PROJECTED table ----------------------
#
# Three instances in one slice, which is what makes this an invariant rather than
# a third patch. A caller computes the root-projected symbol table — the one
# carrying the idempotency contracts and per-call grants — and then hands a
# consumer that RECOMPILES the root a different, unprojected table. Semantic
# validation passes on evidence the recompile cannot see, so the gate passes
# where a report is written and fails where bytes are produced. It happened at
# the artifact compile, at the materialization-plan build, and at the pre-write
# dry emit, and each was found only because someone drove that exact route.
#
# THE POPULATION IS DERIVED, not listed. The first version of this guard named
# three functions in a frozen tuple, and review pointed out that this is the very
# class the guard exists to close, one level up: rename a consumer, or add a
# fourth, and the sweep stops watching it while still reporting success. The
# authority for "recompiles a root" is the compiler package's own declared
# entries, and the population is the transitive closure of what reaches them,
# narrowed to functions that actually accept a symbol table. Measured: that names
# SEVEN consumers where the hand-list named three.
_COMPILER_AUTHORITY = "boomi_mcp/compiler/process_ir/pipeline.py"



def _declared_compiler_entries(src_root):
    """The compile entries the compiler package itself publishes."""
    tree = ast.parse((src_root / _COMPILER_AUTHORITY).read_text(encoding="utf-8"))
    entries = set()
    for node in ast.walk(tree):
        is_all = (
            (isinstance(node, ast.AnnAssign)
             and getattr(node.target, "id", None) == "__all__")
            or (isinstance(node, ast.Assign)
                and any(getattr(t, "id", None) == "__all__" for t in node.targets))
        )
        if not is_all:
            continue
        for element in getattr(node.value, "elts", []):
            if isinstance(element, ast.Constant):
                # EVERY exported entry, with no prefix filter. Filtering on
                # `compile_` dropped `parse_and_compile_process_ir_v1`, which is
                # a public compile entry that takes a symbol table and reaches
                # the compiler through a PRIVATE core — so the closure could not
                # recover it either, and a wrapper built on it was invisible to
                # the whole sweep while the population floor still passed.
                entries.add(element.value)
    return entries


def _called_names(node):
    """Every callee name mentioned anywhere under `node`."""
    names = set()
    for inner in ast.walk(node):
        if isinstance(inner, ast.Call):
            callee = inner.func
            name = (callee.id if isinstance(callee, ast.Name)
                    else callee.attr if isinstance(callee, ast.Attribute) else None)
            if name:
                names.add(name)
    return names


def _recompiling_consumers(src_root):
    """`{function name: {symbol-table parameter names}}`, derived from the tree.

    A consumer recompiles if it transitively reaches a declared compile entry,
    and it is a CONSUMER of a table if it accepts one. Both halves come from the
    source: nothing here lists which functions matter.
    """
    signatures, callees = {}, {}
    for path in sorted(src_root.rglob("*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError:  # pragma: no cover - another test's problem
            continue
        for function in ast.walk(tree):
            if not isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            args = function.args
            params = [a.arg for a in args.posonlyargs + args.args + args.kwonlyargs]
            signatures.setdefault(function.name, []).append(params)
            callees.setdefault(function.name, set()).update(_called_names(function))

    reaching = set(_declared_compiler_entries(src_root))
    growing = True
    while growing:
        growing = False
        for name, called in callees.items():
            if name not in reaching and (called & reaching):
                reaching.add(name)
                growing = True

    consumers = {}
    for name in sorted(reaching):
        for params in signatures.get(name, []):
            carried = {p for p in params if "symbol" in p}
            if carried:
                consumers.setdefault(name, set()).update(carried)
    return consumers, signatures


def _consumer_modules(src_root):
    """`{consumer name: {dotted module}}` — where each derived consumer is defined."""
    consumers, _ = _recompiling_consumers(src_root)
    homes = {}
    for path in sorted(src_root.rglob("*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError:  # pragma: no cover - another test's problem
            continue
        dotted = ".".join(path.relative_to(src_root).with_suffix("").parts)
        for node in ast.walk(tree):
            if (isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                    and node.name in consumers):
                homes.setdefault(node.name, set()).add(dotted)
    return homes






def test_every_recompiling_consumer_on_the_evidenced_route_gets_the_grants():
    """The invariant, checked on the VALUE that arrives rather than on source.

    Five review rounds went into a static version of this, and each found the
    analysis wrong in a new place: the population was hand-listed, then the scope
    let a wrapper discard its caller's table, then the seed dropped a public
    entry, then a tuple index and a stale rebind read as lineage. The last round
    ended it: `materialize_canonical_process_xml` projects its table only under
    `if snapshot is not None`, which is DESIGNED — no snapshot means no account
    data, so there is nothing to enforce — and no syntactic rule can tell that
    conditional from a discard. In the same round, `placeholder_backed_symbols`
    turned out to be an in-tree helper that takes a table and explicitly CLEARS
    its root ref and grants, so "a call over carrying names" cannot mean
    preservation either. Both are facts about values, not about syntax.

    So the check moved to where the answer actually lives. The population is
    still DERIVED — the compiler package's own declared entries, closed over the
    call graph — but each consumer is wrapped and the evidenced route is DRIVEN,
    and the assertion is made against the table each one really received. That is
    the runtime authority the structural rule asks for: `process_root_ref` and
    `idempotency_grants` on the object, not a model of how it got there.
    """
    from _m12_11_support import ingested_operation_capture
    import importlib
    from unittest.mock import MagicMock, patch

    src_root = _TESTS_ROOT.parent / "src"
    consumers, _ = _recompiling_consumers(src_root)
    homes = _consumer_modules(src_root)
    assert len(consumers) >= 8, (
        "the derived population collapsed to %d; the compiler package's declared "
        "entries or the call graph moved, and a sweep over a shrunken population "
        "reports success without looking: %s" % (len(consumers), sorted(consumers))
    )

    sys.path.insert(0, str(_TESTS_ROOT))
    from test_issue_155_contract_projection import (  # noqa: E402
        _evidenced_reference_only_request,
    )

    record, operation, connection, request = _evidenced_reference_only_request()
    # THE ROOTS THIS TEST AUTHORED, read off the request it built. Every other
    # expectation in this check is supplied by the route: the projector's return
    # was the value under test, and the requested root that replaced it is a
    # caller-controlled argument — a consumer asking for the wrong root, or for
    # none, made the key wrong or skipped the branch. These literals are the one
    # thing nothing under test can steer.
    #
    # A SET, not `units[0]`. With a single-unit fixture the first unit's key was
    # a universal oracle, so a regression projecting EVERY process under that one
    # key stayed green while the check appeared to be about roots at all. The
    # two-root arm at the end of this test is what makes the set mean something.
    _AUTHORED_ROOTS = frozenset(
        unit.envelope.component_key for unit in request.intent.units)
    captures = (_TESTS_ROOT.parent
                / "tests/fixtures/boomi/connector_replay/captures"
                / ingested_operation_capture())
    operation_xml = (captures / "component_op_tgt.xml").read_text(encoding="utf-8")
    connection_xml = (captures / "component_connection.xml").read_text(encoding="utf-8")

    seen = []
    stack = []

    # #184 CDX-184-r20-02: the process-cached manifest's FIRST build replays the compiler over
    # the packaged behaviour corpus, whose tables name roots this request never authored.
    # That is process start-up, not the evidenced route, so it happens before any consumer is
    # watched; every compile the route itself makes is still observed below.
    from boomi_mcp.authoring.contract import build_authoring_contract_manifest

    build_authoring_contract_manifest()

    # THE SHARED SEAM, not a per-name patch. Both public compile entries call
    # `_compile_parsed_process_ir_v1`, so instrumenting it observes every
    # compile however it was reached — through a sibling entry, through the
    # package's own re-export, or through an alias this test has never heard of.
    # This repo had already learnt that lesson and written it down at
    # `_process_ir_capability_witnesses.record_compiles`; patching defining
    # modules by name was me re-deriving a weaker version of a mechanism that
    # already existed, and it left exactly the hole that comment describes.
    compiles = []
    routed = []
    frames = []
    dropped = []
    relocatable = []
    pending = {}
    unconsumed = []
    mismatched = []

    def _watch(module_name, consumer, parameters):
        module = importlib.import_module("boomi_mcp." + module_name.split("boomi_mcp.")[-1]
                                         if module_name.startswith("boomi_mcp")
                                         else module_name)
        original = getattr(module, consumer, None)
        if not callable(original):
            return None

        def recording(*args, **kwargs):
            table = None
            for parameter in parameters:
                if parameter in kwargs:
                    table = kwargs[parameter]
            if table is None:
                for candidate in args:
                    if hasattr(candidate, "idempotency_grants"):
                        table = candidate
            if table is None:
                return original(*args, **kwargs)
            root = getattr(table, "process_root_ref", None)
            seen.append((consumer, root,
                         len(getattr(table, "idempotency_grants", ()) or ())))
            if root is None:
                # Entered without a projection: this consumer owes nothing, and
                # the rootless relocatable table is a designed state.
                return original(*args, **kwargs)
            # CORRELATED, incoming against outgoing. Selecting observations by
            # the root the CORE saw meant a consumer erasing both the root and
            # the grants vanished from the comparison altogether, while the other
            # recompiles kept the positive assertions satisfied. The expectation
            # has to come from what the consumer was HANDED, so that erasing it
            # is a violation rather than an exemption.
            frames.append((consumer,
                           (root, tuple(getattr(table, "idempotency_grants", ()) or ()))))
            try:
                return original(*args, **kwargs)
            finally:
                frames.pop()

        return patch.object(module, consumer, recording)

    for consumer, parameters in sorted(consumers.items()):
        for module_name in sorted(homes.get(consumer, ())):
            watcher = _watch(module_name, consumer, parameters)
            if watcher is not None:
                stack.append(watcher)

    assert stack, "no derived consumer could be wrapped; this test proves nothing"

    from boomi_mcp.compiler.process_ir import connector_resolution as CR
    from boomi_mcp.compiler.process_ir import pipeline as _pipeline

    _core = _pipeline._compile_parsed_process_ir_v1

    tables_seen = []
    minted = []

    # WHAT THE PROJECTION ACTUALLY MINTED, captured at the authority itself. The
    # routed assertions compared only non-nullness and a count, so a consumer
    # substituting a DIFFERENT non-null root while keeping one grant passed — and
    # the compile gate enables grant lookup on any non-null root, so a foreign
    # root does not disable the check, it redirects it. Nothing downstream may
    # alter the pair; that is what "the projected table reaches the recompile"
    # means, and comparing shapes never said it.
    _project = CR.project_grants_for_root

    # THE ONE DELIBERATE CLEARING, identified by the helper that performs it.
    # A relocatable table describes no account, so it can hold no account-bound
    # grant, and this helper clears the root and the grants for exactly that
    # reason — it is a documented derivation, not a discard. Recording its
    # OUTPUTS BY IDENTITY is what lets the correlation refuse every other
    # clearing, including the same edit made anywhere else: measured, erasing the
    # projection at the materialization recompile changes no public result, so
    # nothing else in the suite would have objected.
    from boomi_mcp.authoring import process_materialization as _PM

    _placeholder = _PM.placeholder_backed_symbols

    def _recording_placeholder(*args, **kwargs):
        table = _placeholder(*args, **kwargs)
        # THE OBJECT, not its address. `id()` of a released object is reusable,
        # so a later rootless table could land on the same address and be taken
        # for the deliberate relocation — which would let it bypass both erasure
        # checks. A strong reference also keeps the object alive, so the address
        # cannot be recycled while this list holds it.
        relocatable.append(table)
        return table

    def _recording_projection(*args, **kwargs):
        # THE ROOT THAT WAS ASKED FOR, which is an argument rather than a result.
        # Keying obligations on the root the projector RETURNED meant the value
        # under test was also the key: a projection asked for one root and
        # answering with another stored itself under the wrong key, and the
        # compile that followed discharged it with nothing to compare. The
        # requested root is independent of everything the mutant controls
        # downstream, and it was already being intercepted here — the earlier
        # reading that this needed new machinery was simply wrong.
        requested = kwargs.get("process_root_ref")
        if requested is None and len(args) > 2:
            requested = args[2]
        table = _project(*args, **kwargs)
        pair = (table.process_root_ref, tuple(table.idempotency_grants or ()))
        minted.append(pair)
        if requested not in _AUTHORED_ROOTS:
            mismatched.append(("asked-for-an-unauthored-root",
                               sorted(_AUTHORED_ROOTS), requested))
        if requested is not None and table.process_root_ref != requested:
            mismatched.append(("projected-a-different-root", requested,
                               table.process_root_ref))
        # PENDING until a compile consumes it. The frame stack could only see a
        # consumer's INCOMING table, so a consumer entered rootless that projects
        # INTERNALLY — which the wet materialization path does — had no frame and
        # no expectation, and clearing its own projection before recompiling
        # passed every check while the public apply still succeeded. The mint
        # itself is the expectation: once a projection exists, the next compile
        # in this run must be given it.
        if requested is not None:
            # Keyed by the REQUEST, so a projector answering with a foreign root
            # leaves the requested root's obligation outstanding as well as
            # recording the substitution above.
            pending[requested] = pair
        elif pair[0] is not None:
            # KEYED BY ROOT. Two wrong shapes preceded this one and each was the
            # other's failure: a global STACK let a stale pair from one root
            # satisfy a compile for another, and a single SLOT fixed that by
            # discarding an unmatched obligation the moment any later root
            # minted — so a second root could skip its own projection, recompile
            # with the first root's table, and leave nothing to object. An
            # obligation belongs to the root it was minted for; keeping one per
            # root is what makes both shapes impossible rather than trading them.
            pending[pair[0]] = pair
        return table

    def _recording_core(ir, symbols, *args, **kwargs):
        compiles.append((getattr(symbols, "process_root_ref", None),
                         len(getattr(symbols, "idempotency_grants", ()) or ())))
        observed = (getattr(symbols, "process_root_ref", None),
                    tuple(getattr(symbols, "idempotency_grants", ()) or ()))
        routed.append(observed)
        is_relocatable = any(table is symbols for table in relocatable)
        if observed[0] is None and pending and not is_relocatable:
            # A ROOTLESS COMPILE WHILE AN OBLIGATION IS OUTSTANDING. Skipping
            # these left the erasure path open a second time: the violation was
            # recorded nowhere, and the next mint for the same root overwrote the
            # undischarged entry, so a valid later compile discharged it and the
            # run ended clean. Recording it here means the erasure cannot be
            # forgotten by anything that happens afterwards.
            mismatched.append(("erased-while-owed", sorted(pending), observed))
        if observed[0] is not None and not is_relocatable:
            owed = pending.get(observed[0])
            if owed is None:
                # A root-carrying table nobody minted for that root, or a second
                # delivery of one already consumed.
                mismatched.append(("unminted-or-replayed", observed))
            elif observed != owed:
                mismatched.append((owed, observed))
            else:
                del pending[observed[0]]
        if frames and observed != frames[-1][1] and not is_relocatable:
            # INCLUDING a transition to None. A consumer that erases the
            # projection on its way to the core is the defect, not an
            # observation to skip.
            dropped.append((frames[-1][0], frames[-1][1], observed))
        if getattr(symbols, "process_root_ref", None) is not None:
            # Kept so the entry nothing calls can be driven with a REAL projected
            # table and a real model, rather than a constructed pair whose shape
            # I would be choosing.
            tables_seen.append((symbols, ir))
        return _core(ir, symbols, *args, **kwargs)

    submitted = {}

    def _get_xml(_client, component_id, *_a, **_k):
        if component_id == "process-cid-1" and "xml" in submitted:
            return {"type": "process", "xml": submitted["xml"]}
        is_operation = component_id == operation.component_id
        return {
            "component_id": component_id,
            "type": "connector-settings",
            "version": operation.version if is_operation else connection.version,
            "xml": operation_xml if is_operation else connection_xml,
        }

    def _create(_client, _profile, payload_in):
        submitted["xml"] = payload_in["xml"]
        return {"_success": True, "component_id": "process-cid-1"}

    created = {"n": 0}

    def _component(*_a, **_k):
        created["n"] += 1
        return {"_success": True, "component_id": "cid-%d" % created["n"]}

    from boomi_mcp.categories.integration_builder import build_integration_action

    with contextlib.ExitStack() as entered:
        for watcher in stack:
            entered.enter_context(watcher)
        entered.enter_context(patch.object(
            _pipeline, "_compile_parsed_process_ir_v1", _recording_core))
        entered.enter_context(patch.object(
            CR, "project_grants_for_root", _recording_projection))
        entered.enter_context(patch.object(
            _PM, "placeholder_backed_symbols", _recording_placeholder))
        entered.enter_context(patch(
            "boomi_mcp.categories.integration_builder.paginate_metadata",
            lambda *a, **k: []))
        entered.enter_context(patch(
            "boomi_mcp.categories.integration_builder._execute_component", _component))
        entered.enter_context(patch(
            "boomi_mcp.categories.integration_builder.create_component", _create))
        entered.enter_context(patch(
            "boomi_mcp.categories.integration_builder.component_get_xml", _get_xml))
        entered.enter_context(patch(
            "boomi_mcp.categories.components._shared.component_get_xml", _get_xml))

        payload = request.model_dump(mode="json")
        compiled = build_integration_action(
            _evidenced_client(), "qa", "compile", config={"authoring_request": payload})
        assert compiled.get("_success") is True, compiled
        binding = compiled["authoring_result"]["revision_binding"]
        payload["expected_capability_revision"] = binding["capability_revision"]
        payload["expected_compile_hash"] = binding["compile_hash"]
        applied = build_integration_action(
            _evidenced_client(), "qa", "apply",
            config={"authoring_request": payload, "dry_run": False})

    assert applied.get("_success") is True, applied

    # EVERY compile on the route, counted at the seam. `assert seen` accepted a
    # single observation, so a recompile that moved to an uninstrumented binding
    # left this green — the guard's whole claim is population coverage, and one
    # observation is not a population.
    assert compiles, "no compile reached the shared core; the route changed"
    # SEVEN of the eight derived consumers execute on this route — MEASURED, not
    # chosen: the floor was 4 and accepted a route covering half the population.
    # The eighth, `parse_and_compile_process_ir_v1`, is not reached by the typed
    # authoring route at all — and a shared seam cannot observe a function
    # nothing calls, so leaving it at that was an admission that a regression
    # confined to that entry would pass. It is DRIVEN DIRECTLY below, with a real
    # projected table captured from this very route, so the derived population of
    # eight is covered in full: seven by the route, one by that arm.
    reached = {name for name, _root, _grants in seen}
    assert len(reached) >= 7, (
        "only %d derived consumers executed on the evidenced route, so this "
        "witness covers a fraction of the population it names: %s of %s"
        % (len(reached), sorted(reached), sorted(consumers))
    )
    # NOT compared against the consumer count: consumers nest, so several of them
    # ride one core compile. Measured on this route, 6 compiles reach the core
    # under 15 consumer entries, and any relation between the two numbers would
    # be a claim about the call graph rather than about the invariant.
    # EVERY routed projected observation must equal a pair the projection minted
    # — not merely look like one. This is the same exactness the directly driven
    # entries get; applying it only to those left the reached consumers, which
    # are most of the population, on the weaker check.
    assert minted, "the projection never ran on the evidenced route"
    assert all(root in _AUTHORED_ROOTS for root, _grants in routed if root is not None), (
        "a compile received a table projected for a root this request never "
        "authored: %s, authored %s" % (routed, sorted(_AUTHORED_ROOTS))
    )
    assert not mismatched, (
        "a compile received a table that is not the one minted for its root — "
        "the projection was replaced between the mint and the recompile: %s"
        % (mismatched,)
    )
    # EVERY ROOT'S OBLIGATION DISCHARGED. An erasure, or a consumer skipping its
    # own projection and recompiling with another root's table, both show up
    # here: the root that minted never got its table into a compile.
    assert not pending, (
        "a projection was minted for a root and no compile ever received it, so "
        "the recompile ran without the grants that root was granted: %s"
        % (sorted(pending),)
    )
    assert not dropped, (
        "a recompiling consumer was handed a projected table and delivered a "
        "different one to the compile core — the projection was altered or "
        "erased on the way through: %s" % (dropped,)
    )
    stray = [pair for pair in routed if pair[0] is not None and pair not in minted]
    assert not stray, (
        "a recompiling consumer on the route received a projected table that no "
        "projection minted — the root or the grants were substituted between the "
        "mint and the recompile: %s not in %s" % (stray, minted)
    )

    # Every compile of a ROOT-PROJECTED table carries that root's grants. This is
    # the invariant itself, asserted on the value at the one seam both public
    # entries pass through.
    assert all(grants >= 1 for root, grants in compiles if root is not None), (
        "a root-projected table reached the compile core WITHOUT the grants "
        "minted for that root: %s" % (compiles,)
    )
    assert any(root is not None for root, _ in compiles), (
        "no projected table reached the compile core, so this witness would "
        "pass with the projection removed entirely: %s" % (compiles,)
    )

    # A table with a root ref is one that was projected FOR that root, so its
    # grants are the ones the compiler will look for. A consumer reached with a
    # root ref but no grants is the defect this whole line of work started from:
    # the recompile refusing evidence semantic validation had already accepted.
    # THE EIGHTH CONSUMER, driven directly. A shared seam cannot observe a
    # function nothing calls, and the typed authoring route does not call this
    # one — so the floor of seven was an admission that a regression confined to
    # it would pass. It is a public compile entry taking a symbol table, so the
    # invariant applies to it whether or not this repo's own routes reach it.
    assert tables_seen, "no projected table was captured to drive unreached entries with"
    captured_table, captured_ir = tables_seen[0]

    # DERIVED COVERAGE, not a hard-coded omission. The first version named the
    # one entry the route missed, so a ninth consumer arriving unreached would
    # have left the test green while it still claimed the population. The set is
    # computed instead: whatever the route did not reach must have a driver here,
    # and a member with neither fails, naming itself.
    drivers = {
        "parse_and_compile_process_ir_v1": lambda table: _pipeline.
        parse_and_compile_process_ir_v1(captured_ir.model_dump(mode="json"), table),
    }
    unreached = set(consumers) - reached
    undriven = sorted(unreached - set(drivers))
    assert not undriven, (
        "these derived consumers are neither reached by the evidenced route nor "
        "driven directly, so a projection regression in them would pass: %s"
        % (undriven,)
    )

    for consumer in sorted(unreached):
        direct = []

        def _recording_core_direct(ir, symbols, *args, **kwargs):
            # THE TABLE ITSELF, not its shape. Recording `(root, len(grants))`
            # accepted a substituted table carrying a FOREIGN root and one grant
            # — which is worse than dropping the projection, because grants would
            # then be checked against a root that never minted them.
            direct.append((getattr(symbols, "process_root_ref", None),
                           tuple(getattr(symbols, "idempotency_grants", ()) or ())))
            return _core(ir, symbols, *args, **kwargs)

        with patch.object(_pipeline, "_compile_parsed_process_ir_v1",
                          _recording_core_direct):
            drivers[consumer](captured_table)

        assert direct, "%s did not reach the compile core" % (consumer,)
        expected = (captured_table.process_root_ref,
                    tuple(captured_table.idempotency_grants or ()))
        assert all(observed == expected for observed in direct), (
            "%s did not deliver the table it was given to the shared core — the "
            "root or the grants changed on the way through: got %s, gave %s"
            % (consumer, direct, [expected])
        )

    granted = [row for row in seen if row[1] is not None]
    assert granted, (
        "no consumer on the evidenced route received a root-projected table: %s" % (seen,)
    )
    assert all(row[2] >= 1 for row in granted), (
        "a consumer that recompiles a projected root received it WITHOUT the "
        "grants minted for it: %s" % (granted,)
    )
    rootless = [row for row in seen if row[1] is None]
    # Rootless tables are legitimate and deliberately unconstrained — the
    # relocatable materialization table clears its root ref precisely so a grant
    # minted for another root cannot satisfy this one. Recorded, not asserted on.
    assert isinstance(rootless, list)


def test_each_authored_root_is_projected_for_its_own_process():
    """The multi-root arm, as a PAIRING and driven through the wet route.

    Three aggregates preceded this one and each was defeated the same way. The
    authored root was `units[0]`, which a single-unit fixture made a universal
    oracle. Then it was the SET of authored keys, which two consumers swapping
    roots between them satisfies exactly. And the witness drove only compile,
    leaving any regression confined to the apply-only projection unreached. The
    common defect is mine and it is one defect: I kept asserting that the right
    NAMES appeared somewhere, which is not the claim. The claim is that each
    process is projected under its OWN key.

    So the units carry DIFFERENT documents — sharing one IR removed the only
    value-level distinction between them — the recorder keys each projection by
    the document it was handed, and the request goes through compile AND
    revision-bound wet apply.
    """
    import inspect
    import json
    from unittest.mock import MagicMock, patch

    from boomi_mcp.categories.integration_builder import build_integration_action
    from boomi_mcp.compiler.process_ir import connector_resolution as CR

    sys.path.insert(0, str(_TESTS_ROOT))
    from _m12_11_support import (  # noqa: E402
        ProcessAuthoringUnitV1,
        ProcessComponentEnvelopeV1,
    )
    from _process_ir_capability_witnesses import _connector_scope  # noqa: E402
    from boomi_mcp.models.process_ir import parse_process_ir_v1  # noqa: E402
    from test_issue_155_contract_projection import (  # noqa: E402
        _evidenced_reference_only_request,
    )

    record, operation, connection, request = _evidenced_reference_only_request()
    first = request.intent.units[0]
    # A DIFFERENT document for the second root. Reusing the first unit's IR left
    # the two units indistinguishable by value, so a projection could be paired
    # with either and no assertion could tell.
    second = ProcessAuthoringUnitV1(
        envelope=ProcessComponentEnvelopeV1(
            component_key="proc2", name="M12.15 Second", action="create",
            depends_on=first.envelope.depends_on),
        process_ir=parse_process_ir_v1(_connector_scope(
            protected="$ref:op",
            upstream="$ref:getop",
            retry={"count": 3},
            idempotency={"kind": "key_reference",
                         "contract_ref": record.contract_ref},
        )),
    )
    two_root = request.model_copy(update={"intent": request.intent.model_copy(
        update={"units": (first, second)})})

    from _m12_11_support import ingested_operation_capture

    def _fingerprint(ir):
        return json.dumps(ir.model_dump(mode="json"), sort_keys=True)

    expected = {_fingerprint(unit.process_ir): unit.envelope.component_key
                for unit in two_root.intent.units}
    assert len(expected) == 2, (
        "the two units are indistinguishable by value, so no pairing assertion "
        "below can mean anything"
    )

    captures = (_TESTS_ROOT.parent
                / "tests/fixtures/boomi/connector_replay/captures"
                / ingested_operation_capture())
    operation_xml = (captures / "component_op_tgt.xml").read_text(encoding="utf-8")
    connection_xml = (captures / "component_connection.xml").read_text(encoding="utf-8")
    submitted = {}

    def _get_xml(_client, component_id, *_a, **_k):
        if component_id in submitted:
            return {"type": "process", "xml": submitted[component_id]}
        is_operation = component_id == operation.component_id
        return {
            "component_id": component_id,
            "type": "connector-settings",
            "version": operation.version if is_operation else connection.version,
            "xml": operation_xml if is_operation else connection_xml,
        }

    created = {"n": 0}

    def _create(_client, _profile, payload_in):
        created["n"] += 1
        component_id = "process-cid-%d" % created["n"]
        submitted[component_id] = payload_in["xml"]
        return {"_success": True, "component_id": component_id}

    def _component(*_a, **_k):
        created["n"] += 1
        return {"_success": True, "component_id": "cid-%d" % created["n"]}

    pairs = []
    _project = CR.project_grants_for_root

    def _recording_projection(*args, **kwargs):
        root = kwargs.get("process_root_ref")
        if root is None and len(args) > 2:
            root = args[2]
        ir = kwargs.get("root_ir") if "root_ir" in kwargs else (args[0] if args else None)
        # THE PAIRING, PER STAGE: which document was projected under which key,
        # and by whom. A route-wide set of valid pairs is still satisfied when one
        # stage reuses the first unit's document for both units and another stage
        # supplies the missing name — every pair valid, the mapping broken. The
        # caller's own frame names the stage, so the check no longer depends on a
        # list of stages I would have had to write down.
        stage = inspect.stack()[1].function
        pairs.append((stage, _fingerprint(ir) if ir is not None else None, root))
        return _project(*args, **kwargs)

    def _drive(action, config_extra=None):
        with patch("boomi_mcp.categories.integration_builder.paginate_metadata",
                   lambda *a, **k: []), \
             patch("boomi_mcp.categories.integration_builder._execute_component",
                   _component), \
             patch("boomi_mcp.categories.integration_builder.create_component", _create), \
             patch("boomi_mcp.categories.components._shared.component_get_xml", _get_xml), \
             patch("boomi_mcp.categories.integration_builder.component_get_xml", _get_xml), \
             patch.object(CR, "project_grants_for_root", _recording_projection):
            return build_integration_action(
                _evidenced_client(), "qa", action,
                config=dict({"authoring_request": payload}, **(config_extra or {})))

    payload = two_root.model_dump(mode="json")
    compiled = _drive("compile")
    assert compiled.get("_success") is True, compiled
    compile_pairs = list(pairs)

    binding = compiled["authoring_result"]["revision_binding"]
    payload["expected_capability_revision"] = binding["capability_revision"]
    payload["expected_compile_hash"] = binding["compile_hash"]
    del pairs[:]
    # THE WET ROUTE, not compile alone. A regression confined to the apply-only
    # projection is invisible to a compile-only witness, and the single-root wet
    # guard cannot see a root mistake at all.
    applied = _drive("apply", {"dry_run": False})
    assert applied.get("_success") is True, applied
    apply_pairs = list(pairs)

    authored_roots = set(expected.values())
    for label, observed in (("compile", compile_pairs), ("apply", apply_pairs)):
        assert observed, "no projection ran on the %s route" % (label,)
        stages = sorted({stage for stage, _document, _root in observed})
        for stage in stages:
            here = [(document, root) for stage_, document, root in observed
                    if stage_ == stage and document is not None]
            if not here:
                continue
            wrong = [(expected.get(document), root) for document, root in here
                     if expected.get(document) != root]
            assert not wrong, (
                "on the %s route, stage %r projected a process under another "
                "process's key: %s" % (label, stage, wrong)
            )
            # PER STAGE, not per route. A route-wide set of individually valid
            # pairs is still satisfied when one stage reuses the first unit's
            # document for both units and a different stage supplies the missing
            # name. Every stage that projects at all must cover both roots.
            assert {root for _document, root in here} == authored_roots, (
                "on the %s route, stage %r did not project both authored roots: "
                "%s" % (label, stage, sorted({r for _d, r in here}))
            )

    # THE WET LEG ACTUALLY RAN. An apply that stopped at the dry-run branch would
    # leave the apply-only stages unobserved while every assertion above still
    # held on the compile-side projections it did make.
    apply_stages = {stage for stage, _document, _root in apply_pairs}
    assert {"_apply_plan", "materialize_canonical_process_xml"} <= apply_stages, (
        "the wet leg did not reach the apply-only projections: %s"
        % (sorted(apply_stages),)
    )
    assert len(submitted) == len(expected), (
        "the wet apply did not submit one component per authored process: %s"
        % (sorted(submitted),)
    )


# THE TERMINAL-COUNT GUARD WAS REMOVED HERE, and this note is the record of why.
#
# It was added as the structural extension for `DC-155-J` when two checkpoints
# were found recording fewer evaluations than the archive held. Over five review
# rounds it drew NINE critical findings, every one of the form "this guard is
# wrong", and in both directions: it refused valid records — mid-window states,
# offset windows, withdrawn closures, gap records, a same-loop replay after a
# decision to proceed — and it also missed the very understatement it existed to
# catch, because it chose which row to audit using the number it was auditing.
# It never once caught a real occurrence. Both real occurrences were found by the
# review gate, which is the mechanism the workflow already relies on for this.
#
# The subject it tried to mechanise is not a fact in the tree. It is the
# workflow's own decision policy — absolute boundaries, windows that reset,
# cumulative history kept across resets, revisions, withdrawals, supersessions,
# gap records that are not decisions, deferrals that end a loop and deferrals
# that do not, and replays that keep a loop's identity. Every version of the
# guard was a second model of that policy sitting beside the one that already
# works, and a second model of a policy is not an invariant derived from a
# runtime authority — it is the hand-model this class is named for, one level up.
#
# A guard that has never caught its case and has cost nine criticals is not
# insurance against the defect; it is another instance of it. Removing it was
# pre-committed at this loop's sixth-evaluation checkpoint, on exactly the
# condition that then occurred.


def test_an_operation_record_cannot_authorise_a_replay_its_capture_never_saw():
    """A record authorises the replay its capture SAW, not the one it describes.

    A class-level evidence row already binds its verdict to its capture's replay
    observation, and that binding is what makes a row evidence rather than an
    assertion. An operation record carried no such binding: it named a semantics
    definition with a duplicate guarantee, and nothing compared that guarantee
    with what the capture recorded. The issue-level architect gate demonstrated
    the consequence by probe — a record whose replay observation said NOT
    EXERCISED, with its same-effect semantics and stale digest untouched, loaded
    and minted a contract and a grant.

    Provenance-shaped authorization is not replay evidence, and the difference is
    exactly one comparison.
    """
    import json
    from importlib import resources
    from unittest.mock import patch

    import boomi_mcp.connector_replay.registry as registry_module

    raw = json.loads(resources.files("boomi_mcp.connector_replay")
                     .joinpath("registry_v1.json").read_text("utf-8"))
    assert raw["operation_records"], "no packaged record; this witness would be vacuous"
    guaranteed = {d["semantics_id"]: d["duplicate_guarantee"]
                  for d in raw["semantics_definitions"]}
    record = raw["operation_records"][0]
    assert record["capture"]["summary"]["replay"] == guaranteed[record["semantics_id"]], (
        "the packaged record's observation and guarantee already disagree; the "
        "control below would then prove nothing"
    )

    def _load(payload):
        text = json.dumps(payload)

        class _Resource:
            def joinpath(self, *_a):
                return self

            def read_text(self, *_a, **_k):
                return text

        registry_module.load_registry.cache_clear()
        try:
            with patch.object(registry_module.resources, "files",
                              lambda *_a, **_k: _Resource()):
                return registry_module.load_registry(), None
        except registry_module.RegistryInvalid as exc:
            return None, str(exc)
        finally:
            registry_module.load_registry.cache_clear()

    loaded, refusal = _load(raw)
    assert loaded is not None and refusal is None, (
        "the untampered packaged registry must still load, or the refusal below "
        "is not evidence of anything: %s" % (refusal,)
    )

    # THE FORGERY IS COMPLETED, not merely attempted. A tamper that leaves the
    # record digest stale is now caught one check earlier, by the digest itself —
    # so asserting on the raw edit would prove the digest check works and say
    # NOTHING about this one. The digest is recomputed here through the published
    # minter, exactly as a forger with the repository in front of them would, and
    # what remains is a record that is internally consistent in every mechanical
    # way and still claims a replay its capture never saw.
    from boomi_mcp.connector_replay.digests import operation_record_digest_v1
    from boomi_mcp.connector_replay.models import OperationContractRecordV1

    for observation in ("not_exercised", "duplicate_effect"):
        tampered = json.loads(json.dumps(raw))
        row = tampered["operation_records"][0]
        row["capture"]["summary"]["replay"] = observation
        row["record_digest"] = operation_record_digest_v1(
            OperationContractRecordV1(**{**row, "record_digest": "0" * 64})
        )
        loaded, refusal = _load(tampered)
        assert loaded is None, (
            "a record whose capture observed %r still loaded, so it authorises a "
            "replay nobody saw" % (observation,)
        )
        assert "capture observed" in (refusal or ""), (
            "the refusal came from some other check, so this witness no longer "
            "covers the semantic one: %s" % (refusal,)
        )

    # ...and the digest check itself, which the completed forgery above steps
    # past: the same edit WITHOUT recomputing the digest is refused for naming an
    # identity that no longer describes the record it sits on.
    stale = json.loads(json.dumps(raw))
    stale["operation_records"][0]["capture"]["summary"]["replay"] = "not_exercised"
    loaded, refusal = _load(stale)
    assert loaded is None and "hashes to" in (refusal or ""), refusal










def test_a_replay_claim_needs_the_artifact_that_observes_replay():
    """The architect's own probe, closed — and graded past a confound.

    A replay observation states what a SECOND identical execution did to the
    counterparty. Only an endpoint readback observes that; an execution record
    reports that a call completed, not what it left behind. The validator
    demanded the readback for a state EFFECT and left the replay beside it
    unguarded, so a record could name execution-side sources only, claim a replay
    left the effect unchanged, be re-digested through the published minter so
    every other check agreed, and mint a grant.

    THE CONFOUND, which live QA surfaced before it could fool this test: the
    effect rule runs first and `state_unchanged_after_replay` demands the
    readback on its own. A probe using that effect grades the OLD guard and reads
    as a pass, which would make this rule look enforced while being inert. So the
    probe uses `read_only`, the one effect the older rule does not gate — which
    is also exactly the shape the architect's probe used.
    """
    import json

    from boomi_mcp.connector_replay.models import ClosedCaptureObservationsV1
    from boomi_mcp.connector_replay.registry import load_registry

    base = load_registry().operation_records[0].capture.summary.model_dump(mode="json")
    # Sorted, or the sources validator refuses first and confounds the result.
    without_readback = tuple(sorted(
        s for s in base["sources"] if s != "endpoint_readback"))
    assert without_readback, "the packaged record names no other source"

    for replay in ("same_effect", "same_result", "duplicate_effect",
                   "conflict_without_second_effect"):
        payload = dict(base, effect="read_only", replay=replay,
                       sources=without_readback)
        with pytest.raises(Exception) as raised:
            ClosedCaptureObservationsV1.model_validate(payload)
        message = str(raised.value)
        assert "replay" in message and "endpoint readback" in message, (
            "refused by some other rule, so this proves nothing about the replay "
            "requirement: %s" % message[:200]
        )

    # NOT-EXERCISED claims nothing about a second execution, so it needs nothing.
    ClosedCaptureObservationsV1.model_validate(
        dict(base, effect="read_only", replay="not_exercised",
             sources=without_readback))

    # ...and the packaged record itself, which names the readback, still loads.
    assert load_registry().operation_records


#: The one spelling an affected-SHA cell may use when it names no commit.
#: EXACT, not a prefix or a substring: "treat anything without a backticked SHA as
#: the tip form" is how a malformed cell passes as a conforming one.
_TIP_FORM = "correction on the tip"


def affected_sha_cell_deviates(reviewed_sha: str, sha_cell: str) -> bool:
    """Whether a cell fails the convention. ONE definition, used by both nodes.

    The non-vacuity node used to carry its own copy of this comparison, so
    deleting the real check left both tests green — a control that cannot fail
    when the thing it controls is removed is not a control.
    """
    import re

    if sha_cell == _TIP_FORM:
        return False
    named = re.findall(r"`([0-9a-f]{7,40})`", sha_cell)
    if not named:
        return True
    return not any(s.startswith(reviewed_sha) or reviewed_sha.startswith(s)
                   for s in named)




































