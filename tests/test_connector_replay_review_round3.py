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

_CAPTURES = Path(__file__).resolve().parents[1] / "docs/architecture/evidence/issue-155/captures"
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


#: The class roster and each row's instance count at the moment this guard was
#: written. A tally may GROW as a class recurs; it may never shrink, and the
#: roster may not lose a row — a floor on the total missed the removal of a
#: small class while catching the removal of a large one.
_EXPECTED_CLASS_COUNTS = {
    "DC-155-A": 0,
    "DC-155-B": 1,
    "DC-155-C": 13,
    "DC-155-D": 3,
    "DC-155-E": 1,
    "DC-155-F": 0,
    "DC-155-G": 8,
    "DC-155-H": 1,
    "DC-155-I": 2,
    "DC-155-J": 4,
    "DC-155-J2": 3,
    "DC-155-K": 38,
    "DC-155-L": 31,
    "DC-155-M": 1,
}

#: Instances counted before finding ids were enumerated. A CLOSED set: a
#: remainder that only has to add up can absorb a real instance.
_UNROWED = {"DC-155-C": 2, "DC-155-I": 1}

#: Finding rows that name their defect class in PROSE rather than by its id, because
#: they were written before that class had one and are byte-frozen from their first
#: commit. Frozen by name so the gap cannot grow or be swapped into; a row written now
#: has an id available and must use it.
#: Finding rows that NAME a defect class without being an instance of it, each with
#: the reason. Frozen so the set cannot grow silently: a row that names a class is an
#: instance of it unless it appears here.
_NOT_AN_INSTANCE = {
    "SELF-155-r5-02": "the sibling sweep the structural fix owed, not a new instance",
    "SELF-155-r28-02": "likewise a sweep",
    "EVAL-155-08": "names the adjacent process class, not this one",
    "QA-155-r30-04": "finding-refuted, and a refuted row never counts",
    "QA-155-r35-05": "not-validated — a recorded limitation rather than a defect",
    "QA-155-r35-06": "the class's own positive controls",
    "CDX-155-r38-02a": "a revision of a row already counted; counting both double-counts",
    "SELF-155-r44-03a": "likewise a revision of a counted row",
    "CDX-155-r45-01a": "likewise a revision of a counted row",
    "CDX-155-r45-02a": "likewise a revision of a counted row",
    "SELF-155-r55-02": "the structural fix its class owed, not a new instance",
    "CDX-155-r54-01a": "a revision of a row already counted",
    "SELF-155-r51-04": "the structural fix its class owed, not a new instance of it",
    "SELF-155-r47-01a": "a revision of a row that is itself a non-instance",
    "SELF-155-r47-01": "the class being DISCHARGED — the regeneration done correctly from the final tree — not a new instance of regenerating from a non-final one",
}

_CLASS_NAMED_IN_PROSE = [
    "CDX-155-r22-06",
    "CDX-155-r23-01",
    "CDX-155-r27-01",
    "CDX-155-r27-05",
    "CDX-155-r6-01",
    "QA-155-r21-02",
    "QA-155-r23-01",
    "QA-155-r27-01",
    "QA-155-r27-05",
]


def test_every_defect_class_tally_equals_its_own_enumeration():
    """Counts are DERIVED for EVERY class row, because a hand-typed one was wrong.

    The first version of this guard covered only the rows that happened to carry a
    parenthesised list — five of thirteen — so a count written in words, a row with no
    list at all, a duplicated id and a fabricated one all passed it, and the largest
    tally in the table disagreed with its own names unchecked. That is the same
    incomplete-sweep failure the guard was written to end, inside the guard.

    Every row with a non-zero count must now name its instances, the names must exist
    as finding rows, and instances counted before ids were enumerated must say so
    explicitly rather than sit in the gap between a number and a list.
    """
    import re

    ledger = (
        Path(__file__).resolve().parents[1]
        / "docs/architecture/ISSUE_155_AUDIT_LEDGER.md"
    ).read_text()

    known_rows = set(re.findall(r"^\| ([A-Z]+-155-[A-Za-z0-9-]+) \|", ledger, re.M))
    assert known_rows, "no finding rows parsed — the existence check would be vacuous"

    checked = 0
    owners: dict[str, str] = {}
    seen_counts: dict[str, int] = {}
    class_names = {
        line.split("|")[1].strip()
        for line in ledger.splitlines()
        if line.startswith("| DC-155-")
    }
    for line in ledger.splitlines():
        if not line.startswith("| DC-155-"):
            continue
        checked += 1
        name = line.split("|")[1].strip()
        count_cell = line.split("|")[4]

        # A CANONICAL GRAMMAR for the whole cell, not a check per escape. Four rounds
        # of this guard closed one shape each — a count in words, a row with no list,
        # a duplicate, a zero row naming instances — and the fifth arrived as a
        # remainder written OUTSIDE the parentheses, where every one of those checks
        # looked past it. The cell now has exactly two legal forms and anything else
        # is refused, so the space is closed rather than sampled.
        canonical = re.fullmatch(
            r"\s*(?P<count>\d+)\s*(?:\((?P<body>[^()]*)\)(?P<tail>\s*—[^()]*)?)?\s*",
            count_cell,
        )
        assert canonical, (
            f"{name}: the instance cell {count_cell.strip()[:60]!r} is not a count, "
            "optionally one parenthesised instance list, optionally a dash-introduced "
            "note — and nothing else"
        )
        declared = int(canonical.group("count"))
        body = canonical.group("body")
        tail = canonical.group("tail") or ""
        # The note may explain; it may not COUNT. An instance or a remainder written
        # after the list would read as declared while sitting outside everything that
        # checks the list — which is exactly how the previous shape of this guard was
        # walked past.
        assert not re.findall(r"`([A-Z]+-155-[A-Za-z0-9-]+)`", tail), (
            f"{name}: the note after its instance list names findings; instances "
            "belong in the list, where they are counted"
        )
        assert "unrowed" not in tail, (
            f"{name}: the note after its instance list carries a remainder"
        )
        seen_counts[name] = declared

        if declared == 0:
            # A zero row carries NO body at all. Validating the body's CONTENTS was
            # the previous shape of this, and it looked past a remainder written
            # outside the parentheses; the grammar above makes that unwritable, and
            # this asserts the remaining freedom is unused.
            assert not body, f"{name}: declares zero instances and carries {body!r}"
            continue

        assert body, f"{name}: declares {declared} instances and names none"
        # The LIST is a list, not free text. Accepting arbitrary characters between
        # the backticks let an unbackticked identifier sit inside it, counted by
        # nobody and read by a person as declared.
        parts = [segment.strip() for segment in body.split(",")]
        remainders = 0
        for segment in parts:
            # FULL match on both forms. A prefix match let `+2 unrowed <id>` through on
            # the strength of its opening, carrying an uncounted identifier in the one
            # segment allowed to be something other than an identifier.
            if re.fullmatch(r"`[A-Z]+-155-[A-Za-z0-9-]+`", segment):
                continue
            assert re.fullmatch(r"\+\d+ unrowed", segment), (
                f"{name}: {segment[:48]!r} in its instance list is neither a "
                "backticked finding id nor exactly one unrowed remainder"
            )
            remainders += 1
        assert remainders <= 1, (
            f"{name}: carries {remainders} remainders, and only the first is read"
        )
        enumerated = re.findall(r"`([A-Z]+-155-[A-Za-z0-9-]+)`", body)
        unrowed = re.search(r"\+(\d+) unrowed", body)
        unrowed = int(unrowed.group(1)) if unrowed else 0

        # FROZEN, not merely arithmetic. A remainder that only has to add up verifies
        # nothing about membership: it can absorb a real instance, and a row naming
        # nobody at all passes on the strength of its own remainder. These two are the
        # complete historical set — instances counted before ids were enumerated — and
        # no row may invent another or grow one.
        assert unrowed == _UNROWED.get(name, 0), (
            f"{name}: an unrowed remainder of {unrowed} is not the frozen "
            f"{_UNROWED.get(name, 0)}; historical instances are a closed set"
        )

        assert declared == len(enumerated) + unrowed, (
            f"{name}: declares {declared} but names {len(enumerated)}"
            + (f" plus {unrowed} unrowed" if unrowed else "")
        )
        assert len(set(enumerated)) == len(enumerated), (
            f"{name}: an instance is listed twice"
        )
        missing = [i for i in enumerated if i not in known_rows]
        assert not missing, f"{name}: names instances with no finding row: {missing}"
        collides = [i for i in enumerated if i in class_names]
        assert not collides, f"{name}: names a CLASS as an instance: {collides}"

        for instance in enumerated:
            other = owners.get(instance)
            assert other is None, (
                f"{instance} is claimed by both {other} and {name}; the "
                "second-instance trigger requires one defect class per finding, and "
                "two owners double-count it in both tallies"
            )
            owners[instance] = name

    # Every declared class must have been seen. A count floor missed the removal of a
    # SMALL row while catching the removal of a large one, so the roster is pinned by
    # NAME and the per-row counts are pinned individually.
    assert set(_EXPECTED_CLASS_COUNTS) == class_names, (
        "the class roster changed: only expected "
        f"{sorted(set(_EXPECTED_CLASS_COUNTS) - class_names)}, only present "
        f"{sorted(class_names - set(_EXPECTED_CLASS_COUNTS))}"
    )
    assert checked == len(_EXPECTED_CLASS_COUNTS), (
        f"{checked} class rows parsed against {len(_EXPECTED_CLASS_COUNTS)} declared"
    )
    for cls, floor in _EXPECTED_CLASS_COUNTS.items():
        assert seen_counts[cls] >= floor, (
            f"{cls}: declares {seen_counts[cls]} instances, below its recorded floor "
            f"of {floor} — a class tally may grow, never shrink"
        )


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


def test_a_zero_count_class_row_may_not_name_instances():
    """The early return bypassed every check below it."""
    import re

    ledger = (
        Path(__file__).resolve().parents[1]
        / "docs/architecture/ISSUE_155_AUDIT_LEDGER.md"
    ).read_text()

    zero_rows = 0
    for line in ledger.splitlines():
        if not line.startswith("| DC-155-"):
            continue
        cell = line.split("|")[4]
        if not re.match(r"\s*0\s", cell):
            continue
        zero_rows += 1
        assert not re.findall(r"`([A-Z]+-155-[A-Za-z0-9-]+)`", cell), (
            f"{line.split('|')[1].strip()}: zero instances, yet names one"
        )
    assert zero_rows == 2, (
        f"the ledger has {zero_rows} zero-count class rows, not the 2 measured when this "
        "check was written; a class opening or closing changes what it covers"
    )


def test_each_enumerated_instance_is_classed_to_the_row_that_claims_it():
    """The class table and the finding rows must agree, in BOTH directions.

    Deriving the table from the rows' own class assignments is the fix that would end
    this guard's recurrence outright — the count would not be authored at all. It is
    not available here: finding rows are byte-frozen from their first commit, and the
    earliest ones name their class in prose, written before that class had an id, so
    they cannot be retro-labelled. What IS derivable is the other direction — every id
    a class row claims must, where its own row names a class id, name THIS one.

    The exempt set is frozen rather than open-ended, so the gap cannot grow: a row
    written today has an id available and must use it.
    """
    import re

    ledger = (
        Path(__file__).resolve().parents[1]
        / "docs/architecture/ISSUE_155_AUDIT_LEDGER.md"
    ).read_text()

    row_class: dict[str, str | None] = {}
    for line in ledger.splitlines():
        match = re.match(r"^\| ([A-Z]+-155-[A-Za-z0-9-]+) \|", line)
        if not match:
            continue
        cells = line.split("|")
        if len(cells) < 10:
            continue
        named = re.search(r"(DC-155-[A-Z0-9]+)", cells[6])
        row_class[match.group(1)] = named.group(1) if named else None

    exempt, mismatched, checked = [], [], 0
    owners_by_instance: dict[str, str] = {}
    for line in ledger.splitlines():
        if not line.startswith("| DC-155-"):
            continue
        name = line.split("|")[1].strip()
        listed = re.search(r"\(([^)]*)\)", line.split("|")[4])
        if not listed:
            continue
        for instance in re.findall(r"`([A-Z]+-155-[A-Za-z0-9-]+)`", listed.group(1)):
            declared_by_row = row_class.get(instance)
            if declared_by_row is None:
                exempt.append(instance)
                owners_by_instance[instance] = name
                continue
            checked += 1
            owners_by_instance[instance] = name
            if declared_by_row != name:
                mismatched.append((instance, name, declared_by_row))

    assert not mismatched, (
        "a class row claims an instance whose own row names a different class: "
        + ", ".join(f"{i} claimed by {c} but classed {d}" for i, c, d in mismatched)
    )
    assert checked >= 30, f"only {checked} instances were cross-checked"
    # THE REVERSE DIRECTION. Iterating only the ids the class rows already name can
    # never see a finding row the table forgot — which is the stale-count
    # inconsistency this check exists to prevent, walking in through the door the
    # check does not watch. Measured when this was added: six classed rows were
    # genuinely uncounted and are now listed; seven are non-instances and are frozen
    # below with the reason each is one.
    unowned = []
    for instance, declared_class in sorted(row_class.items()):
        if declared_class is None:
            continue
        if owners_by_instance.get(instance) == declared_class:
            continue
        unowned.append((instance, declared_class))
    unexpected = [i for i, _ in unowned if i not in _NOT_AN_INSTANCE]
    assert not unexpected, (
        "these finding rows name a defect class that does not count them, and are "
        f"not recorded as non-instances: {unexpected}"
    )
    stale = [i for i in _NOT_AN_INSTANCE if i not in {x for x, _ in unowned}]
    assert not stale, (
        f"these are recorded as non-instances but their class now counts them: {stale}"
    )

    assert sorted(set(exempt)) == _CLASS_NAMED_IN_PROSE, (
        "the set of instances naming their class in prose rather than by id has "
        f"changed: only expected {sorted(set(_CLASS_NAMED_IN_PROSE) - set(exempt))}, "
        f"only present {sorted(set(exempt) - set(_CLASS_NAMED_IN_PROSE))}. It is frozen "
        "by NAME, not by count, so one row cannot be swapped for another"
    )


def test_a_closing_report_names_the_current_wave_sha_and_proves_darkness():
    """STRUCTURAL: the closing report cannot outlive the tree it certifies.

    Recorded repeatedly as `DC-155-G`. This applies the rule below to the real
    repository; the rule itself is exercised on every run by the table further down.
    There is ONE implementation — an earlier version kept a second copy here, so the
    always-on cases could stay green while the real checks were deleted, which is the
    lost-fix failure the extraction was supposed to end.
    """
    import json
    import re
    import subprocess

    ledger_path = (
        Path(__file__).resolve().parents[1]
        / "docs/architecture/ISSUE_155_AUDIT_LEDGER.md"
    )
    ledger = ledger_path.read_text()
    if "## Slice B — closing report" not in ledger:
        pytest.skip("no closing report yet; the check applies once one is written")

    report = ledger[ledger.index("## Slice B — closing report"):]
    report = report[: report.index("\n## ")] if "\n## " in report else report
    root = ledger_path.parents[2]

    def _git(*args):
        out = subprocess.run(["git", *args], capture_output=True, text=True, cwd=root)
        assert out.returncode == 0, f"git {' '.join(args)} failed: {out.stderr.strip()}"
        return out.stdout.strip()

    # ANCHORED TO THE REPORT'S OWN COMMIT, not to the moving tip. Comparing against
    # HEAD~1 held only until the next commit: the ledger schedules slices C through F
    # after this one, so the first documentation commit that followed would have made
    # this fail on a certified tree that had not changed at all.
    owning = _git("log", "--format=%H", "-1", "-S", "## Slice B — closing report",
                  "--", str(ledger_path.relative_to(root)))
    closing = owning or "HEAD"        # uncommitted while N is being written
    parent = _git("rev-parse", f"{closing}~1") if owning else _git("rev-parse", "HEAD")

    class _RepoRev:
        def __call__(self, ref):
            return parent if ref == "HEAD~1" else _git("rev-parse", ref)

        def precedes(self, earlier, later):
            return subprocess.run(
                ["git", "merge-base", "--is-ancestor", earlier, later],
                capture_output=True, cwd=root).returncode == 0

    archive = ledger_path.parents[0] / "evidence" / "issue-155" / "commit-reviews"
    archived = {
        d.name: (d / "last-reviewed-sha").read_text().strip()
        for d in archive.iterdir()
        if d.is_dir() and (d / "last-reviewed-sha").exists()
    }

    violation = _closing_chain_violation(report, _RepoRev(), archived)
    assert violation is None, (
        f"the closing report violates the closing chain: {violation}. W and N−1 must "
        f"be named, N−1 must be the commit before the one carrying this report "
        f"({parent[:7]}), W must strictly precede it, and an archived review must "
        "attest N−1"
    )

    changed = _git("diff", "--name-only",
                   re.findall(r"`([0-9a-f]{7,40})`\s*\(\*\*W\*\*\)", report)[0],
                   "HEAD", "--", "src", "tests", "scripts").splitlines()
    assert not [c for c in changed if c.strip()], (
        f"the closing report claims darkness from its wave SHA, but these differ: {changed}"
    )


def _closing_chain_violation(report, rev, archived):
    """The closing chain's rule, evaluated against supplied facts.

    Extracted so the RULE is exercised on every run. The guard that applies it to the
    real ledger skips while no closing report exists, so its logic was untested for as
    long as it mattered least — and a correction to it was silently lost to a
    `git reset --hard` during a simulation, recorded as fixed, and caught only by a
    reviewer reading the committed file. Facts are passed in; nothing here touches git
    or the ledger, so the cases below are cheap and always run.
    """
    import re

    named = re.findall(r"`([0-9a-f]{7,40})`\s*\(\*\*W\*\*\)", report)
    if len(named) != 1:
        return "W"
    boundary = re.findall(r"`([0-9a-f]{7,40})`\s*\(\*\*N−1\*\*\)", report)
    if len(boundary) != 1:
        return "N-1"
    w, n1 = named[0], boundary[0]
    if rev(n1) != rev("HEAD~1"):
        return "not-head-1"
    if rev(w) == rev(n1):
        return "equal"
    if not rev.precedes(w, n1):
        return "out-of-order"
    named_runs = re.findall(r"`(cdx-review\.[A-Za-z0-9]+)`", report)
    if not any(archived.get(r, "").startswith(n1) or archived.get(r) == rev(n1)
               for r in named_runs):
        return "unattested"
    return None


class _FakeRev:
    """A tiny linear history: A -> B -> C, with C as HEAD."""

    _order = ["aaaaaaa", "bbbbbbb", "ccccccc"]

    def __call__(self, ref):
        return {"HEAD": "ccccccc", "HEAD~1": "bbbbbbb"}.get(ref, ref)

    def precedes(self, earlier, later):
        return self._order.index(earlier) < self._order.index(later)


def _report(w, n1, run="cdx-review.aaaaaa"):
    return (
        "## Slice B — closing report\n\n"
        f"| `{w}` (**W**) | wave |\n| `{n1}` (**N−1**) | review `{run}` |\n"
    )


@pytest.mark.parametrize(
    "w,n1,archived,expected",
    [
        # the one correct shape: W strictly precedes N−1, N−1 is the commit before the
        # tip, and an archived review carries N−1 as the SHA it reviewed
        ("aaaaaaa", "bbbbbbb", {"cdx-review.aaaaaa": "bbbbbbb"}, None),
        # the reflexive hole four reviews took to close
        ("bbbbbbb", "bbbbbbb", {"cdx-review.aaaaaa": "bbbbbbb"}, "equal"),
        # a boundary that is not the commit before the tip
        ("aaaaaaa", "aaaaaaa", {"cdx-review.aaaaaa": "aaaaaaa"}, "not-head-1"),
        # a review that attests a different tree
        ("aaaaaaa", "bbbbbbb", {"cdx-review.aaaaaa": "aaaaaaa"}, "unattested"),
        # no archived review at all
        ("aaaaaaa", "bbbbbbb", {}, "unattested"),
        # the wave gate must precede the reviewed boundary, never follow it
        ("ccccccc", "bbbbbbb", {"cdx-review.aaaaaa": "bbbbbbb"}, "out-of-order"),
    ],
)
def test_the_closing_chain_rule_admits_exactly_one_shape(w, n1, archived, expected):
    """The rule itself, always exercised — not only once a report exists."""
    assert _closing_chain_violation(_report(w, n1), _FakeRev(), archived) == expected


def test_every_collected_node_is_pinned_by_the_manifest():
    """The floor is a MINIMUM, so an unpinned test never fails anything.

    The required-node manifest is checked in one direction — every required node must
    still collect — which catches a deletion and misses an addition. Three times in
    this slice a batch added tests and left them unpinned: 217 of them once, the
    closure guard once, and its five rule cases once. Each time the gate stayed green
    while the new work was protected by nothing, because collection merely exceeded
    the floor.

    This asserts the other direction for this slice's own file: everything collected
    from it is named in the manifest. Scoped there deliberately — the manifest is
    another issue's artifact and widening the assertion to the whole suite would make
    this test fail for reasons that are not this slice's to fix.
    """
    import json
    import subprocess

    root = Path(__file__).resolve().parents[1]
    manifest = root / "tests/fixtures/wave_gate/test_nodes.jsonl"
    rows = [json.loads(l) for l in manifest.read_text().splitlines() if l.strip()]
    pinned = {r["node_id"] for r in rows[1:]}

    mine = Path(__file__).name
    collected = subprocess.run(
        [sys.executable, "-m", "pytest", f"tests/{mine}", "--collect-only", "-q",
         "-p", "no:cacheprovider"],
        capture_output=True, text=True, cwd=root,
        env={**os.environ, "PYTHONPATH": "src", "PYTHONDONTWRITEBYTECODE": "1"},
    )
    assert collected.returncode == 0, f"collection failed: {collected.stderr[-400:]}"
    nodes = [
        ln.strip() for ln in collected.stdout.splitlines()
        if ln.startswith(f"tests/{mine}::")
    ]
    assert len(nodes) > 40, f"only {len(nodes)} nodes collected; the check would be thin"

    unpinned = sorted(n for n in nodes if n not in pinned)
    assert not unpinned, (
        "these tests collect but are not required by the manifest, so deleting them "
        f"would leave every gate green: {unpinned}"
    )
