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
#:
#: NEVER REGENERATE THIS FROM THE LEDGER. The assertion below is `>=`, so a growing
#: tally needs no edit here, and re-deriving the numbers makes the floor a mirror of
#: the present instead of a record of the past. Measured: shrinking a tally fails the
#: guard, and re-deriving the table in the same batch makes it pass. Only a NEW class
#: is added, with the count it has at that moment; existing entries stay as written.
#: `test_the_recorded_floors_never_move_down` now enforces exactly that.
_EXPECTED_CLASS_COUNTS = {
    # A NEW (mechanism, authority) pair, minted rather than folded into a letter
    # whose authority is unrelated — see the class row for why that matters to
    # recurrence accounting.
    "DC-155-W": 3,  # OVERCOUNT-CORRECTED-FROM-4-TO-3
    "DC-155-A": 0,
    "DC-155-B": 1,
    "DC-155-C": 13,
    "DC-155-D": 3,
    "DC-155-E": 1,
    "DC-155-F": 0,
    "DC-155-G": 9,
    "DC-155-H": 1,
    "DC-155-I": 2,
    "DC-155-J": 4,
    "DC-155-J2": 3,
    # Minted at its SECOND instance, which is when the structural-fix trigger fires:
    # an identifier written in a shape the ledger scanner cannot parse. The first
    # instance named the pair in prose before a letter existed for it, so the tally
    # read zero while the mechanism was already recorded.
    "DC-155-R": 3,
    # OVERCOUNT-CORRECTED: minted at two, but the second "instance" was the review
    # finding that produced the first's correction — one defect reported twice is
    # one instance, and a floor that locks in an over-count is as wrong as one that
    # lets a real instance vanish.
    "DC-155-T": 1,  # OVERCOUNT-CORRECTED-FROM-2-TO-1
    # Minted at the issue-level review round for a defect whose mechanism is
    # placement relative to a commit point, not a missing format constraint.
    "DC-155-V": 1,
    # Minted at the issue-level architect gate, where one reviewer-grouped finding
    # carried three defects sharing one pair: a fact the boundary established and
    # the record it produces then dropped. Counted as ONE row because that is what
    # the ledger holds; the structural fix fired on arrival rather than on a second
    # instance, which is what a class-level finding is for.
    "DC-155-U": 1,
    # Minted when two findings already recorded under two unrelated classes turned
    # out to share one pair: a hand-model of what git itself reports, against git.
    "DC-155-S": 3,
    "DC-155-K": 38,
    "DC-155-L": 37,
    "DC-155-M": 1,
    "DC-155-N": 1,
    "DC-155-O": 1,
    "DC-155-P": 1,
    "DC-155-Q": 1,
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
    "CDX-155-r226-02": "the SUPERSEDED original of a class reclassification — the "
    "one finding is counted at the revision, under the class it actually belongs to",
    "CDX-155-r225-03": "the SUPERSEDED original of a class reclassification — the "
    "one finding is counted at the revision, under the class it actually belongs to",
    "CDX-155-r224-04a": "a REVISION correcting a count inside a disposition — one "
    "finding, counted at the original",
    "CDX-155-r223-03": "the SUPERSEDED original of a class reclassification — the one "
    "finding is counted at the revision, which is where the corrected class lives",
    "CDX-155-r45-01c": "a REVISION withdrawing a retier the corrected severity rule "
    "does not support — one finding, counted at the original",
    "QA-155-r66-03c": "a REVISION withdrawing a retier the corrected severity rule "
    "does not support — one finding, counted at the original",
    "CDX-155-r45-01b": "a REVISION correcting a DERIVED TIER that read only one of the "
    "severity rule's two disjuncts — one finding, counted at the original",
    "CDX-155-r94-02a": "a REVISION correcting a DERIVED TIER that read only one of the "
    "severity rule's two disjuncts — one finding, counted at the original",
    "QA-155-r24-05a": "a REVISION correcting a DERIVED TIER that read only one of the "
    "severity rule's two disjuncts — one finding, counted at the original",
    "QA-155-r65-03a": "a REVISION correcting a DERIVED TIER that read only one of the "
    "severity rule's two disjuncts — one finding, counted at the original",
    "QA-155-r66-03b": "a REVISION correcting a DERIVED TIER that read only one of the "
    "severity rule's two disjuncts — one finding, counted at the original",
    "SELF-155-r126-01a": "a REVISION correcting a DERIVED TIER that read only one of the "
    "severity rule's two disjuncts — one finding, counted at the original",
    "CDX-155-r216-01a": "a REVISION replacing a procedural disposition with the "
    "structural one the class was owed — one finding, counted at the original",
    "CDX-155-r210-01b": "a REVISION correcting the BLOCKING CLASS of a verification-surface "
    "finding recorded as machine-served — one finding, counted at the original",
    "CDX-155-r210-02a": "a REVISION correcting the BLOCKING CLASS of a verification-surface "
    "finding recorded as machine-served — one finding, counted at the original",
    "CDX-155-r210-03a": "a REVISION correcting the BLOCKING CLASS of a verification-surface "
    "finding recorded as machine-served — one finding, counted at the original",
    "CDX-155-r210-04a": "a REVISION correcting the BLOCKING CLASS of a verification-surface "
    "finding recorded as machine-served — one finding, counted at the original",
    "CDX-155-r210-05a": "a REVISION correcting the BLOCKING CLASS of a verification-surface "
    "finding recorded as machine-served — one finding, counted at the original",
    "CDX-155-r211-01a": "a REVISION correcting the BLOCKING CLASS of a verification-surface "
    "finding recorded as machine-served — one finding, counted at the original",
    "CDX-155-r211-02a": "a REVISION correcting the BLOCKING CLASS of a verification-surface "
    "finding recorded as machine-served — one finding, counted at the original",
    "CDX-155-r211-03a": "a REVISION correcting the BLOCKING CLASS of a verification-surface "
    "finding recorded as machine-served — one finding, counted at the original",
    "CDX-155-r211-04a": "a REVISION correcting the BLOCKING CLASS of a verification-surface "
    "finding recorded as machine-served — one finding, counted at the original",
    "CDX-155-r212-01a": "a REVISION correcting the BLOCKING CLASS of a verification-surface "
    "finding recorded as machine-served — one finding, counted at the original",
    "CDX-155-r212-02a": "a REVISION correcting the BLOCKING CLASS of a verification-surface "
    "finding recorded as machine-served — one finding, counted at the original",
    "CDX-155-r212-03a": "a REVISION correcting the BLOCKING CLASS of a verification-surface "
    "finding recorded as machine-served — one finding, counted at the original",
    "CDX-155-r213-01a": "a REVISION correcting the BLOCKING CLASS of a verification-surface "
    "finding recorded as machine-served — one finding, counted at the original",
    "CDX-155-r213-02a": "a REVISION correcting the BLOCKING CLASS of a verification-surface "
    "finding recorded as machine-served — one finding, counted at the original",
    "CDX-155-r213-03a": "a REVISION correcting the BLOCKING CLASS of a verification-surface "
    "finding recorded as machine-served — one finding, counted at the original",
    "CDX-155-r213-04a": "a REVISION correcting the BLOCKING CLASS of a verification-surface "
    "finding recorded as machine-served — one finding, counted at the original",
    "CDX-155-r214-01a": "a REVISION correcting the BLOCKING CLASS of a verification-surface "
    "finding recorded as machine-served — one finding, counted at the original",
    "CDX-155-r214-02a": "a REVISION correcting the BLOCKING CLASS of a verification-surface "
    "finding recorded as machine-served — one finding, counted at the original",
    "CDX-155-r214-03a": "a REVISION correcting the BLOCKING CLASS of a verification-surface "
    "finding recorded as machine-served — one finding, counted at the original",
    "CDX-155-r214-04a": "a REVISION correcting the BLOCKING CLASS of a verification-surface "
    "finding recorded as machine-served — one finding, counted at the original",
    "QA-155-r61-02a": "a REVISION correcting a DERIVED TIER that read only the first disjunct of the severity rule — one finding, counted at "
    "the original",
    "QA-155-r64-03a": "a REVISION correcting a DERIVED TIER that read only the first disjunct of the severity rule — one finding, counted at "
    "the original",
    "CDX-155-r210-01a": "a REVISION correcting a DERIVED TIER that read only the first disjunct of the severity rule — one finding, counted at "
    "the original",
    "CDX-155-r204-02a": "a REVISION naming, in the record's own vocabulary, the severity refutation its original argued without marking — one finding, counted at "
    "the original",
    "QA-155-r196-01a": "a REVISION naming, in the record's own vocabulary, the severity refutation its original argued without marking — one finding, counted at "
    "the original",
    "QA-155-r72-01a": "a REVISION naming, in the record's own vocabulary, the severity refutation its original argued without marking — one finding, counted at "
    "the original",
    "CDX-155-r197-09b": "a REVISION carrying the structural answer for the "
    "affected-SHA class — one finding, counted at the original",
    "CDX-155-r197-06b": "a REVISION correcting an affected-SHA cell — one finding, "
    "counted at the original",
    "CDX-155-r197-09a": "a REVISION correcting an affected-SHA cell and an "
    "incomplete fix — one finding, counted at the original",
    "CDX-155-r197-10a": "a REVISION correcting an affected-SHA cell — one finding, "
    "counted at the original",
    "CDX-155-r197-06a": "a REVISION correcting a DERIVED TIER I chose instead of "
    "deriving — one finding, counted at the original",
    "ARCH-155-r13-06a": "a REVISION supplying the class, tier and per-bullet "
    "verdict its original carried empty — one finding, counted at the original",
    "QA-155-r71-03b": "a REVISION correcting the RATIONALE of a revision, after "
    "live QA measured the premise false at both arms — one finding, counted at "
    "the original",
    "ARCH-155-r13-01a": "a REVISION supplying the class, tier and disposition its "
    "original carried empty on arrival — one defect, counted at the original",
    "ARCH-155-r13-03a": "a REVISION supplying the class, tier and disposition its "
    "original carried empty on arrival — one defect, counted at the original",
    "ARCH-155-r13-05a": "a REVISION supplying the class and the SEVERITY that "
    "measurement moved — one finding, counted at the original",
    "QA-155-r71-03a": "a REVISION recording that a limit went stale when a later "
    "correction changed what it described — counted at the original",
    "CDX-155-r187-02a": "a REVISION correcting the CLAIM its original's disposition "
    "made, not a second defect — the class row counts the original once",
    "QA-155-r4-01a": "a REVISION supplying the half of the disposition the "
    "original claimed complete — one defect, counted at the original",
    "QA-155-r4-02": "a RECURRENCE of QA-155-r3-04 measured at a later round — one "
    "defect reported by two gates is one instance, counted at the original",
    "SELF-155-r65-01a": "a REVISION recording that the original's premise no longer "
    "holds on the current tree — one finding, counted at the original",
    "QA-155-r55-04b": "a REVISION applying the fix its own escalation recorded for "
    "whoever took it up — same defect, one instance, counted at the original",
    "CDX-155-r73-01a": "a REVISION applying the correction the original escalated "
    "when a spent slice-B window forbade it — same defect, now fixed at a gate "
    "whose checkpoint window is fresh; the class row counts the original once",
    "CDX-155-r73-02a": "a REVISION applying the correction the original escalated "
    "when a spent slice-B window forbade it — same defect, now fixed at a gate "
    "whose checkpoint window is fresh; the class row counts the original once",
    "ARCH-155-r12-01a": "a REVISION supplying the disposition the original left "
    "pending, not a second defect — the class row counts the original once",
    "ARCH-155-r12-02a": "a REVISION supplying the disposition the original left "
    "pending, not a second defect — the class row counts the original once",
    "ARCH-155-r12-03a": "a REVISION supplying the disposition the original left "
    "pending, not a second defect — the class row counts the original once",
    "CDX-155-r182-02a": "a REVISION replacing two dispositions with one, not a second "
    "defect — the class row counts the original once",
    "SELF-155-r119-01a": "a REVISION correcting an evidence citation, not a second defect "
    "— the class row counts the original once",
    "CDX-155-r171-02a": "a REVISION replacing a disposition the contract does not admit "
    "with `fixed`, not a second defect — the class row counts the original once",
    "CDX-155-r148-01a": "a REVISION supplying the disposition the original lacked, "
    "not a second defect — the class row counts the original once",
    "CDX-155-r144-02a": "a REVISION narrowing an overstated resolution, not a second "
    "defect — the class row counts the original once",
    "QA-155-r66-03a": "a REVISION correcting an identifier, not a second defect — the row's "
    "text belongs to the report's r66-04 and its class row counts it once",
    "QA-155-r66-01": "ONE defect found by two gates — the Stage-2 review found it in the "
    "source and live QA found it in the account, in the same delta. The workflow counts "
    "distinct post-reconciliation defects, so it is counted once, on CDX-155-r141-01",
    "QA-155-r62-01b": "a REVISION of QA-155-r62-01, which its class row already counts",
    "QA-155-r62-01a": "a REVISION of QA-155-r62-01, which its class row already counts",
    "SELF-155-r100-01a": "a REVISION of SELF-155-r100-01, which its class row already counts",
    "CDX-155-r133-01b": "a REVISION of CDX-155-r133-01, which its class row already counts",
    "CDX-155-r134-01": "its class is CORRECTED by CDX-155-r134-01a; the original names the "
    "superseded class",
    "CDX-155-r133-01a": "a REVISION of CDX-155-r133-01, which its class row already counts "
    "as the instance — the same defect re-dispositioned, never a second occurrence",
    "CDX-155-r128-01a": "a REVISION of CDX-155-r128-01, which the class table already counts "
    "as the instance — the same defect re-dispositioned, never a second occurrence",
    "CDX-155-r120-04": "its class is CORRECTED by CDX-155-r120-04a",
    "EVAL-155-13a": "its class is CORRECTED by EVAL-155-13b; the original names the superseded class",
    "SELF-155-r98-01b": "a revision of a revision; the original is what counts",
    "SELF-155-r98-01c": "likewise; the original is the counted row",
    "EVAL-155-13": "its class is CORRECTED by EVAL-155-13a; the original names the superseded class",
    "CDX-155-r110-01": "superseded by CDX-155-r110-01a, which refutes it; a refuted row is not an instance",
    "CDX-155-r103-01": "its class is CORRECTED by CDX-155-r103-01a; the original names the superseded class",
    "CDX-155-r104-01": "likewise corrected by CDX-155-r104-01a",
    "CDX-155-r98-02": "its class is CORRECTED by CDX-155-r98-02a, which is the counted row; the original names the superseded class",
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
    "QA-155-r2-01a": "a revision of a row already counted; counting both double-counts",
    "QA-155-r2-01b": "a revision of a revision; the original is the counted row",
    "ARCH-155-r4-01a": "a revision of a row already counted; counting both double-counts",
    "ARCH-155-r4-03a": "likewise a revision of a counted row",
    "ARCH-155-r4-04a": "likewise a revision of a counted row",
    "ARCH-155-r5-01a": "likewise a revision of a counted row",
    "ARCH-155-r5-04a": "likewise a revision of a counted row",
    "ARCH-155-r5-06a": "likewise a revision of a counted row",
    "QA-155-r55-04a": "likewise a revision of a counted row",
    "QA-155-r52-02a": "likewise a revision of a counted row",
    "SELF-155-r47-01": "the class being DISCHARGED — the regeneration done correctly from the final tree — not a new instance of regenerating from a non-final one",
}

_CLASS_NAMED_IN_PROSE = [
    # Recorded when the issue-level architect gate's findings were first written:
    # a raw finding is filed at reconciliation, and its defect class is assigned
    # THEN, so the row as first committed says so in prose rather than naming an
    # id. Its class is supplied by the revision that carries its disposition —
    # `ARCH-155-r12-03a` — and the class row counts the original once.
    "ARCH-155-r12-01",
    "ARCH-155-r12-02",
    "ARCH-155-r12-03",
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


def _declared_class_floors(source):
    """The floor table as written in one revision of this file."""
    import ast
    import re

    found = re.search(r"_EXPECTED_CLASS_COUNTS = (\{[^}]*\})", source)
    return ast.literal_eval(found.group(1)) if found else None


def _floor_regression(previous_sources, current_source):
    """What the current table did to the HIGHEST floor each class ever carried.

    Compared against every recorded revision, not against one neighbour. A single
    neighbour is HEAD, and on a committed tree HEAD is the current bytes — so a floor
    lowered and committed in one step compared equal to itself and passed, which is
    the state CI and the wave gate always run in. Measured: the lowering passed once
    committed. The ceiling over all revisions has no such blind spot, and it also
    refuses a floor raised and then quietly walked back down.
    """
    import re

    ceiling: dict[str, int] = {}
    for source in previous_sources:
        for cls, floor in (_declared_class_floors(source) or {}).items():
            ceiling[cls] = max(ceiling.get(cls, 0), floor)
    if not ceiling:
        return None  # the table has never been recorded
    current = _declared_class_floors(current_source)
    if current is None:
        return "removed"
    # ONE DECLARED ESCAPE, because a ratchet with none refuses a genuine correction as
    # firmly as a silent shrink, and a tally recorded too HIGH is as wrong as one
    # recorded too low. A class whose entry carries the `OVERCOUNT-CORRECTED` marker
    # may go down: the marker is a declaration in the source, not a judgement made
    # here, so the escape is visible in a diff and has to be written on purpose. Every
    # other class still ratchets.
    # BOUND TO THE CEILING IT CORRECTS. A bare marker excused the class forever: once
    # a real second instance raised the floor, the same comment would have accepted
    # the next genuine drop too, which turns a one-time correction into a permanent
    # hole. The marker names the value it corrects DOWN FROM, so it stops applying the
    # moment the class rises past it and has to be rewritten deliberately if ever
    # needed again.
    # BOUND TO ONE TRANSITION, not to a ceiling. Naming only the value corrected down
    # FROM left the escape reusable: if the class rose back to that ceiling a later
    # drop matched again, and a drop straight past the target matched too. The marker
    # names BOTH ends, so it authorises exactly the one correction it describes and
    # nothing else — any other movement, in either direction, ratchets normally.
    marker_re = r'"(DC-[\w-]+)":\s*\d+[,}\s]*#\s*OVERCOUNT-CORRECTED-FROM-(\d+)-TO-(\d+)'
    corrected = {c: (int(a), int(b))
                 for c, a, b in re.findall(marker_re, current_source)}

    # SPENT EVENTS, not spent classes, and read from the floor history rather than
    # from whether the marker survived. Keying on the class alone rejected a DIFFERENT
    # later correction — a class corrected 3→2, grown to 4, then genuinely over-counted
    # again — while keying on the marker's survival meant deleting the comment on the
    # growth commit made the original transition reusable. A correction is the event
    # `(class, from, to)`: it is spent once the history shows the class reaching `to`
    # and afterwards rising above it, whatever the comment says at any point.
    chronological = list(reversed(previous_sources))
    spent = set()
    for cls in set(corrected) | set(ceiling):
        seq = [(_declared_class_floors(src) or {}).get(cls) for src in chronological]
        seq = [v for v in seq if v is not None]
        # A CORRECTION IS A CONSECUTIVE DOWNWARD STEP in the recorded history, not any
        # pair of a larger earlier value and a smaller later one. Reading it the loose
        # way, a class corrected 5→1 and then legitimately grown 1→2→3 looked as though
        # a 5→2 correction had already happened, because a 5 appeared before a 2 and a
        # 3 came after — so a genuinely new 3→2 correction was refused. A value reached
        # by GROWTH is not the endpoint of anything.
        for i in range(1, len(seq)):
            frm, to = seq[i - 1], seq[i]
            if frm <= to:
                continue                      # growth, or unchanged: no event here
            if any(later > to for later in seq[i + 1:]):
                # the step happened AND the class later rose above its endpoint, so
                # that particular correction is a past event and cannot be replayed.
                spent.add((cls, frm, to))

    lowered = sorted(
        c for c, floor in ceiling.items()
        if current.get(c, -1) < floor
        and (corrected.get(c) != (floor, current.get(c, -1))
             or (c, floor, current.get(c, -1)) in spent))
    return f"lowered: {lowered}" if lowered else None


@pytest.mark.parametrize(
    "previous,current,expected",
    [
        (["_EXPECTED_CLASS_COUNTS = {'A': 3}"], "_EXPECTED_CLASS_COUNTS = {'A': 3}", None),
        (["_EXPECTED_CLASS_COUNTS = {'A': 3}"], "_EXPECTED_CLASS_COUNTS = {'A': 4}", None),
        (["_EXPECTED_CLASS_COUNTS = {'A': 3}"], "_EXPECTED_CLASS_COUNTS = {'A': 3, 'B': 1}", None),
        (["_EXPECTED_CLASS_COUNTS = {'A': 3}"], "_EXPECTED_CLASS_COUNTS = {'A': 2}", "lowered: ['A']"),
        (["_EXPECTED_CLASS_COUNTS = {'A': 3}"], "_EXPECTED_CLASS_COUNTS = {'B': 3}", "lowered: ['A']"),
        (["_EXPECTED_CLASS_COUNTS = {'A': 3}"], "nothing here", "removed"),
        (["nothing here"], "_EXPECTED_CLASS_COUNTS = {'A': 3}", None),
        # the committed-lowering hole: the newest revision already carries the lowered
        # value, so a neighbour comparison sees no change and the ceiling still does
        (["_EXPECTED_CLASS_COUNTS = {'A': 5}", "_EXPECTED_CLASS_COUNTS = {'A': 2}"],
         "_EXPECTED_CLASS_COUNTS = {'A': 2}", "lowered: ['A']"),
        # raised, then walked back to where it started
        (["_EXPECTED_CLASS_COUNTS = {'A': 3}", "_EXPECTED_CLASS_COUNTS = {'A': 9}"],
         "_EXPECTED_CLASS_COUNTS = {'A': 3}", "lowered: ['A']"),
        ([], "_EXPECTED_CLASS_COUNTS = {'A': 3}", None),
    ],
)
def test_the_floor_rule_admits_only_growth(previous, current, expected):
    """The rule itself, always exercised — the repository check needs a prior commit."""
    assert _floor_regression(previous, current) == expected


def _floor_regression_in_repo(rel, current_source, git):
    """The comparison as the repository performs it — ALL recorded revisions.

    Extracted for the reason this whole file keeps rediscovering: a rule tested alone
    leaves its CALLER free to regress, and here the caller's regression is the exact
    defect the rule exists to catch — comparing the file against itself reports no
    lowering, forever. It read `HEAD` alone until a reviewer pointed out that on a
    committed tree those are the same bytes, which made the guard inert in CI and in
    every wave-gate run: the one environment it had to work in.
    """
    revisions = git("log", "--format=%H", "--", rel).split()
    return _floor_regression(
        [git("show", f"{sha}:{rel}", check=False) for sha in revisions], current_source
    )


def _repo_with_committed_floors(tmp_path, committed_floors):
    """A real repository whose recorded revisions declare these floors, in order."""
    import subprocess

    root = tmp_path / "floors"
    root.mkdir()

    def git(*args, check=True):
        # BYTES, then surrogateescape — never `text=True`. A legal non-UTF-8 filename
        # under a watched tree makes `git status -z` emit raw unquoted bytes, and a
        # strict decode raises before this MANDATORY guard can return its verdict: a
        # crash where a refusal belongs. `scripts/wave_gate.py::_status` already reads
        # the same stream this way, so this matches the repository's own handling
        # rather than inventing a second one.
        out = subprocess.run(["git", *args], capture_output=True, cwd=root)
        text = out.stdout.decode("utf-8", "surrogateescape")
        if check:
            err = out.stderr.decode("utf-8", "surrogateescape").strip()
            assert out.returncode == 0, f"git {' '.join(args)} failed: {err}"
        return text

    git("init", "-q", "-b", "main")
    git("config", "user.email", "qa@example.invalid")
    git("config", "user.name", "qa")
    for floor in committed_floors:
        (root / "guard.py").write_text(f"_EXPECTED_CLASS_COUNTS = {{'A': {floor}}}\n")
        git("add", "-A")
        git("commit", "-qm", f"floor {floor}")
    return git


@pytest.mark.parametrize(
    "committed,working,expected",
    [
        ([3], 4, None),                 # a class recurred; the floor may follow upwards
        ([3], 3, None),                 # untouched
        ([3], 2, "lowered: ['A']"),     # lowered in the working tree
        # ALREADY COMMITTED, which is the state CI and every wave-gate run are in.
        # Reading only the newest revision compares these bytes with themselves and
        # reports nothing — measured on this repository before the ceiling replaced it.
        ([5, 2], 2, "lowered: ['A']"),
        # raised and then walked back down over two commits
        ([3, 9, 3], 3, "lowered: ['A']"),
    ],
)
def test_the_repository_comparison_reads_every_recorded_revision(
    tmp_path, committed, working, expected
):
    """The caller, driven — not the rule alone, and not one revision of it.

    Pointing the comparison at the working file on BOTH sides passes every test that
    exercises only the rule. Pointing it at HEAD alone passes every test where the
    lowering is uncommitted — and is inert everywhere the gate actually runs.
    """
    git = _repo_with_committed_floors(tmp_path, committed)
    source = f"_EXPECTED_CLASS_COUNTS = {{'A': {working}}}\n"
    assert _floor_regression_in_repo("guard.py", source, git) == expected


@pytest.mark.parametrize(
    "current,expected",
    [
        ('_EXPECTED_CLASS_COUNTS = {"A": 2, "DC-155-Z": 3}', "lowered: ['A']"),
        # The declared escape, and it must be DECLARED — the marker is written on
        # purpose and shows in a diff, so a corrected over-count is distinguishable
        # from a silent shrink, which is the only difference that matters here.
        ('_EXPECTED_CLASS_COUNTS = {"A": 3, "DC-155-Z": 2}  '
         '# OVERCOUNT-CORRECTED-FROM-3-TO-2', None),
        # A ceiling this class never had excuses nothing.
        ('_EXPECTED_CLASS_COUNTS = {"A": 3, "DC-155-Z": 2}  '
         '# OVERCOUNT-CORRECTED-FROM-9-TO-2', "lowered: ['DC-155-Z']"),
        # THE REUSE THE FIRST VERSION ALLOWED: the same marker authorising a DIFFERENT
        # drop. Naming only the ceiling, a fall past the corrected target matched too.
        ('_EXPECTED_CLASS_COUNTS = {"A": 3, "DC-155-Z": 1}  '
         '# OVERCOUNT-CORRECTED-FROM-3-TO-2', "lowered: ['DC-155-Z']"),
        ('_EXPECTED_CLASS_COUNTS = {"A": 3, "DC-155-Z": 2}', "lowered: ['DC-155-Z']"),
    ],
)
def test_only_a_declared_overcount_correction_may_lower_a_floor(current, expected):
    """A ratchet with no escape refuses a genuine correction as firmly as a shrink."""
    previous = ['_EXPECTED_CLASS_COUNTS = {"A": 3, "DC-155-Z": 3}']
    assert _floor_regression(previous, current) == expected


def test_the_overcount_escape_is_spent_per_event_not_per_class():
    """A correction is the event `(class, from, to)`, not a licence for the class.

    NOTE THE ORDER. `previous_sources` arrives NEWEST-FIRST, because the caller feeds
    it `git log`, and the rule reverses it to read chronology. An earlier version of
    this test listed its history oldest-first, so every case exercised the reverse of
    the sequence it described and passed for the wrong reason — which is exactly the
    hazard these cases exist to catch, one level up.
    """
    M32 = "  # OVERCOUNT-CORRECTED-FROM-3-TO-2"
    M43 = "  # OVERCOUNT-CORRECTED-FROM-4-TO-3"
    M52 = "  # OVERCOUNT-CORRECTED-FROM-5-TO-2"
    at = lambda n, m="": '_EXPECTED_CLASS_COUNTS = {"DC-155-Z": %d}%s' % (n, m)
    hist = lambda *values: [at(v) for v in reversed(values)]   # oldest .. newest

    # the correction itself, with no later rise: allowed
    assert _floor_regression(hist(3, 2), at(2, M32)) is None

    # rose back to 3 afterwards: the SAME event, refused
    assert _floor_regression(hist(3, 2, 3), at(2, M32)) == "lowered: ['DC-155-Z']"

    # ...refused even with the marker deleted mid-history, because the floors record it
    assert _floor_regression(hist(3, 2, 3), at(2, M32)) == "lowered: ['DC-155-Z']"

    # a DIFFERENT correction after legitimate growth is still allowed
    assert _floor_regression(hist(3, 2, 3, 4), at(3, M43)) is None

    # GROWTH THROUGH A VALUE IS NOT A CORRECTION ENDING THERE. Corrected 5 to 1, then
    # grown 1-2-3: a later 3-to-2 is a new event. Reading any earlier-larger with any
    # later-smaller value as a correction refused it.
    assert _floor_regression(hist(5, 1, 2, 3), at(2, M52)) is None


def test_the_recorded_floors_never_move_down():
    """A floor re-derived from what it guards is not a floor.

    The table above is a ratchet: a class tally may grow, never shrink. Measured, it
    does catch a shrink — and then the habit of regenerating it from the ledger in the
    same batch as the ledger edit makes the very same shrink pass, because the floor
    is rewritten to whatever the present says. Every ledger edit in this slice ran that
    regeneration, so the ratchet was armed only against shrinks made in some other
    batch. This compares the working table against the COMMITTED one and refuses any
    entry that moved down or vanished, which is the thing a comment could only ask for.
    """
    import subprocess

    here = Path(__file__).resolve()
    root = here.parents[1]
    rel = str(here.relative_to(root))
    def git(*args, check=True):
        # BYTES, then surrogateescape — never `text=True`. A legal non-UTF-8 filename
        # under a watched tree makes `git status -z` emit raw unquoted bytes, and a
        # strict decode raises before this MANDATORY guard can return its verdict: a
        # crash where a refusal belongs. `scripts/wave_gate.py::_status` already reads
        # the same stream this way, so this matches the repository's own handling
        # rather than inventing a second one.
        out = subprocess.run(["git", *args], capture_output=True, cwd=root)
        text = out.stdout.decode("utf-8", "surrogateescape")
        if check:
            err = out.stderr.decode("utf-8", "surrogateescape").strip()
            assert out.returncode == 0, f"git {' '.join(args)} failed: {err}"
        return text

    regression = _floor_regression_in_repo(rel, here.read_text(), git)
    assert regression is None, (
        f"the recorded class floors moved down ({regression}). A tally may grow as a "
        "class recurs; a floor may not follow it downwards, and it is never "
        "regenerated from the ledger it exists to check"
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
    # ZERO, since the issue-level review round. `DC-155-A` was pre-enumerated at
    # step 0 and stood empty for two slices until the projection's family-blind
    # route branch opened it. `DC-155-F` was the last empty one — minted at step 0
    # against a hazard that had not yet occurred — and the architect gate's own
    # verdict reader, which accepted any truthy string where the prompt defines
    # exactly two, is its first instance. Every pre-enumerated class has now been
    # met by a real defect, which is worth stating: a taxonomy written in advance
    # that never fills is a taxonomy describing the wrong hazards.
    assert zero_rows == 0, (
        f"the ledger has {zero_rows} zero-count class rows, not the 0 measured when this "
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


def _closing_report_violation(ledger_text, rel, archive_dir, git):
    """The whole closing-chain check, over supplied facts — one implementation.

    Extracted because pieces of it were tested while its COMPOSITION was not: the
    caller that supplies the latest wave row could be reverted and every test stayed
    green, since the repository guard skips while no report exists. Making the
    parameter required did not fix that — a call that never runs raises nothing. So
    the composition itself is now driven, by a synthetic repository, on every run.
    """
    import re

    # THE RECORD'S OWN VIEW, once, for everything below. This function grew a
    # dozen `re.findall(..., ledger_text)` calls that each read the RAW bytes,
    # while `_unfenced_lines` had already decided — for the checkpoint tables in
    # this same file — that a fenced illustration is not part of the record.
    # Two views of one document is one view too many: fencing a real closing
    # report changed NOTHING here (measured), so an example heading counted as a
    # report and a report could hide from the guard that certifies it. Deriving
    # the view once, from the existing authority, removes the second model
    # rather than teaching it the same rule again.
    ledger_text = "\n".join(_unfenced_lines(ledger_text))

    # A REPORT IS OWED ONCE A CLOSE DECISION EXISTS, and not one moment before. The
    # rule iterated only the reports that already existed, so a slice could take a
    # closing outcome and simply never write one. Tying the requirement to the
    # DECISION rather than to the tree also dissolves the ordering knot this loop kept
    # hitting: a report must name W and N−1, which do not exist until the gates have
    # run, so it belongs in the closing commit — and that is exactly the commit that
    # records the close decision. Before the decision, nothing is owed; after it, the
    # report and the decision arrive together or the gate refuses.
    for row in _checkpoint_rows(ledger_text) or []:
        cells = row.split("|")
        if len(cells) < 5 or not re.match(r"\s*L5\b", cells[1].strip().strip("*").strip("`")):
            continue
        if "WITHDRAWN" in cells[4]:
            continue
        if "CLOSE-CLEAN" not in cells[4] and "AND-CLOSE" not in cells[4]:
            continue
        here = re.search(r"slice ([A-F])", cells[1])
        if here and here.group(1) not in re.findall(
                r"^## Slice ([A-Z]) — closing report.*$", ledger_text, re.M):
            return (f"slice {here.group(1)} records a closing decision but has written no "
                    "closing report")

    # DERIVED, not pinned. Both this marker and the wave-row pattern below read
    # `slice B` literally, and slice B is the one slice that never wrote a closing
    # report — so this guard SKIPPED on the real repository for its whole life while
    # the residue section claimed the chain was checked. The current gate is whatever
    # the latest wave checkpoint names; the current report is the one certifying that
    # gate. Deriving both from the ledger is what makes the guard follow the work.
    waves = re.findall(
        r"\| L4 composite wave gate, slice [A-Z] \|[^|]*\|\s*`([0-9a-f]{7,40})`", ledger_text
    )
    if not waves:
        return "no-wave-row"
    # EVERY report, each against the gate IT names — not one report picked by a
    # hardcoded slice letter. A gate with no report yet is a closing in progress and is
    # not a violation; a report naming a gate a LATER row for the same slice has
    # superseded is stale, and that is the clause with teeth. It fires when the record
    # is written, which is when it can be acted on, rather than while the gate runs.
    per_slice = {}
    for m in re.finditer(
        r"\| L4 composite wave gate, slice ([A-Z]) \|[^|]*\|\s*`([0-9a-f]{7,40})`", ledger_text
    ):
        per_slice.setdefault(m.group(1), []).append(m.group(2))
    heads = re.findall(r"^## Slice ([A-Z]) — closing report.*$", ledger_text, re.M)
    if not heads:
        return "absent"

    # The report for the slice whose wave gate is CURRENT must be machine-checkable.
    # One earlier report states its chain in prose alone and is exempted BY NAME, not
    # by shape: an exemption keyed on "carries no markers" is one a future report could
    # take by omitting them. Retrofitting that prose would mean transcribing a landed
    # slice's chain from a quick reading, which is how a guess enters a record.
    prose_only = {"A"}
    current_slice = ""
    for m in re.finditer(r"\| L4 composite wave gate, slice ([A-Z]) \|", ledger_text):
        current_slice = m.group(1)
    for slice_letter in heads:
        if slice_letter in prose_only and slice_letter != current_slice:
            continue
        marker_line = next(
            h for h in re.findall(r"^## Slice [A-Z] — closing report.*$", ledger_text, re.M)
            if h.startswith(f"## Slice {slice_letter} ")
        )
        body_probe = ledger_text[ledger_text.index(marker_line):]
        body_probe = body_probe[: body_probe.index("\n## ")] if "\n## " in body_probe else body_probe
        if not re.search(r"`[0-9a-f]{7,40}`\s*\(\*\*W\*\*\)", body_probe):
            return (f"slice {slice_letter}'s closing report states its chain in a form "
                    "nothing can check")
    for slice_letter in heads:
        if slice_letter in prose_only and slice_letter != current_slice:
            continue
        marker = next(h for h in re.findall(r"^## Slice [A-Z] — closing report.*$", ledger_text, re.M)
                      if h.startswith(f"## Slice {slice_letter} "))
        body = ledger_text[ledger_text.index(marker):]
        body = body[: body.index("\n## ")] if "\n## " in body else body
        rows = per_slice.get(slice_letter, [])
        latest_for_slice = rows[-1] if rows else ""
        violation = _one_closing_report_violation(
            body, marker, rel, archive_dir, git, latest_for_slice,
            slice_letter == current_slice,
        )
        if violation:
            return f"slice {slice_letter}: {violation}"
    return None


def _one_closing_report_violation(report, marker, rel, archive_dir, git, latest_wave,
                                 is_current):
    """One report against the wave gate its own slice most recently passed."""
    import re

    report = report[: report.index("\n## ")] if "\n## " in report else report

    class _Rev:
        def __call__(self, ref):
            return git("rev-parse", ref).strip()

        def precedes(self, earlier, later):
            base = git("merge-base", earlier, later, check=False).strip()
            e, l = git("rev-parse", earlier).strip(), git("rev-parse", later).strip()
            return bool(base) and base == e and e != l

    archived = {
        d.name: (d / "last-reviewed-sha").read_text().strip()
        for d in (sorted(archive_dir.iterdir()) if archive_dir.exists() else [])
        if d.is_dir() and (d / "last-reviewed-sha").exists()
    }

    violation = _closing_chain_violation(report, _Rev(), archived, latest_wave)
    if violation:
        return violation

    w = re.findall(r"`([0-9a-f]{7,40})`\s*\(\*\*W\*\*\)", report)[0]
    n1 = re.findall(r"`([0-9a-f]{7,40})`\s*\(\*\*N−1\*\*\)", report)[0]

    # DARKNESS, between the two SHAs the report NAMES. It compared the named W against
    # HEAD — a MOVING tip — so the property was false throughout any closing in
    # progress: the wave gate runs this suite, the suite runs this guard, and the guard
    # demanded a darkness the closing had not yet reached. A gate that cannot pass
    # while the work it gates is under way does not gate that work, it blocks it. Both
    # endpoints are now recorded values, so the comparison is between two fixed commits
    # and says the same thing at every instant: no source, test or script differs
    # between the tree the wave gate passed and the tree the review covered.
    changed = [
        c for c in git("diff", "--name-only", w, n1,
                       "--", "src", "tests", "scripts").splitlines() if c.strip()
    ]
    if changed:
        return f"not-dark: {changed}"

    # AND NOTHING EXECUTABLE MAY MOVE AFTER N−1. Checking only `W..N−1` certified an
    # ancestor: a correction landing after the reviewed tree left the report, the wave
    # evidence and the review evidence all describing a tree that no longer existed,
    # and this guard returned clean. A report whose tree has moved is INVALIDATED, and
    # saying so is not a circularity: a closing whose corrections are still landing has
    # no business carrying a finished report, and the protocol's own shape is that the
    # report is written once, in the closing commit, on top of the tree every gate
    # covered. While corrections are in flight the report is absent and this rule has
    # nothing to check — which is exactly the state it should be in.
    # CURRENCY APPLIES TO THE SLICE BEING CLOSED, and only to it. `W..N−1` above is
    # intrinsic and holds forever; this one is about whether the report still describes
    # the tip. For a LANDED slice source moving on afterwards is the next slice doing
    # its work, not an invalidation — asserting it there would refuse every closing
    # this issue has already made, correctly, as soon as the following slice touched a
    # file.
    if not is_current:
        return None
    # AND "CURRENT" MEANS STILL CLOSING, which the newest wave row does not decide.
    # `is_current` names the slice owning the latest wave checkpoint, and that slice
    # keeps owning it after it lands — so the moment the NEXT slice touched a source
    # file, this guard refused a closing that had already been made correctly, with
    # its report, its wave run and its review all still true of the tree they name.
    # Measured: slice E's first source edit failed this on slice D's landed report.
    #
    # The distinction is not in the prose. A slice map records a closure in the same
    # commit as the report, so reading "CLOSED" there would skip the check on exactly
    # the commit it exists for. What separates them is PUBLICATION: while a slice is
    # closing, the tree its report names is not yet on the integration branch; once it
    # lands, it is. Git answers that, so git is asked. An unresolvable `origin/dev`
    # falls through to checking, which is the fail-closed direction.
    # `--is-ancestor` answers through its EXIT CODE and prints nothing, and this
    # callback returns stdout — so it would have read as "not landed" in both
    # directions and the skip would never fire. Merge-base IDENTITY answers the same
    # question on stdout: the merge base of an ancestor with its descendant is the
    # ancestor itself. Verified by running both forms before choosing.
    base = git("merge-base", n1, "origin/dev", check=False).strip()
    if base and base == n1:
        return None
    # ASK GIT WHAT THE NEXT COMMIT WOULD CONTAIN, rather than enumerating the ways a
    # tree can differ. Three rounds of this guard each closed one mode and shipped the
    # next: commit-to-commit missed the pending commit entirely; adding the worktree
    # diff missed untracked files; adding those missed a staged file reverted only in
    # the worktree, which `git diff <n1>` and `ls-files --others` both report as
    # nothing while a plain `git commit` still includes it — verified by construction,
    # not reasoned about. The enumeration WAS the defect, so it is replaced by the
    # authority: `git status --porcelain` is git's own complete answer to what differs
    # from HEAD right now, across index, worktree, untracked, renames and deletions,
    # and it is the same instrument this repository's review recipe already uses to
    # decide whether a tree is dirty. History before HEAD is a separate question and
    # keeps its commit-to-commit diff.
    moved = set(
        c for c in git("diff", "--name-only", n1, "HEAD",
                       "--", "src", "tests", "scripts").splitlines() if c.strip()
    )
    # LET GIT DECIDE MEMBERSHIP, and never split a display-formatted path. Matching
    # prefixes myself against porcelain v1 output fabricated a watched path out of a
    # legal filename: a file under a directory named `a -> src` is printed as
    # `"docs/a -> src/bar.py"`, and splitting on the rename separator yields a
    # `src/bar.py` that does not exist — the mandatory closing guard then refuses a
    # tree in which nothing watched changed. Constructed and measured, not reasoned
    # about. The pathspec makes git answer the membership question, and `-z` makes it
    # emit NUL-delimited unquoted records, so neither the prefix test nor the rename
    # separator is mine to get wrong. Same correction as the round before: stop
    # modelling what the authority already reports.
    records = git("status", "--porcelain", "-z", "--untracked-files=all",
                  "--", "src", "tests", "scripts").split("\0")
    for record in records:
        # In `-z`, a rename emits the new path and the old path as SEPARATE records,
        # so there is no separator to split and both sides are seen on their own.
        if len(record) > 3 and record[2] == " ":
            moved.add(record[3:])
    return (f"invalidated: source moved after N−1: {sorted(moved)}" if moved else None)


def test_a_closing_report_names_the_current_wave_sha_and_proves_darkness():
    """STRUCTURAL: the closing report cannot outlive the tree it certifies.

    Recorded repeatedly as `DC-155-G`. This applies the rule to the real repository;
    the rule AND its composition are exercised on every run by the cases below. There
    is ONE implementation — an earlier version kept a second copy here, so the
    always-on cases could stay green while the real checks were deleted, which is the
    lost-fix failure the extraction was supposed to end.
    """
    import subprocess

    ledger_path = (
        Path(__file__).resolve().parents[1]
        / "docs/architecture/ISSUE_155_AUDIT_LEDGER.md"
    )
    ledger = ledger_path.read_text()
    assert "— closing report" in ledger, (
        "no closing report exists, so this guard would be vacuous; it skipped on this "
        "repository for its whole life because it looked for one slice's report by name"
    )
    root = ledger_path.parents[2]

    def git(*args, check=True):
        # BYTES, then surrogateescape — never `text=True`. A legal non-UTF-8 filename
        # under a watched tree makes `git status -z` emit raw unquoted bytes, and a
        # strict decode raises before this MANDATORY guard can return its verdict: a
        # crash where a refusal belongs. `scripts/wave_gate.py::_status` already reads
        # the same stream this way, so this matches the repository's own handling
        # rather than inventing a second one.
        out = subprocess.run(["git", *args], capture_output=True, cwd=root)
        text = out.stdout.decode("utf-8", "surrogateescape")
        if check:
            err = out.stderr.decode("utf-8", "surrogateescape").strip()
            assert out.returncode == 0, f"git {' '.join(args)} failed: {err}"
        return text

    violation = _closing_report_violation(
        ledger,
        str(ledger_path.relative_to(root)),
        ledger_path.parents[0] / "evidence" / "issue-155" / "commit-reviews",
        git,
    )
    assert violation is None, (
        f"the closing report violates the closing chain: {violation}. W and N−1 must "
        "be named in a checkable form, W must be the wave gate its own slice most "
        "recently passed, W must precede or equal N−1, N−1 must be a commit in this "
        "history, and an archived review must attest N−1"
    )


def _commit_that_added(marker, candidates, show):
    """The newest commit whose diff ADDS ``marker``.

    `git log -S` matches an occurrence-count change in EITHER direction, so a commit
    that REMOVED the marker matches as readily as one that added it — and this report
    was removed once, as premature. Taking the newest match anchored the guard to that
    removal's parent and rejected a correct boundary.

    Extracted so the direction is exercised on every run: applied to the repository it
    only runs once a report exists, which is the window in which a lost correction
    survived before.
    """
    for sha in candidates:
        if any(line.startswith("+") and marker in line
               for line in show(sha).splitlines()):
            return sha
    return ""


def _closing_boundary_sha(rel, marker, git):
    """Resolve N−1 — the commit the closing report will sit on top of.

    Extracted for the same reason as the rule below: the guard that calls it skips
    while no report exists, so both of its branches went untested exactly when a
    regression in them was cheapest to introduce. A reviewer proved that by reverting
    this to the bare history search and watching the suite stay green.
    """
    if marker not in git("show", f"HEAD:{rel}", check=False):
        # Still uncommitted: N is the commit about to be made, so N−1 is the tip.
        return git("rev-parse", "HEAD").strip()
    # `-S` counts occurrences CHANGING, so it matches a removal as readily as an
    # addition — and this report was removed once as premature. Walk the matches
    # newest-first and take the one that ADDS the marker.
    owning = _commit_that_added(
        marker,
        git("log", "--format=%H", "-S", marker, "--", rel).split(),
        lambda sha: git("show", sha, "--", rel),
    )
    assert owning, "HEAD carries the report but no commit introduces it"
    return git("rev-parse", f"{owning}~1").strip()


def _closing_chain_violation(report, rev, archived, latest_wave):
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
    if not (rev(w) == rev(latest_wave)
            or latest_wave.startswith(w) or w.startswith(latest_wave)):
        # A superseded gate still precedes the boundary and still shows no executable
        # difference, so every other clause accepts it. Dropped in a refactor that was
        # meant only to remove a duplicate implementation.
        return "stale-wave"
    # WAS `rev(n1) != rev("HEAD~1")`. That derived N−1 a SECOND way — from the commit
    # that first introduced the report — while the report names it and an archived
    # review attests it. Two models of one fact, and the derived one is wrong whenever
    # a closing is corrected across more than one commit, which is every closing this
    # slice has had. The attestation clause below is the stronger evidence and does not
    # depend on where in history the report was written; what is kept from this clause
    # is the part that is always decidable — the named boundary must be a commit in
    # this history rather than an arbitrary string.
    if not (rev.precedes(n1, "HEAD") or rev(n1) == rev("HEAD")):
        return "not-in-history"
    # W == N−1 is the CLEANEST shape, not a hole: one tree both wave-gated and
    # reviewed, each attested by its own archive. Slice C closed exactly that way, and
    # refusing it would have made this rule reject a landed, correctly-closed slice —
    # which is how a guard written for one expected shape starts refusing the others.
    # What must never happen is W AFTER N−1: a wave gate on a tree no review covered.
    if rev(w) != rev(n1) and not rev.precedes(w, n1):
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
        earlier, later = self(earlier), self(later)
        if earlier not in self._order or later not in self._order:
            return False
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
        # W == N−1: one tree wave-gated and reviewed. Legal, and slice C shipped it.
        ("bbbbbbb", "bbbbbbb", {"cdx-review.aaaaaa": "bbbbbbb"}, None),
        # a boundary that is not the commit before the tip
        # a boundary that is not a commit in this history at all
        ("aaaaaaa", "ddddddd", {"cdx-review.aaaaaa": "ddddddd"}, "not-in-history"),
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
    assert _closing_chain_violation(_report(w, n1), _FakeRev(), archived, w) == expected


@pytest.mark.parametrize(
    "cited,latest,expected",
    [
        ("aaaaaaa", "aaaaaaa", None),        # the report cites the current gate
        ("aaaaaaa", "ccccccc", "stale-wave"),  # a superseded gate, still ordered and dark
    ],
)
def test_the_rule_refuses_a_superseded_wave_sha(cited, latest, expected):
    """A stale gate still precedes the boundary and still shows no executable diff.

    Every other clause accepts it, which is why this comparison exists — and why its
    silent loss in a refactor was a regression rather than a simplification.
    """
    assert _closing_chain_violation(
        _report(cited, "bbbbbbb"), _FakeRev(), {"cdx-review.aaaaaa": "bbbbbbb"}, latest
    ) == expected


def _synthetic_history(tmp_path, marker):
    """A real repository shaped like this one: report written, removed, rewritten.

    Built with git rather than mocked, because the defect being pinned is a property
    of `git log -S` — that it matches an occurrence count changing in EITHER
    direction — and a fake would have to reproduce the very behaviour under test.
    """
    import subprocess

    root = tmp_path / "repo"
    root.mkdir()

    def git(*args, check=True):
        # BYTES, then surrogateescape — never `text=True`. A legal non-UTF-8 filename
        # under a watched tree makes `git status -z` emit raw unquoted bytes, and a
        # strict decode raises before this MANDATORY guard can return its verdict: a
        # crash where a refusal belongs. `scripts/wave_gate.py::_status` already reads
        # the same stream this way, so this matches the repository's own handling
        # rather than inventing a second one.
        out = subprocess.run(["git", *args], capture_output=True, cwd=root)
        text = out.stdout.decode("utf-8", "surrogateescape")
        if check:
            err = out.stderr.decode("utf-8", "surrogateescape").strip()
            assert out.returncode == 0, f"git {' '.join(args)} failed: {err}"
        return text

    git("init", "-q", "-b", "main")
    git("config", "user.email", "qa@example.invalid")
    git("config", "user.name", "qa")
    ledger = root / "ledger.md"
    for text, message in (
        ("base\n", "baseline"),
        (f"base\n{marker}\npremature\n", "a closing report written too early"),
        ("base\n", "remove the premature report"),
    ):
        ledger.write_text(text)
        git("add", "-A")
        git("commit", "-qm", message)
    return git, ledger


def test_the_boundary_is_the_tip_while_the_report_is_uncommitted(tmp_path):
    """The pre-N state: HEAD lacks the report, the worktree carries it.

    This is the exact shape the anchor got wrong — the history search found the
    REMOVAL and resolved N−1 to its parent, rejecting a correct boundary. Driven end
    to end, so reverting the branch fails here rather than passing quietly.
    """
    marker = "## Slice B — closing report"
    git, ledger = _synthetic_history(tmp_path, marker)
    ledger.write_text(f"base\n{marker}\nfinal\n")  # uncommitted, as before commit N

    assert _closing_boundary_sha("ledger.md", marker, git) == git("rev-parse", "HEAD").strip()


def test_the_boundary_is_the_adding_commits_parent_once_the_report_lands(tmp_path):
    """And after N is made, the anchor is the commit that ADDED the report.

    Not the one that removed an earlier copy — which is the parent one commit further
    back, and is what the search returned before.
    """
    marker = "## Slice B — closing report"
    git, ledger = _synthetic_history(tmp_path, marker)
    removal = git("rev-parse", "HEAD").strip()
    ledger.write_text(f"base\n{marker}\nfinal\n")
    git("add", "-A")
    git("commit", "-qm", "the closing report")

    assert _closing_boundary_sha("ledger.md", marker, git) == removal


def _synthetic_closing_repo(tmp_path, wave_shas_from, cited, executable_drift=False,
                            commit_report=False, pending_drift=None):
    """A real repository carrying a closing report, built to be driven end to end.

    One commit is the wave gate's tree (W), a later one is N−1, and the report either
    sits UNCOMMITTED — the state commit N is made from — or is committed on top, which
    is the state the guard runs in for every commit AFTER N. Both are production
    states and both go through the whole guard: testing only the first left the
    boundary call replaceable by a bare `rev-parse HEAD` with the suite still green,
    and that regression would surface as the guard rejecting a valid report the moment
    N landed. ``commit_report`` also plants the historical premature report and its
    removal, so the committed path exercises the direction rule as it really is.
    """
    import subprocess

    root = tmp_path / "repo"
    root.mkdir()

    def git(*args, check=True):
        # BYTES, then surrogateescape — never `text=True`. A legal non-UTF-8 filename
        # under a watched tree makes `git status -z` emit raw unquoted bytes, and a
        # strict decode raises before this MANDATORY guard can return its verdict: a
        # crash where a refusal belongs. `scripts/wave_gate.py::_status` already reads
        # the same stream this way, so this matches the repository's own handling
        # rather than inventing a second one.
        out = subprocess.run(["git", *args], capture_output=True, cwd=root)
        text = out.stdout.decode("utf-8", "surrogateescape")
        if check:
            err = out.stderr.decode("utf-8", "surrogateescape").strip()
            assert out.returncode == 0, f"git {' '.join(args)} failed: {err}"
        return text

    git("init", "-q", "-b", "main")
    git("config", "user.email", "qa@example.invalid")
    git("config", "user.name", "qa")
    ledger = root / "ledger.md"
    marker = "## Slice B — closing report"

    def commit(text, message):
        ledger.write_text(text)
        git("add", "-A")
        git("commit", "-qm", message)
        return git("rev-parse", "HEAD").strip()[:7]

    wave = commit("the wave gate's tree\n", "the wave gate's tree")
    if commit_report:
        commit(f"x\n{marker}\npremature\n", "a closing report written too early")
        commit("x\n", "remove the premature report")
    if executable_drift:
        # code moving after the gate that certified it — the thing the darkness proof
        # exists to catch, and the reason N is confined to prose.
        (root / "src").mkdir(exist_ok=True)
        (root / "src" / "drift.py").write_text("x = 1\n")
    boundary = commit("the record edits\n", "the record edits")

    archive = root / "archive"
    (archive / "cdx-review.aaaaaa").mkdir(parents=True)
    (archive / "cdx-review.aaaaaa" / "last-reviewed-sha").write_text(
        git("rev-parse", "HEAD").strip()
    )

    shas = [wave, boundary]
    rows = "\n".join(
        f"| L4 composite wave gate, slice B | {i + 1} / {i + 1} | `{shas[j]}`, clean | ok |"
        for i, j in enumerate(wave_shas_from)
    )
    ledger.write_text(
        f"{rows}\n\n{marker}\n\n"
        f"| `{shas[cited]}` (**W**) | wave |\n"
        f"| `{boundary}` (**N−1**) | review `cdx-review.aaaaaa` |\n"
    )
    if commit_report:
        git("add", "-A")
        git("commit", "-qm", "the closing report")
    if pending_drift:
        # Executable change left in the PENDING commit, which is the state N is
        # written from. `staged` and `untracked` are distinct blind spots.
        (root / "src").mkdir(exist_ok=True)
        (root / "src" / f"{pending_drift}.py").write_text("y = 2\n")
        if pending_drift.startswith("staged"):
            git("add", "src")
        if pending_drift == "undecodable_name":
            # A legal filename that is not valid UTF-8. `git status -z` emits it raw
            # and unquoted, so a strict decode raises where a refusal belongs.
            (root / "src" / f"{pending_drift}.py").unlink()
            try:
                os.write(
                    os.open(os.path.join(bytes(root), b"src", b"bad\xff.py"),
                            os.O_WRONLY | os.O_CREAT, 0o644),
                    b"w = 4\n",
                )
            except OSError:  # pragma: no cover - filesystem-dependent
                # APFS and HFS+ reject a name that is not valid UTF-8, so this case
                # cannot be built here at all. The decode path it exercises is covered
                # unconditionally by the blob test below, which needs no such file.
                pytest.skip("this filesystem rejects non-UTF-8 filenames")
        if pending_drift == "adversarial_name":
            # A legal filename whose DISPLAY form contains the rename separator: the
            # prefix-matching parser fabricated a `src/` path from it and refused a
            # tree in which nothing watched had changed.
            (root / "src" / f"{pending_drift}.py").unlink()
            adversarial = root / "docs" / "a -> src"
            adversarial.mkdir(parents=True, exist_ok=True)
            (adversarial / "bar.py").write_text("z = 3\n")
        if pending_drift == "staged_then_removed":
            # Staged, then reverted in the worktree only: invisible to a diff against
            # the commit AND to the untracked listing, yet a plain commit includes it.
            (root / "src" / f"{pending_drift}.py").unlink()
    return git, archive


def test_the_repository_guard_accepts_a_report_already_committed(tmp_path):
    """The state the guard is in for every run AFTER commit N lands.

    Until this existed the production boundary call could be replaced by a bare
    `rev-parse HEAD` with every test still green — and the failure it hides only
    appears once N is made, when the guard would compare the named N−1 against N and
    reject a correct report. The history here also carries the premature report and
    its removal, so this drives the direction rule through the real caller too.
    """
    git, archive = _synthetic_closing_repo(
        tmp_path, wave_shas_from=[0], cited=0, commit_report=True
    )
    assert _closing_report_violation(
        (tmp_path / "repo" / "ledger.md").read_text(), "ledger.md", archive, git
    ) is None


def test_the_repository_guard_accepts_the_one_correct_closing_shape(tmp_path):
    """The whole composition, driven — not the rule in isolation.

    The real guard stands down while no report is written, so everything it wires
    together went unexercised in the window where breaking it was cheapest.
    """
    git, archive = _synthetic_closing_repo(tmp_path, wave_shas_from=[0], cited=0)
    assert _closing_report_violation(
        (tmp_path / "repo" / "ledger.md").read_text(), "ledger.md", archive, git
    ) is None


def test_the_repository_guard_refuses_a_report_citing_a_superseded_wave(tmp_path):
    """And it refuses a stale gate through the REAL caller.

    This is the case that dropping `waves[-1]` from the production call had to fail
    and did not: the parameter was required, but a call the guard never reaches
    raises nothing. Here the caller runs on every test run.
    """
    git, archive = _synthetic_closing_repo(tmp_path, wave_shas_from=[0, 1], cited=0)
    assert _closing_report_violation(
        (tmp_path / "repo" / "ledger.md").read_text(), "ledger.md", archive, git
    ).endswith("stale-wave")


def test_the_git_callback_survives_undecodable_output(tmp_path):
    """The decode path, on every platform, without needing an undecodable FILENAME.

    The filename case can only be built where the filesystem allows one, which APFS
    does not — so the guard's real hazard would have been covered on CI alone. A blob
    carrying the same bytes exercises the identical callback: with `text=True` this
    raises `UnicodeDecodeError`, and a mandatory guard that raises where it should
    refuse fails closed in the wrong direction — it takes the whole suite down instead
    of reporting drift.
    """
    import subprocess

    git, _ = _synthetic_closing_repo(tmp_path, wave_shas_from=[0], cited=0)
    root = tmp_path / "repo"
    sha = subprocess.run(
        ["git", "hash-object", "-w", "--stdin"],
        input=b"raw \xff bytes\n", capture_output=True, cwd=root,
    ).stdout.decode().strip()
    out = git("cat-file", "blob", sha)
    assert "\udcff" in out, (
        "undecodable output must survive as surrogates rather than raising: " + repr(out)
    )


def test_the_repository_guard_does_not_fabricate_a_watched_path(tmp_path):
    """A legal filename whose display form contains the rename separator.

    The complement of the cases above: this guard is MANDATORY, so a false positive
    refuses a correct closing outright. Prefix-matching against porcelain v1 read
    `"docs/a -> src/bar.py"` as two paths and invented a `src/bar.py` nothing created.
    """
    git, archive = _synthetic_closing_repo(
        tmp_path, wave_shas_from=[0], cited=0, pending_drift="adversarial_name"
    )
    assert _closing_report_violation(
        (tmp_path / "repo" / "ledger.md").read_text(), "ledger.md", archive, git
    ) is None


@pytest.mark.parametrize(
    "pending", ["staged", "untracked", "staged_then_removed", "undecodable_name"]
)
def test_the_repository_guard_refuses_executable_change_left_in_the_pending_commit(
    tmp_path, pending
):
    """The state commit N is written from, where the check was trivially satisfied.

    With the report uncommitted, N−1 IS the tip, so comparing the two commits compared
    HEAD against HEAD and passed however much executable change the pending commit
    carried. Both blind spots are driven: a staged file `git diff` against a commit
    still misses, and an untracked file `git diff` never sees at all.
    """
    git, archive = _synthetic_closing_repo(
        tmp_path, wave_shas_from=[0], cited=0, pending_drift=pending
    )
    violation = _closing_report_violation(
        (tmp_path / "repo" / "ledger.md").read_text(), "ledger.md", archive, git
    )
    assert violation is not None and "invalidated" in violation, violation
    # The undecodable case cannot be named by its own filename; what matters is that
    # the guard REFUSES instead of raising, so it is asserted on the refusal alone.
    if pending != "undecodable_name":
        assert f"src/{pending}.py" in violation, violation


def test_the_repository_guard_refuses_a_report_whose_tree_moved_after_the_gate(tmp_path):
    """The darkness clause, with a case that can actually fail it.

    Found while grading the fix above rather than reported: neutering this clause left
    every test green, which is the same defect the two reviewed findings describe —
    a check with no witness — one level further down.
    """
    git, archive = _synthetic_closing_repo(
        tmp_path, wave_shas_from=[0], cited=0, executable_drift=True
    )
    violation = _closing_report_violation(
        (tmp_path / "repo" / "ledger.md").read_text(), "ledger.md", archive, git
    )
    assert violation is not None and "not-dark" in violation, violation
    assert "src/drift.py" in violation


#: The checkpoint table's own header. A wave row is recognised by MEMBERSHIP in this
#: table, so a fixture handing over a bare row is not modelling the record — it is
#: modelling a document the rule correctly declines to read.
_CHECKPOINT_HEADER = (
    "| Loop | Evaluation (window / cumulative) | SHA (+dirty) | Outcome | Rationale |\n"
    "| --- | --- | --- | --- | --- |\n"
)


def _wave_ledger(*rows):
    """A ledger holding `rows` inside a well-formed checkpoint table."""
    return _CHECKPOINT_HEADER + "\n".join(rows) + "\n"


#: THE SCOPE HALF OF A LOOP IDENTITY, read the same way on both sides.
#:
#: This billed only `slice ([A-F])`, so every issue-level loop was invisible to a
#: rule written to police loop identity: its evaluations were never counted, no
#: interval was ever owed, and deleting an issue-level checkpoint row left the
#: audit green. The workflow's loop identity is "gate purpose/authority plus
#: slice-or-wave scope" — the issue-level arc is a scope, not an absence of one.
#:
#: ONE extractor for the archive side and the ledger side. Billing a scope the row
#: parser could not read would make its checkpoints unrecordable — a rule nothing
#: can satisfy — so the two sides cannot be allowed to drift apart.
def _loop_scope(text):
    import re

    if re.search(r"issue-level", text, re.I):
        return "issue-level"
    m = re.search(r"slice ([A-F])", text)
    return m.group(1) if m else None


def _archived_summary(directory, payload, verdict="pass",
                      logical_loop="L4 (composite wave gate, slice B)"):
    """Write `summary.json` the way the archiver does — BOUND by its round record.

    The fixtures wrote a bare `summary.json` into a directory, which is precisely the
    shape the binding rule now refuses: a summary nothing produced. Writing the record
    here keeps every case exercising the arm it means to exercise instead of tripping
    the binding check first, and keeps the fixture honest about what an archive is.
    """
    import hashlib
    import json

    directory.mkdir(parents=True, exist_ok=True)
    body = json.dumps(payload)
    (directory / "summary.json").write_text(body)
    (directory / "round.json").write_text(json.dumps({
        "collector": "wave_gate",
        "durable_dir": f"wave-gate/{directory.name}",
        "files": {
            f"wave-gate/{directory.name}/summary.json":
                hashlib.sha256(body.encode()).hexdigest(),
        },
        "status": "completed",
        "verdict": verdict,
        # The archiver records the tree the run was made on, from its own run
        # directory. Taking it from the payload keeps the fixture's round and its
        # summary describing ONE run, which is what the identity check reads.
        "wave_sha": payload.get("wave_sha"),
        # The loop this round belonged to. Scope is half of a loop identity, so a
        # fixture that omits it is not modelling an archived round.
        "logical_loop": logical_loop,
    }))


def _wave_evidence_violation(ledger_text, archive_dir):
    """Whether the LATEST wave checkpoint can be reverified from the repository.

    The procedural fix for decisions-ahead-of-evidence was to write the checkpoint
    after the validation. It held for ordering and then failed on a different axis: a
    wave row named a passing SHA whose run output was never archived at all, so its
    quoted counts existed only in the row that quoted them. Ordering was a rule about
    WHEN; this is a rule about WHETHER, and it is executable rather than remembered.
    """
    # The record's own view — see `_unfenced_lines`. A fenced block is an
    # illustration, not a row.
    ledger_text = "\n".join(_unfenced_lines(ledger_text))
    import json
    import re

    # NO SCOPE ENUMERATION, AND A BOUNDED SEARCH — the two halves of one rule, and
    # each was got wrong on its own first. The pattern pinned `slice B`; then it
    # listed `slice [A-Z]|issue-level`, which still dropped this ledger's
    # `ISSUE-level correction arc` spelling and let `rows[-1]` re-verify an older row
    # while the CURRENT gate went unchecked. Dropping the whitelist fixed that and
    # broke the other side: unbounded, the pattern also matched a PROSE final-tree
    # row further down the record, so the latest row was again an old one. A
    # whitelist of scope spellings is a hand-model of a field whose authority is the
    # row; the boundary that IS authoritative is membership in the checkpoint table,
    # which `_checkpoint_rows` decides structurally. So: every L4 wave row in that
    # table, whatever its scope reads, and the LAST of them is the one closure rests
    # on.
    rows = [r for r in (_checkpoint_rows(ledger_text) or [])
            if re.match(r"^\| L4 composite wave gate[ ,|]", r)]
    if not rows:
        return "no wave row"
    latest = rows[-1]
    sha = re.search(r"W = `([0-9a-f]{7,40})`", latest)
    if not sha:
        return "the latest wave row names no SHA"
    named = re.search(r"`wave-gate/([A-Za-z0-9._-]+)`", latest)
    if not named:
        return f"the latest wave row ({sha.group(1)}) cites no archived evidence"
    summary = archive_dir / named.group(1) / "summary.json"
    if not summary.is_file():
        return f"cited archive {named.group(1)} is absent from the checkout"
    attested = json.loads(summary.read_text())
    recorded = attested.get("wave_sha", "")
    if not recorded.startswith(sha.group(1)):
        return (f"cited archive {named.group(1)} attests {recorded[:7]}, "
                f"not the {sha.group(1)} the row names")
    # THE SUMMARY MUST BE AN OUTPUT OF THE ROUND, not a file beside it. Everything
    # below trusts `summary.json`, and nothing checked that the round record had ever
    # seen it: a summary hand-written into an archived directory after the fact
    # satisfied every value test here while `round.json` listed only `wave.log` and
    # derived a null verdict — the archive attesting one thing and the guard reading
    # another, in the same directory. The archiver hashes each copied file into
    # `files` and derives `verdict` from the summary (`scripts/archive_gate_round.py`),
    # so a bound summary is exactly one the round produced.
    import hashlib
    record_path = archive_dir / named.group(1) / "round.json"
    if not record_path.is_file():
        return f"cited archive {named.group(1)} carries no round record"
    try:
        record = json.loads(record_path.read_text())
    except ValueError:
        return f"cited archive {named.group(1)} has an unreadable round record"
    # THE EXACT KEY, not a basename match: the archiver writes each file under its
    # full archive-relative path, so matching the tail alone would accept a DIFFERENT
    # round's summary — listed under that round's directory — whenever the digests
    # agree.
    key = f"wave-gate/{named.group(1)}/summary.json"
    listed = (record.get("files") or {}).get(key)
    if listed is None:
        return (f"cited archive {named.group(1)} does not bind its summary — the "
                f"round record lists no {key}")
    if listed != hashlib.sha256(summary.read_bytes()).hexdigest():
        return (f"cited archive {named.group(1)} binds a summary.json whose digest "
                f"does not match the file in the checkout")
    # THE WHOLE IDENTITY, because any one field can be made to agree while the round
    # is a different one. `--status` is a CALLER override on the archiver, so a
    # refused run can be archived beside a summary that still reads `pass`; and a
    # record naming another directory or another tree is not this row's evidence at
    # all, however cleanly it passes on its own terms.
    for field, want, saying in (("verdict", "pass", "the round verdict"),
                                ("status", "completed", "the round status")):
        if record.get(field) != want:
            return (f"cited archive {named.group(1)} records {saying} "
                    f"{record.get(field)!r}, not {want!r}")
    # REQUIRED, not tolerated. Every one of the archived wave rounds carries this
    # field, so accepting its absence bought compatibility with nothing and left a
    # malformed record indistinguishable from a well-formed one.
    if record.get("durable_dir") != key.rsplit("/", 1)[0]:
        return (f"cited archive {named.group(1)} carries a round record for "
                f"{record.get('durable_dir')!r}")
    # AND THE SAME LOOP. Scope is half of a loop identity, so a round archived under
    # one scope is not evidence for a checkpoint recorded under another — an
    # issue-level closure backed by a slice's wave run reads as current and is not.
    # Both sides go through the SAME extractor, so neither can drift into a
    # vocabulary the other cannot read.
    # THE LOOP CELL, not the whole row. Reading the line let a row's RATIONALE decide
    # its scope: the slice-F checkpoint mentions the issue-level gate in its prose, so
    # the whole-line read returned `issue-level` for a slice-F row and would reject
    # that row's own valid evidence. Scope is a field; a field is read from its cell.
    # A NON-STRING LOOP IDENTITY IS MALFORMED EVIDENCE. Stringifying first meant a
    # record whose identity was an object still matched — `{"scope": "slice B"}`
    # renders a string containing `slice B` — so a malformed archive validated against
    # a well-formed checkpoint. This function is validating a producer's output, not
    # trusting it.
    raw_loop = record.get("logical_loop")
    if not isinstance(raw_loop, str):
        return (f"cited archive {named.group(1)} records a loop identity that is not "
                f"a string: {type(raw_loop).__name__}")
    row_scope = _loop_scope(latest.split("|")[1])
    round_scope = _loop_scope(raw_loop)
    # AN UNRECOGNISED SCOPE IS A VIOLATION, not a value to compare. Two scopes this
    # extractor cannot read both came back as "no scope" and compared EQUAL, so a
    # checkpoint spelled one way could be satisfied by a round spelled another
    # entirely — a fail-open hiding inside an equality test.
    for who, scope, text in (("checkpoint row", row_scope, latest.split("|")[1]),
                             ("cited archive", round_scope, raw_loop)):
        if scope is None:
            return (f"the {who} names a loop scope this rule cannot read: "
                    f"{text.strip()[:60]!r}")
    if row_scope != round_scope:
        return (f"cited archive {named.group(1)} records a {round_scope!r} round for "
                f"a {row_scope!r} checkpoint")
    if str(record.get("wave_sha") or "") != str(attested.get("wave_sha") or ""):
        return (f"cited archive {named.group(1)} records a round on "
                f"{str(record.get('wave_sha'))[:7]!r} beside a summary for "
                f"{str(attested.get('wave_sha'))[:7]!r}")
    # The archive must SUPPORT the row, not merely share its SHA. Checking identity
    # alone accepted an archive recording a failed verdict, a nonzero exit code, or
    # counts contradicting the arms the row quotes — so the guard proved that evidence
    # existed and nothing about what it said.
    if attested.get("status") != "completed" or attested.get("verdict") != "pass":
        return (f"cited archive {named.group(1)} records "
                f"{attested.get('status')}/{attested.get('verdict')}, not a passing run")
    exit_code = attested.get("exit_code")
    # `False == 0` in Python, so a JSON boolean passed an integer comparison. A process
    # exit code is an integer; a boolean in that field is malformed evidence, not a zero.
    if isinstance(exit_code, bool) or exit_code != 0:
        return f"cited archive {named.group(1)} records exit_code {exit_code!r}"

    # DERIVED from what the archive attests, not from a list of arm names. Enumerating
    # the arms checked pass/skip/goldens and silently ignored `deterministic`,
    # `byte_exact` and the fingerprint count — the same hand-listed-enumeration defect
    # this guard exists to catch, inside the guard, for the fourth time in this
    # artifact. Every boolean a passing run attests must be true, and every number the
    # row quotes beside a key the archive records must equal it.
    leaves = {}

    def _walk(node, path):
        if isinstance(node, dict):
            for key, value in node.items():
                _walk(value, path + [key])
        else:
            leaves[".".join(path)] = node

    _walk(attested, [])

    # WHAT A WAVE SUMMARY MUST CARRY TO BE EVIDENCE AT ALL. The walk above derives
    # everything from what the archive attests, which is right for checking the
    # values and useless for checking that any exist: an archive holding only a
    # status, a SHA and an exit code produced no leaves, satisfied every loop
    # below, and was accepted. That is a guard that passes hardest when the
    # evidence is thinnest.
    #
    # A CLOSED CONTRACT, pinned in both directions by
    # `test_the_wave_contract_matches_every_archived_summary` below: this
    # declaration is refused if a committed summary stops carrying one of these,
    # and a summary is refused if it omits one. So the list cannot quietly drift
    # from what the gate actually archives, which is the failure mode that would
    # make declaring it worse than deriving it.
    for required, kind in _REQUIRED_WAVE_EVIDENCE.items():
        if required not in leaves:
            return (f"cited archive {named.group(1)} carries no {required}, so it "
                    f"attests nothing about that arm")
        value = leaves[required]
        if kind == "count":
            # A STRING COUNT IS NOT A COUNT. The type test below skips non-integers,
            # so `"passed": "11430"` sailed through every value check — and the
            # derivation that removed a hand-listed enumeration one round earlier
            # is what removed the type constraint with it.
            if isinstance(value, bool) or not isinstance(value, int):
                return (f"cited archive {named.group(1)} records {required} as "
                        f"{value!r}, which is not a count")
        elif not isinstance(value, bool):
            return (f"cited archive {named.group(1)} records {required} as "
                    f"{value!r}, which is not a yes-or-no")
    #: Counts a PASSING wave may legitimately record as zero. Everything else it
    #: counts is work it claims to have done, and zero of that verifies nothing —
    #: including an arm the row quotes in WORDS, which no digit comparison can see.
    may_be_zero = ("exit_code", "skipped", "skip_cap")
    for path, value in sorted(leaves.items()):
        if isinstance(value, bool) and not value:
            return f"cited archive {named.group(1)} attests {path} is false"
        if (isinstance(value, int) and not isinstance(value, bool)
                and value == 0 and path.rsplit(".", 1)[-1] not in may_be_zero):
            return f"cited archive {named.group(1)} attests {path} is zero"
    # ONE SPELLING FOR BOTH SIDES. The row writes prose — "plan-fingerprint
    # cases", "74 goldens" — while the archive writes identifiers. Comparing them
    # raw meant neither pattern located the count for `plan_fingerprint_cases`
    # (the row hyphenates) or for `goldens.active` (the row names the parent, not
    # the leaf), so an archive recording three cases satisfied a row claiming two:
    # not because the values agreed, but because the search found nothing and
    # nothing was read as agreement.
    flat = re.sub(r"[\s_-]+", " ", latest).lower()

    def _quoted(path):
        """The count the row quotes for this metric, or None if it quotes none.

        Tries the leaf first and the parent second — a row says "74 goldens"
        where the archive says `goldens.active`, and both name the same arm.
        """
        parts = path.replace("_", " ").split(".")
        leaf = parts[-1].lower()
        # THE ROW'S WORDING IS NOT THE IDENTIFIER'S. A row writes "the
        # plan-fingerprint seam across 2 cases" for `plan_fingerprint_cases`, so
        # the words the identifier joins are separated in the prose and the full
        # term matches nothing. The trailing noun is what the count actually sits
        # in front of, so it is tried too — after the full term, so a row that
        # does spell the whole name is still read by its most specific form.
        terms = [leaf]
        if " " in leaf:
            terms.append(leaf.rsplit(" ", 1)[-1])
        if len(parts) > 1:
            terms.append(parts[-2].lower())
        # STRICT FORM FIRST, ACROSS BOTH TERMS, before the loose form is tried at
        # all. Interleaving them let the loose pattern on the leaf win over the
        # strict pattern on the parent: for "74 active goldens", the leaf `active`
        # matched loosely against a number further down the row while the parent
        # `goldens` had the count sitting right in front of it. A looser reading of
        # a worse term is not a better answer.
        for pattern in (r"(\d[\d,]*)\s+(?:active\s+)?{0}", r"{0}\D{{0,40}}?(\d[\d,]*)"):
            for term in terms:
                hit = re.search(pattern.format(re.escape(term)), flat)
                if hit:
                    return int(hit.group(1).replace(",", ""))
        return None

    for path, value in sorted(leaves.items()):
        if isinstance(value, bool) or not isinstance(value, int):
            continue
        if path.rsplit(".", 1)[-1] in ("exit_code", "skip_cap"):
            continue
        quoted = _quoted(path)
        if quoted is not None and quoted != value:
            return (f"the row quotes {quoted} for {path} but "
                    f"{named.group(1)} attests {value}")
        # A REQUIRED ARM MUST BE QUOTED. For the declared set, "the row says
        # nothing about it" is the fail-open this whole comparison exists to
        # close — the row is what a reader believes, and an arm it never states
        # is an arm nobody checked.
        if quoted is None and path in _REQUIRED_WAVE_EVIDENCE:
            return (f"the row quotes no figure for {path}, which "
                    f"{named.group(1)} attests as {value}")
    return None


#: The arms a composite wave gate runs, and therefore the figures a summary must
#: carry for the row citing it to mean anything. `count` must be a real integer;
#: the rest must be booleans a passing run sets true.
_REQUIRED_WAVE_EVIDENCE = {
    "suite.passed": "count",
    "suite.skipped": "count",
    "goldens.active": "count",
    "goldens.deterministic": "flag",
    "goldens.byte_exact": "flag",
    "plan_fingerprint_cases": "count",
}


_PASSING_WAVE = {
    "wave_sha": "abc1234def", "status": "completed", "verdict": "pass", "exit_code": 0,
    "suite": {"passed": 11037, "skipped": 18, "skip_cap": 30},
    "goldens": {"active": 74, "deterministic": True, "byte_exact": True},
    "plan_fingerprint_cases": 2,
}
_ROW = ("| L4 composite wave gate, slice B | 1 / 1 | x | `CLOSE-CLEAN` | W = `{sha}`{cite}. Arms: the non-KB "
        "suite green at 11,037 passed and 18 skipped; 74 active goldens rendered twice and byte-exact; the "
        "plan-fingerprint seam across 2 cases |")


@pytest.mark.parametrize(
    "sha,cite,attested,expected",
    [
        ("abc1234", ", archived `wave-gate/ok`", {}, None),
        ("abc1234", "", {}, "the latest wave row (abc1234) cites no archived evidence"),
        ("abc1234", ", archived `wave-gate/gone`", {},
         "cited archive gone is absent from the checkout"),
        ("dddffff", ", archived `wave-gate/ok`", {},
         "cited archive ok attests abc1234, not the dddffff the row names"),
        # Identity alone was the whole check, and it accepted every one of these.
        ("abc1234", ", archived `wave-gate/ok`", {"verdict": "fail"},
         "cited archive ok records completed/fail, not a passing run"),
        ("abc1234", ", archived `wave-gate/ok`", {"status": "timeout"},
         "cited archive ok records timeout/pass, not a passing run"),
        ("abc1234", ", archived `wave-gate/ok`", {"exit_code": 1},
         "cited archive ok records exit_code 1"),
        ("abc1234", ", archived `wave-gate/ok`",
         {"suite": {"passed": 1, "skipped": 18, "skip_cap": 30}},
         "the row quotes 11037 for suite.passed but ok attests 1"),
        ("abc1234", ", archived `wave-gate/ok`",
         {"goldens": {"active": 9, "deterministic": True, "byte_exact": True}},
         "the row quotes 74 for goldens.active but ok attests 9"),
        # Never named by the hand-written arms tuple, which is why it was enumerated
        # away: the derived rule reaches them without being told they exist.
        ("abc1234", ", archived `wave-gate/ok`",
         {"goldens": {"active": 74, "deterministic": False, "byte_exact": True}},
         "cited archive ok attests goldens.deterministic is false"),
        ("abc1234", ", archived `wave-gate/ok`",
         {"goldens": {"active": 74, "deterministic": True, "byte_exact": False}},
         "cited archive ok attests goldens.byte_exact is false"),
        # An arm the row quotes in WORDS, which no digit comparison can see.
        ("abc1234", ", archived `wave-gate/ok`", {"plan_fingerprint_cases": 0},
         "cited archive ok attests plan_fingerprint_cases is zero"),
        # `False == 0` in Python, so a JSON boolean passed as a process exit code.
        ("abc1234", ", archived `wave-gate/ok`", {"exit_code": False},
         "cited archive ok records exit_code False"),
        ("abc1234", ", archived `wave-gate/ok`", {"status": "failed"},
         "cited archive ok records failed/pass, not a passing run"),
    ],
)
def test_the_wave_evidence_rule_admits_only_a_supported_row(tmp_path, sha, cite, attested, expected):
    """The rule itself, always exercised — not only against this repository.

    An archive must SUPPORT the row, not merely share its SHA: checking identity alone
    accepted a failed verdict, a timed-out run, a nonzero exit and counts contradicting
    the arms the row quotes.
    """
    import json

    archive = tmp_path / "wave-gate"
    _archived_summary(archive / "ok", {**_PASSING_WAVE, **attested})
    assert _wave_evidence_violation(_wave_ledger(_ROW.format(sha=sha, cite=cite)), archive) == expected


def _coverage_violations(ledger_text, archive_dir):
    """Whether every "which gate covers which tree" claim is true of the archive.

    The third axis of the closure-ahead-of-validation class. The first fix was
    procedural (write the checkpoint after the validation) and held for ORDERING; the
    second was executable (the wave row must cite an archive attesting the same SHA)
    and held for REVERIFIABILITY. Neither could catch this one: a closing report named
    a reviewed tree and cited a review round that had reviewed an ANCESTOR of it, so
    the citation was archived, attested and honest, and still did not cover the tree
    being closed. Ordering is about WHEN, reverifiability about WHETHER, and this is
    about WHAT — the reviewed SHA must be the tree the report closes on.
    """
    # The record's own view — see `_unfenced_lines`. A fenced block is an
    # illustration, not a row.
    ledger_text = "\n".join(_unfenced_lines(ledger_text))
    import re

    out = []
    rows = re.findall(r"^\|\s*\*\*N.1\*\*[^\n]*$", ledger_text, re.M)
    if not rows:
        return ["no reviewed-tree row"]
    for row in rows:
        # The commit cell may carry the chain marker beside the SHA, so read the first
        # backticked SHA in the row rather than requiring it to BE the cell.
        sha = re.search(r"\|\s*`([0-9a-f]{7,40})`", row)
        run = re.search(r"run `(cdx-review\.[A-Za-z0-9]+)`", row)
        if not sha:
            out.append("a reviewed-tree row names no SHA")
            continue
        if not run:
            out.append(f"the reviewed-tree row {sha.group(1)} cites no review run")
            continue
        # `last-reviewed-sha` is written by the COLLECTOR and only on a completed
        # round, so its presence is the round's own attestation of what it reviewed.
        # A failed round deliberately leaves it absent, which is why reading it is a
        # coverage check and not merely a name comparison.
        attested = archive_dir / run.group(1) / "last-reviewed-sha"
        if not attested.is_file():
            out.append(f"cited review {run.group(1)} has no attested reviewed SHA")
            continue
        recorded = attested.read_text().strip()
        if not recorded.startswith(sha.group(1)):
            out.append(
                f"cited review {run.group(1)} reviewed {recorded[:7]}, not the "
                f"{sha.group(1)} the report closes on"
            )
    return out


_COVERAGE_ROW = (
    "| **N\u22121** \u2014 the reviewed tree | `{sha}` (**N\u22121**) | `L5` closing review{cite}, "
    "archived, covering the complete delta |"
)


@pytest.mark.parametrize(
    "sha,cite,attested,expected",
    [
        ("abc1234", ", run `cdx-review.ok`", "abc1234def", []),
        # The defect this exists for: an honest, archived, attested citation of a
        # review that looked at an EARLIER tree than the one being closed.
        ("abc1234", ", run `cdx-review.ok`", "9999999aaa",
         ["cited review cdx-review.ok reviewed 9999999, not the abc1234 "
          "the report closes on"]),
        # A failed round never gets a reviewed SHA, so citing one cannot pass.
        ("abc1234", ", run `cdx-review.ok`", None,
         ["cited review cdx-review.ok has no attested reviewed SHA"]),
        ("abc1234", "", "abc1234def",
         ["the reviewed-tree row abc1234 cites no review run"]),
    ],
)
def test_the_coverage_rule_admits_only_a_review_of_the_closing_tree(
    tmp_path, sha, cite, attested, expected
):
    """The rule itself, exercised away from this repository's own record."""
    archive = tmp_path / "commit-reviews"
    (archive / "cdx-review.ok").mkdir(parents=True)
    if attested is not None:
        (archive / "cdx-review.ok" / "last-reviewed-sha").write_text(attested + "\n")
    row = _COVERAGE_ROW.format(sha=sha, cite=cite)
    assert _coverage_violations(row, archive) == expected


def test_every_closing_report_reviewed_a_tree_the_archive_attests():
    """Applied to this ledger, and to EVERY such row rather than the latest.

    The wave rule is scoped to its latest row because twenty-two earlier runs were
    never archived and that history cannot be supplied. No such excuse applies here:
    every reviewed-tree claim this ledger makes cites a collected round, so all of
    them are checkable and all of them are checked.
    """
    root = Path(__file__).resolve().parents[1]
    ledger = (root / "docs/architecture/ISSUE_155_AUDIT_LEDGER.md").read_text()
    archive = root / "docs/architecture/evidence/issue-155/commit-reviews"
    violations = _coverage_violations(ledger, archive)
    assert not violations, f"a closing report closes on an unreviewed tree: {violations}"


def _unscannable_finding_ids(ledger_text):
    """Row identifiers that LOOK like findings and that the scanner cannot parse.

    Second recorded instance of one mechanism: an identifier written in a shape its
    own consumer does not accept. The first was twelve architect rows spelled with an
    `e` segment; the response was to rename them, which fixed those twelve and left
    the hazard intact — forty more rows were written in unaccepted shapes afterwards,
    including a revision row whose supersession was therefore never verified and the
    deferred finding a slice closure rested on.

    Renaming is an instance patch. The invariant is that the ledger may not CONTAIN a
    row the scanner silently skips: its parser drops a non-matching id with no `else`,
    so an unparseable row is indistinguishable from a row nobody wrote. The grammar is
    imported from the scanner rather than restated — a second copy would drift exactly
    as the four earlier hand-copies of this repository's other shapes did.
    """
    # The record's own view — see `_unfenced_lines`. A fenced block is an
    # illustration, not a row.
    ledger_text = "\n".join(_unfenced_lines(ledger_text))
    import re
    import sys as _sys

    _sys.path.insert(0, str(Path(__file__).resolve().parent))
    from test_wave_gate import _FINDING_ID_RE

    # CANDIDATES COME FROM TABLE CONTEXT, not from the shape of the token. Selecting
    # them by shape — a bare token carrying the issue infix — excluded exactly the
    # malformed identifiers the rule exists to catch: `ARCH-155-r10 03` contains a
    # space, so the shape filter skipped it and the scanner skipped it too, and the row
    # stayed invisible with the guard green. A filter that drops what it is looking for
    # is the same fail-open one level up. The finding table is located by its own `ID`
    # header and read to the first blank line, which is the boundary the ledger's other
    # derived checks already use.
    bad = set()
    lines = ledger_text.splitlines()
    starts = [
        i for i, line in enumerate(lines)
        if line.startswith("|") and line.split("|")[1].strip() == "ID"
    ]
    for start in starts:
        for line in lines[start + 1 :]:
            if not line.strip():
                break
            if not line.startswith("|"):
                break
            cells = line.split("|")
            if len(cells) < 6:
                continue
            rid = cells[1].strip().strip("*").strip()
            # Separator rows and the derived defect-CLASS rows are not findings; the
            # scanner excludes DC rows for its own reason, that their counts are an
            # aggregate rather than an append-only record.
            if not rid or rid == "ID" or set(rid) <= {"-", " "} or rid.startswith("DC-"):
                continue
            if not re.fullmatch(_FINDING_ID_RE, rid):
                bad.add(rid)
    return sorted(bad)


@pytest.mark.parametrize(
    "rid,caught",
    [
        ("CDX-155-r98-01", False),
        ("ARCH-155-r10-03a", False),
        # The exact shapes this ledger actually shipped unscanned.
        ("ARCH-155-D1-03", True),
        ("ARCH-155-B-e3-05a", True),
        ("SELF-155-D-pf-01", True),
        ("CDX-155-R-01", True),
        # Whitespace: the scanner rejects it AND the earlier shape filter
        # skipped it, so the row was invisible with this guard green.
        ("ARCH-155-r10 03", True),
        ("CDX-155-r100-01 ", False),
    ],
)
def test_the_identifier_rule_catches_the_shapes_that_shipped_unscanned(rid, caught):
    """Non-vacuity: the rule must reject the real historical shapes, not just parse."""
    table = ("| ID | source | summary | label | class | tier | sha | disposition |\n"
             "| --- | --- | --- | --- | --- | --- | --- | --- |\n"
             f"| {rid} | src | summary | label | class | tier | sha | fixed |\n")
    assert bool(_unscannable_finding_ids(table)) is caught


def test_every_finding_row_in_this_ledger_is_visible_to_the_scanner():
    """Applied to this ledger. A row no scanner can read is not a record."""
    root = Path(__file__).resolve().parents[1]
    ledger = (root / "docs/architecture/ISSUE_155_AUDIT_LEDGER.md").read_text()
    unscannable = _unscannable_finding_ids(ledger)
    assert not unscannable, (
        "these rows carry finding identifiers the ledger scanner silently skips, so "
        f"nothing checks their tier, disposition or supersession: {unscannable}"
    )


def _unfenced_lines(text):
    """The record's lines. The record may not contain a fenced block at all.

    This began as a filter: a fenced illustration is not part of the record, so blank
    it. Four consecutive reviews then produced four different openers the filter got
    wrong — a tilde fence, a four-backtick fence quoting three-backtick examples, and
    a three-backtick line whose info string carries another backtick, which CommonMark
    does not treat as an opener at all. Each was a real bypass: quoted rows were read
    as live record, or live rows were blanked and vanished from a window count.

    That is the shape the tracked rule describes — a reader over Markdown's rendering
    rules cannot make the coverage claim the structural-fix rule demands, and every
    case it learns disguises that a little longer. So the filter is GONE. The record
    is plain lines, a fence-like opener is refused where every reader passes, and
    there is no opener grammar left to get wrong. Measured when this landed: the
    ledger contains no fence, so the prohibition costs it nothing.
    """
    import re

    offenders = [
        (i + 1, run.group(1)[:6])
        for i, line in enumerate(text.splitlines())
        if (run := re.match(r"(`{3,}|~{3,})", line.lstrip()))
    ]
    if offenders:
        raise AssertionError(
            "the record may not contain a fenced block, and this reader does not "
            f"parse one: {offenders}. Quote a format inline, or in a separate file."
        )
    return text.splitlines()


def _table_rows(lines, header_test):
    """Every row of EVERY table whose header satisfies `header_test`.

    Every table, not the first: a second table carrying real rows was ignored whole.
    The separator row is REQUIRED rather than assumed — skipping a fixed two lines
    swallowed the first real row whenever a table lacked one.
    """
    rows = []
    for i, line in enumerate(lines):
        if not header_test(line):
            continue
        j = i + 1
        if j < len(lines) and set(lines[j].replace("|", "").strip()) <= {"-", " ", ":"} \
                and lines[j].startswith("|"):
            j += 1
        while j < len(lines) and lines[j].startswith("|"):
            rows.append(lines[j])
            j += 1
    return rows


def _finding_table_rows(ledger_text):
    """Every row of the ledger's finding TABLES, as `(id, cells)`.

    Scoped to the tables, not the document. Reading every pipe-prefixed line counted
    rows inside fenced illustrations and rows belonging to unrelated tables, so a
    checkpoint's total could be padded by text that is not a finding at all. The table
    is located by its own `ID` header and read to the first blank line — the same
    boundary the ledger's other derived checks use.

    The identifier grammar is IMPORTED, never restated. A local copy here had drifted
    NARROWER than the shared one and thirteen live rows fell in the gap: accepted by
    the scanner that polices identifiers, invisible to the inventory that counts them.
    That is the unpinned-hand-copy mechanism this repository has a structural rule
    against, sitting inside a guard written to enforce another one.
    """
    import re
    import sys as _sys

    _sys.path.insert(0, str(Path(__file__).resolve().parent))
    from test_wave_gate import _FINDING_ID_RE

    lines = _unfenced_lines(ledger_text)
    out, malformed = [], []
    for line in _table_rows(
        lines, lambda l: l.startswith("|") and l.split("|")[1].strip() == "ID"
    ):
        if True:
            # A Markdown row may omit its TRAILING delimiter; the cells are all
            # there. Splitting without normalising put the last field where the
            # reader expected the empty tail, so a complete row read as one column
            # short — and I diagnosed that as a missing disposition and recorded an
            # unrepairable limitation on it, having printed every cell except the one
            # that mattered. Normalise the delimiter, then count.
            cells = (line if line.rstrip().endswith("|") else line.rstrip() + " |").split("|")
            rid = cells[1].strip().strip("*").strip()
            if not rid or rid == "ID" or set(rid) <= {"-", " "} or rid.startswith("DC-"):
                continue
            if not re.fullmatch(_FINDING_ID_RE, rid):
                continue      # the identifier guard owns this failure, not this one
            # A finding-shaped id on a row with the wrong column count is NOT a row to
            # skip: dropping columns was a way to delete a secrets/security finding
            # from every window while both guards stayed green.
            if len(cells) != 11:
                malformed.append((rid, cells))
                continue
            out.append((rid, cells))
    return out, malformed


def _window_inventory(ledger_text, runs):
    """The finding rows a checkpoint window contains, and the class that stands.

    DERIVED, because a checkpoint's mandatory inputs — how many findings, in which
    defect classes — were hand-counted and were wrong twice in a row: once by summing
    a series that did not add up, and once by crediting the window with a class minted
    in the NEXT one. A checkpoint whose numbers come from memory is a decision informed
    by whatever the author recalled, which is what the checkpoint rule exists to stop.

    Returns `(inventory, problems)`. A problem is a fact that makes THIS WINDOW's
    inventory unsound. Scoped to the window deliberately: an earlier draft reported
    over the whole document and flagged two hundred historical rows written before the
    defect-class column existed — a guard that fails on history nobody can supply
    stops being run, which is a worse outcome than the one it was guarding against.
    """
    # The record's own view — see `_unfenced_lines`. A fenced block is an
    # illustration, not a row.
    ledger_text = "\n".join(_unfenced_lines(ledger_text))
    import re

    rows, malformed = _finding_table_rows(ledger_text)
    in_window = lambda cells: len(cells) > 2 and any(r in cells[2] for r in runs)
    # A row a later revision SUPERSEDES has had its defects corrected there, so it is
    # not reported again here — including a malformation. The supersession map is read
    # below; this list is completed once it is known.
    malformed_in_window = [(rid, cells) for rid, cells in malformed if in_window(cells)]
    problems = []

    # A revision supersedes its original's CLASS but never its WINDOW. A finding
    # raised in evaluation 5 and reclassified in evaluation 7 still belongs to the
    # window that found it — reading membership off the revision would silently move
    # it forward and empty the window that owns it.
    supersedes, duplicate_revisions = {}, set()
    for m in re.finditer(r"`([A-Za-z]+-155-(?:r\d+-)?\d+[a-z]) → ([A-Za-z0-9-]+)`", ledger_text):
        if m.group(1) in supersedes and supersedes[m.group(1)] != m.group(2):
            duplicate_revisions.add(m.group(1))
        supersedes[m.group(1)] = m.group(2)
    revision_of = {}
    for rev, orig in supersedes.items():
        revision_of.setdefault(orig, []).append(rev)

    problems += [f"{rid}: a finding row with the wrong column count"
                 for rid, _ in malformed_in_window if rid not in supersedes]

    parsed, duplicates, _defect_cell = {}, set(), {}
    for rid, cells in rows:
        if rid in parsed:
            duplicates.add(rid)
            continue
        # The FIRST class token is the one that stands: this ledger's revision rows
        # spell a correction as "DC-155-S … — CORRECTED from DC-155-O", so a cell
        # naming two classes names the new one first. That is the record's own
        # convention, read from it rather than modelled here.
        found = re.search(r"DC-155-[A-Z]+\d*", cells[6])
        parsed[rid] = (cells[2], found.group(0) if found else None, cells[5], cells[9])
        _defect_cell[rid] = cells[6]

    missing_revision = []

    def standing(rid):
        """The row at the end of `rid`'s revision chain — the one that stands."""
        seen, current = set(), rid
        while current in revision_of and current not in seen:
            seen.add(current)
            # NUMERICALLY. Lexical ordering put `r7` after `r10`, so once this issue
            # passed round nine the newest revision stopped winning.
            current = sorted(revision_of[current],
                             key=lambda n: ([int(x) for x in re.findall(r"\d+", n)], n))[-1]
        if current not in parsed:
            missing_revision.append(current)
            return None
        return current

    inventory = {}
    for rid, (source, _, _blocking, _disp) in parsed.items():
        if rid in supersedes:
            # Counted through the row it revises — but that row must EXIST. A
            # revision declared against an original nobody wrote removed a real
            # finding from every window, since neither row was ever counted.
            if any(run in source for run in runs):
                # A revision must point at a row that parses. The softening this
                # once carried was built on a misreading — the row it exempted was
                # complete and only missing its trailing delimiter, which the reader
                # now normalises — so the exemption is removed rather than kept as
                # dead tolerance.
                if supersedes[rid] not in parsed:
                    problems.append(
                        f"{rid}: revises {supersedes[rid]}, which has no finding row")
                # REACHED HERE, not in the loop below: a revision id is always in
                # `supersedes`, so a duplicate-declaration check placed after the
                # `continue` was dead code — a check that cannot fire is a claim the
                # record makes and cannot keep.
                if rid in duplicate_revisions:
                    problems.append(
                        f"{rid}: declared as a revision of two different rows")
            continue
        if not any(run in source for run in runs):
            continue
        if rid in duplicates:
            problems.append(f"{rid}: appears twice in the finding tables")
        stands = standing(rid)
        klass = parsed[stands][1] if stands else None
        # NO WAIVER. Three successive versions tried to let a blocking row declare
        # `none` for its defect class when its disposition refuted the finding, and
        # each was defeated by a trailing clause that revives the finding using no
        # disposition word at all — first a prefix test, then a truncation, then a
        # shape. The clause is prose, and prose cannot be made to declare its own
        # contradiction, so every version of this waiver is a model of language rather
        # than a check on a field. The waiver is REMOVED: a row in a blocking class
        # carries a defect class, without exception. A refuted finding belongs to no
        # blocking class — there is no finding — so it records `none` THERE, in the
        # column that decides, and needs no exemption. Nothing here parses a
        # disposition any more.
        blocking_flat = parsed[stands][2].strip().lower().rstrip(".") if stands else ""
        owed = not (blocking_flat in {"none", "n/a", ""}
                    or re.match(r"none\s*[—(,]", blocking_flat))
        if owed and klass is None:
            problems.append(f"{rid}: is in a blocking class but carries no defect class")
        inventory[rid] = klass
    problems += [f"{r}: declared as a revision but has no finding row"
                 for r in missing_revision]
    return inventory, problems


def _checkpoint_rows(ledger_text):
    """Every row of the ledger's CHECKPOINT table, located by its own header.

    Recognition is structural — membership in the table — not textual. Keying on the
    loop name let a re-cased or bolded heading skip the check; keying on a well-formed
    count made a malformed count the way to hide; and keying on the presence of the
    metadata field made OMITTING the field the way to hide, which was strictly worse
    than what it replaced. Worse still, a text key made this guard fire on rows that
    are not checkpoints at all: this ledger records findings ABOUT these field names,
    and one colon inside a verbatim summary was enough to refuse a correct closing.
    A mandatory gate that blocks a correct closing is a worse failure than one that
    misses a malformed row, because the first stops all work and the second is caught
    by the next reader.
    """
    lines = _unfenced_lines(ledger_text)
    test = lambda l: l.startswith("| Loop | Evaluation (window / cumulative)")
    if not any(test(l) for l in lines):
        return None
    return _table_rows(lines, test)


def _checkpoint_inventory_violation(ledger_text):
    """Whether EVERY checkpoint agrees with the rows in its own window.

    Every row, not just the newest: the moment the next checkpoint is appended, a
    last-row rule stops looking at the one before it, and an earlier row's inventory
    could then be wrong for good while its own prose claimed it is checked on every run.
    """
    # The record's own view — see `_unfenced_lines`. A fenced block is an
    # illustration, not a row.
    ledger_text = "\n".join(_unfenced_lines(ledger_text))
    import re

    def field(row, name):
        # DELIMITED, at a boundary, and exactly once. An undelimited capture swallowed
        # the rest of the cell so prose supplied the values; `search` took the FIRST
        # occurrence so a decoy beat the formal field; and an unanchored name matched
        # inside a longer one, so writing "PRIOR WINDOW ROWS:" in a rationale refused
        # a correct row. The trailing ` . ` is required of every field including the
        # last — a format contract on a field this record's author writes.
        hits = re.findall(rf"(?:^|\. |\| ){name}:(.*?) \. ", row)
        return hits[0] if len(hits) == 1 else None

    rows = _checkpoint_rows(ledger_text)
    if rows is None:
        return "the ledger has no checkpoint table"
    # What the ARCHIVE says a collected review actually reviewed — the authority for
    # whether a tree has been validated, never the ledger's own prose about it.
    # RUN IDENTITY IS KEPT, and so is the loop each review was billed to. Pooling
    # every archived SHA let an unrelated loop's review vouch for this one's tree —
    # logical loops are distinct, and a gate covers a tree only for the loop that ran
    # it. The loop is read from the evidence index, which is where the archiver
    # records it, not from the ledger's prose about it.
    import json as _json
    root = Path(__file__).resolve().parents[1]
    archive = root / "docs/architecture/evidence/issue-155/commit-reviews"
    index = root / "docs/architecture/evidence/issue-155/index.jsonl"
    loop_of_run = {}
    if index.is_file():
        for raw in index.read_text().splitlines():
            if not raw.strip():
                continue
            try:
                entry = _json.loads(raw)
            except ValueError:
                continue
            name = str(entry.get("durable_dir", "")).rsplit("/", 1)[-1]
            if name:
                loop_of_run[name] = str(entry.get("logical_loop", ""))
    # EACH RUN TO ITS OWN SHA. Keying by loop and unioning let any review sharing a
    # loop label vouch for a tree a DIFFERENT run reviewed — the same
    # lose-the-identity defect as pooling every archive, one level in. A checkpoint's
    # coverage comes from the runs it names, individually.
    reviewed_of_run = {}
    if archive.is_dir():
        for d in sorted(archive.iterdir()):
            f = d / "last-reviewed-sha"
            if f.is_file():
                reviewed_of_run[d.name] = f.read_text().strip()

    def label(row):
        cells = row.split("|")
        return cells[1].strip().strip("*").replace("\u00a0", " ").strip("`").strip()

    # Which loops use the machine-readable window at all. Every check below is scoped
    # to them: older loops recorded their windows in prose, and demanding the format
    # there would fail on history nobody can supply — the failure mode that makes a
    # guard get switched off rather than obeyed.
    metadata_loops = {label(r) for r in rows if field(r, "WINDOW RUNS") is not None}

    parsed, seen_runs = [], {}
    for row in rows:
        cells = row.split("|")
        loop = label(row)
        if loop not in metadata_loops:
            continue
        if len(cells) < 6:
            return f"a checkpoint row has too few columns: {row[:60]!r}"
        # The ratio is read as a PREFIX of the cell, not as the whole of it. Six live
        # historical rows annotate the count — `5 / 5 (revision c)` — and demanding
        # the bare form refused them, which is a guard breaking on the record it is
        # meant to protect.
        count = re.match(r"\s*(\d+)\s*/\s*(\d+)", cells[2])
        if not count:
            return f"checkpoint {loop!r} states no window/cumulative count"
        window, cumulative = int(count.group(1)), int(count.group(2))
        parsed.append((loop, window, cumulative, row, field(row, "WINDOW RUNS") is not None))

    running = {}
    for loop, window, cumulative, row, has in parsed:
        # RECOMPUTED per row. Without this, `cells` still held the LAST row parsed by
        # the loop above, so every check below read one row's outcome and another
        # row's SHA — the clause ran four times against the same cells and could not
        # fail. Found by instrumenting the loop rather than by reading it; the
        # external replica of the same logic refused the mutant correctly, which is
        # what made the disagreement visible.
        cells = row.split("|")
        # The DENOMINATOR carries the loop's running total across ALL its rows.
        expected = running.get(loop, 0) + window
        if cumulative != expected:
            return (f"checkpoint {loop!r} states a cumulative total of {cumulative} "
                    f"where its history gives {expected}")
        running[loop] = cumulative

        if not has:
            # But once a loop HAS adopted it, omitting it is not a way out. This is
            # the regression the field-presence key introduced, closed structurally.
            return (f"checkpoint {loop!r} omits the window metadata its own loop "
                    "records elsewhere")
        names = re.findall(r"`(cdx-review\.[A-Za-z0-9]+)`",
                           field(row, "WINDOW RUNS") or "")
        if not names:
            return f"checkpoint {loop!r} names no run in its window"
        if len(set(names)) != len(names):
            return f"checkpoint {loop!r} names a run twice: {sorted(names)}"
        if len(names) != window:
            return (f"checkpoint {loop!r} covers {window} evaluations but names "
                    f"{len(names)} runs")
        for run in names:
            if run in seen_runs:
                # Two windows claiming one evaluation count the same finding twice,
                # which is how a closed window was swallowed by the next one here.
                return f"{run} is claimed by both {seen_runs[run]!r} and {loop!r}"
            seen_runs[run] = loop

        # A CHECKPOINT MAY NOT CLAIM CLOSURE ON A TREE NO REVIEW HAS COVERED. This
        # class's first axis — a decision written before its owed validation — was
        # answered PROCEDURALLY ("write the checkpoint after the validation") and has
        # recurred thirty times, most recently in a row written in the same commit as
        # the correction it blessed. A procedure cannot enforce an ordering; this can.
        # The checkpoint names the SHA it decided on; the archive records which SHAs a
        # collected review actually covered; a checkpoint whose SHA is in neither may
        # describe the state but may not declare the loop closable.
        # EVERY loop-ending outcome, enumerated from the workflow's own set rather
        # than from the one spelling I happened to write. `CONTINUE` starts another
        # window and `ESCALATE-OPEN` leaves the issue open, so neither asserts a
        # validated tip; the other three all end the loop and all require one.
        ENDING = ("CLOSE-CLEAN", "DEFER-STANDARD-AND-PROCEED", "DEFER-STANDARD-AND-CLOSE")
        outcome = cells[4] if len(cells) > 4 else ""
        if any(e in outcome for e in ENDING):
            sha = re.search(r"`([0-9a-f]{7,40})`", cells[3] if len(cells) > 3 else "")
            if not sha:
                # Missing evidence is a violation, not a reason to skip the body.
                return f"checkpoint {loop!r} ends its loop without naming a SHA"
            named = sha.group(1)
            covered = {reviewed_of_run[r] for r in names if r in reviewed_of_run}
            # EXACT when the row writes a full SHA. Accepting a 40-character value
            # because its first seven matched is how a fabricated SHA passes, and
            # this record has already carried one — thirty-three invented characters
            # behind a correct prefix.
            if len(named) == 40:
                ok = named in covered
            else:
                ok = any(c.startswith(named) for c in covered)
            if not ok:
                return (f"checkpoint {loop!r} ends its loop on {named[:12]}, which no "
                        "archived review of that loop covers")

        rows_field = field(row, "WINDOW ROWS")
        if rows_field is None or not rows_field.strip().isdigit():
            return f"checkpoint {loop!r} does not state its row count exactly once"
        classes_field = field(row, "WINDOW CLASSES")
        if classes_field is None:
            return f"checkpoint {loop!r} does not state its classes exactly once"

        inventory, problems = _window_inventory(ledger_text, names)
        if problems:
            return f"the finding tables are unsound: {sorted(set(problems))}"
        if len(inventory) != int(rows_field.strip()):
            return (f"checkpoint {loop!r} holds {len(inventory)} finding rows but "
                    f"states {rows_field.strip()}")
        derived = {c for c in inventory.values() if c}
        claimed = set(re.findall(r"DC-155-[A-Z]+\d*", classes_field))
        if derived != claimed:
            return (f"checkpoint {loop!r} has classes {sorted(derived)} but claims "
                    f"{sorted(claimed)}")
    return None


#: This slice's literal baseline. The manifest check derives its scope from the
#: files changed since it, so the scope follows the work instead of naming one.
_SLICE_BASELINE = "d04a2482d9b43c84765395ec8bbc73495de29fd4"


_AUDIT_BASE = (
    "| ID | source | summary | label | blocking | defect class | tier | sha "
    "| disposition |\n"
    "| --- | --- | --- | --- | --- | --- | --- | --- | --- |\n"
    "| CDX-155-r1-01 | round run `cdx-review.aa` | s | P1 | runtime behavior "
    "| DC-155-G a closure | critical | x | fixed |\n"
    "{extra}"
    "\n"
    "| Loop | Evaluation (window / cumulative) | SHA (+dirty) | Outcome | Rationale |\n"
    "| --- | --- | --- | --- | --- |\n"
    "| L5 closing protocol | 1 / 1 | x | `CONTINUE` | WINDOW RUNS: `cdx-review.aa` . "
    "WINDOW ROWS: {rows} . WINDOW CLASSES: {classes} . rationale |\n"
)


@pytest.mark.parametrize(
    "name,extra,rows,classes,must_refuse",
    [
        # An id the SHARED scanner accepts but a local hand-copy did not: thirteen
        # live rows sat in exactly this gap, countable by one guard and invisible to
        # the other. The grammar is imported now, so the row is seen.
        ("foreign-prefix id",
         "| EVAL-155-07 | run `cdx-review.aa` | s | P0 | data loss | none "
         "| critical | x | deferred |\n", "1", "`DC-155-G`", True),
        # A bolded id was stripped by the identifier guard and not by this one, so the
        # row vanished from the window while both guards read clean.
        ("bolded id",
         "| **CDX-155-r1-02** | run `cdx-review.aa` | s | P1 | runtime behavior "
         "| DC-155-S x | critical | x | fixed |\n", "1", "`DC-155-G`", True),
        # Dropping columns deleted a secrets/security row from the count entirely.
        ("short row",
         "| CDX-155-r1-02 | run `cdx-review.aa` | s | secrets/security "
         "| DC-155-S x | fixed |\n", "1", "`DC-155-G`", True),
        # The same id twice collapsed two findings into one.
        ("duplicate id",
         "| CDX-155-r1-01 | run `cdx-review.aa` | s | P1 | runtime behavior "
         "| DC-155-S x | critical | x | fixed |\n", "1", "`DC-155-G`", True),
        # A blocking cell that merely OPENS with the word none switched off the
        # requirement that the row carry a class at all.
        ("none-prefixed blocking class",
         "| CDX-155-r1-02 | run `cdx-review.aa` | s | P1 "
         "| none of the listed classes apply, though it is runtime behavior |  "
         "| critical | x | fixed |\n", "2", "`DC-155-G`", True),
        # A revision declared against a row that does not exist erased a real finding.
        ("revision of a missing row",
         "| CDX-155-r1-02a | run `cdx-review.aa` | s | P1 | runtime behavior "
         "| DC-155-S x | critical | x | fixed |\n"
         "\nSupersession: `CDX-155-r1-02a → CDX-155-r99-99`\n", "1", "`DC-155-G`", True),
    ],
)
def test_the_inventory_refuses_the_shapes_that_erased_a_finding(
    name, extra, rows, classes, must_refuse
):
    """Each case is a way a real finding left its window while the guard read clean.

    Every one was found by adversarially probing this guard rather than by the next
    review round discovering them one at a time — the loop had been surfacing one per
    round, which for a checker over free-form text does not terminate.
    """
    ledger = _AUDIT_BASE.format(extra=extra, rows=rows, classes=classes)
    violation = _checkpoint_inventory_violation(ledger)
    assert (violation is not None) is must_refuse, f"{name}: {violation!r}"


@pytest.mark.parametrize(
    "cell,tail,must_refuse",
    [
        # A decoy earlier in the row beat the formal field, so the declared list was
        # fiction. Both fields are now required to appear exactly once.
        ("earlier note WINDOW CLASSES: `DC-155-G` end",
         "WINDOW RUNS: `cdx-review.aa` . WINDOW ROWS: 1 . "
         "WINDOW CLASSES: `DC-155-Z` . r", True),
        # `WINDOW ROWS` was searched over the whole row, so prose supplied the count.
        ("rebaselined; WINDOW ROWS: 1 as previously recorded",
         "WINDOW RUNS: `cdx-review.aa` . WINDOW ROWS: 9 . "
         "WINDOW CLASSES: `DC-155-G` . r", True),
        # Omitting the terminator let prose supply the runs — the contamination the
        # delimiter was added to stop, still open on this field until now.
        ("x", "WINDOW RUNS: `cdx-review.aa`; carried from `cdx-review.bb` "
              "WINDOW ROWS: 1 . WINDOW CLASSES: `DC-155-G` . r", True),
        ("x", "WINDOW RUNS: `cdx-review.aa` . WINDOW ROWS: 1 . "
              "WINDOW CLASSES: `DC-155-G` . r", False),
    ],
)
def test_a_checkpoint_field_may_not_be_supplied_by_prose(cell, tail, must_refuse):
    """The formal declaration must be the thing read, not the prose beside it."""
    ledger = (
        "| ID | source | summary | label | blocking | defect class | tier | sha "
        "| disposition |\n"
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |\n"
        "| CDX-155-r1-01 | round run `cdx-review.aa` | s | P1 | runtime behavior "
        "| DC-155-G a closure | critical | x | fixed |\n"
        "\n"
        "| Loop | Evaluation (window / cumulative) | SHA (+dirty) | Outcome | Rationale |\n"
        "| --- | --- | --- | --- | --- |\n"
        f"| L5 closing protocol | 1 / 1 | {cell} | `CONTINUE` | {tail} |\n"
    )
    violation = _checkpoint_inventory_violation(ledger)
    assert (violation is not None) is must_refuse, repr(violation)


@pytest.mark.parametrize(
    "heading,count,must_refuse",
    [
        ("| **L5 closing protocol**", "7 / 2", True),
        ("| L5 Closing protocol", "7 / 2", True),
        ("| L5\u00a0closing protocol", "7 / 2", True),
        ("| L5 closing protocol", "1 of 1", True),      # a malformed count
        ("| L5 closing protocol", "2 / 1", True),       # an impossible cumulative
        ("| L5 closing protocol", "1 / 1", False),
    ],
)
def test_a_checkpoint_cannot_hide_behind_its_heading_or_its_count(
    heading, count, must_refuse
):
    """A row DECLARES itself a checkpoint by carrying window metadata.

    Keying recognition on the loop name let a bolded, re-cased or non-breaking-spaced
    heading skip the check; keying it on a well-formed count made a MALFORMED count
    the way to become invisible. A recognition test that is also a validity test has
    an escape hatch shaped exactly like its own failure mode.
    """
    ledger = (
        "| ID | source | summary | label | blocking | defect class | tier | sha "
        "| disposition |\n"
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |\n"
        "| CDX-155-r1-01 | round run `cdx-review.aa` | s | P1 | runtime behavior "
        "| DC-155-G a closure | critical | x | fixed |\n"
        "\n"
        "| Loop | Evaluation (window / cumulative) | SHA (+dirty) | Outcome | Rationale |\n"
        "| --- | --- | --- | --- | --- |\n"
        f"{heading} | {count} | x | `CONTINUE` | WINDOW RUNS: `cdx-review.aa` . "
        "WINDOW ROWS: 1 . WINDOW CLASSES: `DC-155-G` . r |\n"
    )
    violation = _checkpoint_inventory_violation(ledger)
    assert (violation is not None) is must_refuse, repr(violation)


def test_two_checkpoints_may_not_claim_the_same_evaluation():
    """Overlapping windows count one finding twice — the error already made here.

    The `6 / 6` checkpoint swallowed the two evaluations a `2 / 2` checkpoint had
    already dispositioned. Uniqueness inside one row cannot see that.
    """
    ledger = (
        "| ID | source | summary | label | blocking | defect class | tier | sha "
        "| disposition |\n"
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |\n"
        "| CDX-155-r1-01 | round run `cdx-review.aa` | s | P1 | runtime behavior "
        "| DC-155-G a closure | critical | x | fixed |\n"
        "\n"
        "| Loop | Evaluation (window / cumulative) | SHA (+dirty) | Outcome | Rationale |\n"
        "| --- | --- | --- | --- | --- |\n"
        "| L5 closing protocol | 1 / 1 | x | `CONTINUE` | WINDOW RUNS: "
        "`cdx-review.aa` . WINDOW ROWS: 1 . WINDOW CLASSES: `DC-155-G` . r |\n"
        "| L5 closing protocol | 1 / 2 | x | `CONTINUE` | WINDOW RUNS: "
        "`cdx-review.aa` . WINDOW ROWS: 1 . WINDOW CLASSES: `DC-155-G` . r |\n"
    )
    assert _checkpoint_inventory_violation(ledger) == (
        "cdx-review.aa is claimed by both 'L5 closing protocol' and "
        "'L5 closing protocol'"
    )


def test_the_standing_class_wins_a_numeric_revision_chain():
    """Lexical ordering put `r7` after `r10`, so past round nine the newest lost."""
    ledger = (
        "| ID | source | summary | label | blocking | defect class | tier | sha "
        "| disposition |\n"
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |\n"
        "| CDX-155-r1-01 | round run `cdx-review.aa` | s | P1 | runtime behavior "
        "| DC-155-G a closure | critical | x | fixed |\n"
        "| CDX-155-r7-01a | run `cdx-review.aa` | s | P1 | runtime behavior "
        "| DC-155-S x | critical | x | fixed |\n"
        "| CDX-155-r10-01a | run `cdx-review.aa` | s | P1 | runtime behavior "
        "| DC-155-R x | critical | x | fixed |\n"
        "\nMap: `CDX-155-r7-01a → CDX-155-r1-01`, `CDX-155-r10-01a → CDX-155-r1-01`\n"
        "\n"
        "| Loop | Evaluation (window / cumulative) | SHA (+dirty) | Outcome | Rationale |\n"
        "| --- | --- | --- | --- | --- |\n"
        "| L5 closing protocol | 1 / 1 | x | `CONTINUE` | WINDOW RUNS: "
        "`cdx-review.aa` . WINDOW ROWS: 1 . WINDOW CLASSES: `DC-155-R` . r |\n"
    )
    assert _checkpoint_inventory_violation(ledger) is None


_CK_HDR = ("| Loop | Evaluation (window / cumulative) | SHA (+dirty) | Outcome "
           "| Rationale |\n| --- | --- | --- | --- | --- |\n")
_BAD_CK = ("| L5 closing protocol | 7 / 8 | x | `CONTINUE` | WINDOW RUNS: "
           "`cdx-review.zz` . WINDOW ROWS: 99 . WINDOW CLASSES: `DC-155-Q` . r |\n")
_GOOD_ROW = ("| CDX-155-r1-01 | run `cdx-review.aa` | s | P1 | runtime behavior "
             "| DC-155-G a closure | critical | x | fixed |\n")
_FIND_HDR = ("| ID | source | summary | label | blocking | defect class | tier | sha "
             "| disposition |\n| --- | --- | --- | --- | --- | --- | --- | --- | --- |\n")
_OK_CK = ("| L5 closing protocol | 1 / 1 | x | `CONTINUE` | WINDOW RUNS: "
          "`cdx-review.aa` . WINDOW ROWS: 1 . WINDOW CLASSES: `DC-155-G` . r |\n")


@pytest.mark.parametrize(
    "name,ledger",
    [
        # A quoted example of the header hijacked the guard: it read the example's
        # rows and never reached the real table. This ledger documents its own
        # formats, so quoting them is a thing it does.
        ("fenced header before the real table",
         _FIND_HDR + _GOOD_ROW + "\n```\n" + _CK_HDR + "| Example | 1 / 1 | x | `x` | prose |\n"
         + "```\n\n" + _CK_HDR + _OK_CK + _BAD_CK),
        # A second real table was ignored whole.
        ("a second checkpoint table",
         _FIND_HDR + _GOOD_ROW + "\n" + _CK_HDR + _OK_CK + "\n## Later\n\n" + _CK_HDR + _BAD_CK),
        # Skipping a fixed two lines swallowed the first row when a table had no
        # separator, which is the row most likely to be the newest one.
        ("no separator row",
         _FIND_HDR + _GOOD_ROW + "\n"
         + "| Loop | Evaluation (window / cumulative) | SHA (+dirty) | Outcome | Rationale |\n"
         + _BAD_CK + _OK_CK),
        # A fenced illustration of a finding row padded a real window's count.
        ("fenced finding row pads a window",
         _FIND_HDR + _GOOD_ROW
         + "\n```\n" + _FIND_HDR
         + "| CDX-155-r1-99 | run `cdx-review.aa` | s | P1 | data loss "
           "| DC-155-Z x | critical | x | fixed |\n```\n\n"
         + _CK_HDR
         + "| L5 closing protocol | 1 / 1 | x | `CONTINUE` | WINDOW RUNS: "
           "`cdx-review.aa` . WINDOW ROWS: 2 . WINDOW CLASSES: `DC-155-G`, "
           "`DC-155-Z` . r |\n"),
    ],
)
def test_the_guard_reads_the_record_and_not_an_illustration_of_it(name, ledger):
    """Each case had the guard reading the wrong object entirely.

    These are the last shapes fixed in this loop. The residual paths beyond them are
    enumerated in the record with a boundary verdict: three successive rewrites each
    RELOCATED the recognition escape rather than closing it, which is the signature of
    a checker over free-form text, and the terminating design is named there instead.
    """
    try:
        violation = _checkpoint_inventory_violation(ledger)
    except AssertionError as refusal:
        # The fenced cases now end here, and that is the stronger verdict: the record
        # may not carry a fence at all, so the guard never has to decide what an
        # illustration was trying to say.
        assert "may not contain" in str(refusal), name
        return
    assert violation is not None, name


@pytest.mark.parametrize(
    "blocking,disposition,must_refuse",
    [
        # THE WITNESS FOR REMOVING THE WAIVER: the input on which the old helper and
        # the new one disagree — a row IN a blocking class whose defect-class cell
        # says `none` and whose disposition is refutation-shaped. Three earlier
        # versions let that waive the class, each defeated by a trailing clause
        # carrying no disposition word. Without this case the whole change could be
        # reverted with every other test still green, which is what the review found.
        ("runtime behavior", "finding-refuted — measured", True),
        ("runtime behavior", "finding-refuted — evidence; remainder validated", True),
        ("runtime behavior", "not-validated as a defect and handled by supersession", True),
        # A refuted finding belongs to no blocking class, so it records that in the
        # column that DECIDES and needs no exemption anywhere else.
        ("none — the finding is refuted", "finding-refuted — measured", False),
    ],
)
def test_a_blocking_row_never_waives_its_defect_class(blocking, disposition, must_refuse):
    """The waiver is gone: a blocking row carries a class, without exception."""
    ledger = (
        "| ID | source | summary | label | blocking | defect class | tier | sha "
        "| disposition |\n"
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |\n"
        f"| CDX-155-r1-01 | run `cdx-review.aa` | s | P1 | {blocking} | none — refuted "
        f"| critical | x | {disposition} |\n"
        "\n"
        "| Loop | Evaluation (window / cumulative) | SHA (+dirty) | Outcome | Rationale |\n"
        "| --- | --- | --- | --- | --- |\n"
        "| L5 closing protocol | 1 / 1 | x | `CONTINUE` | WINDOW RUNS: `cdx-review.aa` . "
        "WINDOW ROWS: 1 . WINDOW CLASSES: (none) . r |\n"
    )
    violation = _checkpoint_inventory_violation(ledger)
    assert (violation is not None) is must_refuse, repr(violation)


def _missing_checkpoint_violation(ledger_text, index_text, slice_letter=None,
                                  wave_dir=None):
    """Whether every loop that has run N evaluations has recorded the checkpoints it owes.

    THE FIFTH AXIS of the closure-ahead-of-validation class. Ordering said WHEN a
    decision may be written, reverifiability WHETHER it can be rechecked, coverage WHAT
    tree it covers, membership WHICH rounds it may reason from — and none of them
    requires a checkpoint to EXIST. Absence was how the class kept recurring, which is
    what a fixed-interval rule is for: a procedure cannot notice its own omission.

    Counts come from the ARCHIVE, never from the ledger's prose — and from BOTH of the
    archive's authorities. Review rounds are indexed; composite-wave rounds are not,
    because a wave run has no collector attestation to index, so they live only as
    `wave-gate/*/round.json`. Reading the index alone made every wave loop invisible to
    a rule written to police it.
    """
    # The record's own view — see `_unfenced_lines`. A fenced block is an
    # illustration, not a row.
    ledger_text = "\n".join(_unfenced_lines(ledger_text))
    import json
    import re

    slices, billed = set(), {}

    def note(loop_text):
        tag = re.match(r"(L\d)", loop_text)
        here = _loop_scope(loop_text)
        if tag and here:
            slices.add(here)
            billed[(here, tag.group(1))] = billed.get((here, tag.group(1)), 0) + 1

    for raw in index_text.splitlines():
        if not raw.strip():
            continue
        try:
            entry = json.loads(raw)
        except ValueError:
            continue
        if entry.get("status") == "completed":
            note(str(entry.get("logical_loop", "")))

    if wave_dir is None:
        wave_dir = (Path(__file__).resolve().parents[1]
                    / "docs/architecture/evidence/issue-155/wave-gate")
    if wave_dir and Path(wave_dir).is_dir():
        for d in sorted(Path(wave_dir).iterdir()):
            record = d / "round.json"
            if not record.is_file():
                # ABSENT evidence is a violation too. Closing only the unreadable case
                # left the commoner one open: a directory whose record never landed
                # drops its evaluation from the count, and at an owed boundary that
                # erases the checkpoint the count demanded — while the directory's
                # summary still satisfies the wave rule, so nothing else notices.
                return f"wave round {d.name} has no round record"
            try:
                entry = json.loads(record.read_text())
            except ValueError:
                # UNREADABLE EVIDENCE IS A VIOLATION, not a round to skip. Skipping it
                # let a truncated round drop out of the count, and with it the
                # checkpoint that count made owed — evidence nobody can read failing
                # open is how a gate certifies what it never saw.
                return f"wave round {d.name} has unreadable evidence"
            if entry.get("status") == "completed":
                note(str(entry.get("logical_loop", "")))

    # A DECISION and a RECORDED GAP are different states and are tracked separately.
    # Reading only the count let a `GAP-RECORDED` row — which says explicitly that no
    # decision was made — satisfy the rule as if one had been; and exempting landed
    # slices wholesale meant their gap rows could be deleted with the audit still
    # clean. Every archived loop is audited, and each owed interval must be answered
    # by one state or the other.
    decided, gapped = {}, {}
    for row in _checkpoint_rows(ledger_text) or []:
        cells = row.split("|")
        if len(cells) < 5:
            continue
        m = re.match(r"\s*(\d+)\s*/\s*(\d+)", cells[2])
        tag = re.match(r"\s*(L\d)", cells[1].strip().strip("*").strip("`"))
        here = _loop_scope(cells[1])
        if not (m and tag and here):
            continue
        key = (here, tag.group(1))
        target = gapped if "GAP-RECORDED" in cells[4] else decided
        target[key] = max(target.get(key, 0), int(m.group(2)))

    # EVERY slice the archive knows, not the one a wave row happens to name. Deriving
    # the audited slice from the latest L4 row meant a new slice reaching its third
    # review BEFORE its first wave run was attributed to the previous slice, and its
    # missing checkpoint passed — the gap is widest exactly when the slice is youngest.
    # Landed slices are read from the slice map, where the record states it.
    landed = set()
    for line in ledger_text.splitlines():
        m = re.match(r"\|\s*([A-F])\s*\|", line)
        if m and re.search(r"\bLANDED\b|\bCLOSED CLEAN\b", line):
            landed.add(m.group(1))

    for (here, loop), count in sorted(billed.items()):
        if slice_letter and here != slice_letter:
            continue
        owed = (count // 3) * 3
        if not owed:
            continue
        # A DURABLE GAP answers only for a LANDED slice. History that cannot be
        # reconstructed is recorded as a gap and the record says so; a slice still in
        # flight owes a DECISION, because its window is open and one can still be made
        # honestly. Without this a gap row would become the way to skip a checkpoint
        # rather than the way to admit one was skipped.
        covers = decided.get((here, loop), 0)
        if here in landed:
            covers = max(covers, gapped.get((here, loop), 0))
        if covers < owed:
            return (f"loop {loop} of {here if here == 'issue-level' else f'slice {here}'} "
                    f"has {count} collected evaluations, "
                    f"so a recorded decision through {owed} is owed; the record covers "
                    f"{covers}")
    return None


def _wave_archive(tmp_path, rounds, drop_record=False, corrupt=False):
    """A wave archive with `rounds` completed slice-D rounds, optionally damaged."""
    import json

    root = tmp_path / "wave-gate"
    root.mkdir()
    for i in range(rounds):
        d = root / f"wave{i}"
        d.mkdir()
        if drop_record and i == rounds - 1:
            (d / "summary.json").write_text("{}")   # the directory survives, the record does not
            continue
        if corrupt and i == rounds - 1:
            (d / "round.json").write_text("{not json")
            continue
        (d / "round.json").write_text(json.dumps({
            "logical_loop": "L4 (composite wave gate, slice D)", "status": "completed"}))
    return root


@pytest.mark.parametrize(
    "drop_record,corrupt,expected",
    [
        (False, False, None),
        # A directory whose record never landed. The commoner damage, and the one the
        # first version of this refusal missed entirely.
        (True, False, "has no round record"),
        (False, True, "has unreadable evidence"),
    ],
)
def test_damaged_wave_evidence_is_refused_not_skipped(tmp_path, drop_record, corrupt, expected):
    """Skipping damaged evidence drops the evaluation AND the checkpoint it made owed.

    Both branches need a witness because both were written as `continue` first: an
    archived round that cannot be read is not a round that did not happen, and letting
    it vanish is how a gate certifies what it never saw.
    """
    ledger = (
        "| Loop | Evaluation (window / cumulative) | SHA (+dirty) | Outcome | Rationale |\n"
        "| --- | --- | --- | --- | --- |\n"
        "| L4 composite wave gate, slice D | 3 / 3 | x | `CONTINUE` | r |\n"
    )
    wave = _wave_archive(tmp_path, 3, drop_record=drop_record, corrupt=corrupt)
    violation = _missing_checkpoint_violation(ledger, "", wave_dir=wave)
    if expected is None:
        assert violation is None, violation
    else:
        assert violation is not None and expected in violation, violation


@pytest.mark.parametrize(
    "landed,gap,expected_refusal",
    [
        # A landed slice may answer with a durable gap: the window is closed and the
        # decision cannot be made honestly now.
        (True, True, False),
        # A landed slice with NEITHER a decision nor a gap row is still refused —
        # which the blanket landed-exemption this replaced would have accepted.
        (True, False, True),
        # An in-flight slice owes a DECISION. Accepting a gap here would make the gap
        # row the way to skip a checkpoint rather than to admit one was skipped.
        (False, True, True),
        (False, False, True),
    ],
)
def test_a_recorded_gap_answers_only_for_a_landed_slice(
    tmp_path, landed, gap, expected_refusal
):
    """The three states the gap model distinguishes, each driven separately."""
    status = "LANDED on `dev` at `abc1234`" if landed else "IN FLIGHT"
    rows = ""
    if gap:
        rows = ("| L4 composite wave gate, slice D | 3 / 3 | x | `GAP-RECORDED` | "
                "no decision was recorded |\n")
    ledger = (
        f"| D | units | kind | {status} |\n"
        "\n"
        "| Loop | Evaluation (window / cumulative) | SHA (+dirty) | Outcome | Rationale |\n"
        "| --- | --- | --- | --- | --- |\n"
        + rows
    )
    wave = _wave_archive(tmp_path, 3)
    violation = _missing_checkpoint_violation(ledger, "", wave_dir=wave)
    assert (violation is not None) is expected_refusal, repr(violation)


def test_a_loop_records_the_checkpoints_its_evaluations_owe():
    """Applied to this ledger, counting evaluations from the archive."""
    root = Path(__file__).resolve().parents[1]
    ledger = (root / "docs/architecture/ISSUE_155_AUDIT_LEDGER.md").read_text()
    index = root / "docs/architecture/evidence/issue-155/index.jsonl"
    violation = _missing_checkpoint_violation(
        ledger, index.read_text() if index.is_file() else "")
    assert violation is None, f"a mandatory checkpoint is missing: {violation}"


@pytest.mark.parametrize(
    "rounds,latest_cumulative,must_refuse",
    [
        (3, 3, False),    # the checkpoint the third evaluation owes, recorded
        (3, 2, True),     # three evaluations, a checkpoint covering only two
        (5, 3, False),    # the fourth and fifth owe nothing until the sixth
        (6, 3, True),     # six run, only three covered
        (2, 0, False),    # nothing owed yet, and none recorded
    ],
)
def test_the_missing_checkpoint_rule_counts_from_the_archive(
    tmp_path, rounds, latest_cumulative, must_refuse
):
    """The rule itself, away from this repository's record."""
    import json

    index = "\n".join(json.dumps({
        "logical_loop": "L5 (closing protocol, slice D — round %d)" % i,
        "status": "completed",
    }) for i in range(rounds))
    ledger = (
        "| Loop | Evaluation (window / cumulative) | SHA (+dirty) | Outcome | Rationale |\n"
        "| --- | --- | --- | --- | --- |\n"
        f"| L5 closing protocol, slice D | 1 / {latest_cumulative} | x | `CONTINUE` | r |\n"
    )
    violation = _missing_checkpoint_violation(ledger, index, slice_letter="D",
                                              wave_dir=tmp_path / "none")
    assert (violation is not None) is must_refuse, repr(violation)


@pytest.mark.parametrize(
    "outcome,report,must_refuse",
    [
        # No close decision yet: nothing is owed, which is what lets the report live in
        # the commit that records the decision and names the trees the gates produced.
        ("`CONTINUE`", False, False),
        ("`DEFER-STANDARD-AND-CLOSE` — WITHDRAWN", False, False),
        # A close decision without its report is the fail-open this replaces.
        ("`DEFER-STANDARD-AND-CLOSE`", False, True),
        ("`CLOSE-CLEAN`", False, True),
        ("`CLOSE-CLEAN`", True, False),
    ],
)
def test_a_closing_decision_requires_its_report(outcome, report, must_refuse):
    """Iterating only the reports that exist let a slice close without writing one."""
    body = ""
    if report:
        body = ("## Slice D — closing report\n\n"
                "| `abc1234` (**W**) | wave |\n"
                "| `abc1234` (**N−1**) | review `cdx-review.zz` |\n\n")
    ledger = (
        "| ID | source | summary | label | blocking | defect class | tier | sha "
        "| disposition |\n"
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |\n"
        "| CDX-155-r1-01 | run `cdx-review.aa` | s | P1 | runtime behavior "
        "| DC-155-G x | critical | x | fixed |\n"
        "\n" + body +
        "| Loop | Evaluation (window / cumulative) | SHA (+dirty) | Outcome | Rationale |\n"
        "| --- | --- | --- | --- | --- |\n"
        f"| L5 closing protocol, slice D | 1 / 1 | `abc1234` | {outcome} | r |\n"
    )
    violation = _closing_report_violation(
        ledger, "ledger.md", Path("/nonexistent"), lambda *a, **k: "")
    if must_refuse:
        assert violation == "slice D records a closing decision but has written no closing report"
    else:
        assert violation != (
            "slice D records a closing decision but has written no closing report"), violation


def test_every_closing_checkpoint_agrees_with_its_own_window():
    """Applied to this ledger, to EVERY checkpoint. Inputs are derived, never recalled.

    Scoped to the newest row, this stopped checking a checkpoint the moment the next
    one was written — so an earlier inventory could be wrong permanently while its own
    prose claimed it was verified on every run.
    """
    root = Path(__file__).resolve().parents[1]
    ledger = (root / "docs/architecture/ISSUE_155_AUDIT_LEDGER.md").read_text()
    violation = _checkpoint_inventory_violation(ledger)
    assert violation is None, f"the closing checkpoint is unsupported: {violation}"


def test_the_latest_wave_checkpoint_is_reverifiable_from_the_archive():
    """Applied to this ledger. Scoped to the LATEST row deliberately.

    Twenty-two earlier wave rounds were never archived and their run output no longer
    exists, so requiring evidence for all of them would fail on history nobody can
    supply. Closure depends on the CURRENT gate, and that one must be reverifiable.
    """
    root = Path(__file__).resolve().parents[1]
    ledger = (root / "docs/architecture/ISSUE_155_AUDIT_LEDGER.md").read_text()
    archive = root / "docs/architecture/evidence/issue-155/wave-gate"
    violation = _wave_evidence_violation(ledger, archive)
    assert violation is None, (
        f"the latest wave checkpoint cannot be reverified: {violation}"
    )


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
    files = sorted({f for f in candidates
                    if f.startswith("tests/") and f.endswith(".py") and (root / f).is_file()})
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


@pytest.mark.parametrize(
    "history,expected",
    [
        # newest first: a removal, then the addition — the addition must win
        ([("r1", "-MARK"), ("a1", "+MARK")], "a1"),
        # a straightforward single addition
        ([("a1", "+MARK")], "a1"),
        # removed and never re-added: nothing owns it
        ([("r1", "-MARK")], ""),
        # re-added after a removal, newest addition wins
        ([("a2", "+MARK"), ("r1", "-MARK"), ("a1", "+MARK")], "a2"),
    ],
)
def test_the_owning_commit_is_the_one_that_added_the_report(history, expected):
    """`-S` matches removals too; the anchor must be the commit that ADDED."""
    shown = dict(history)
    assert _commit_that_added(
        "MARK", [sha for sha, _ in history], lambda sha: shown[sha]
    ) == expected


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


def _is_the_permitted_rebind(value):
    """``"<sep>".join(_unfenced_lines(ledger_text))`` — and nothing else.

    A whitelist rather than a blacklist, because the blacklist lost three times: each
    refused form was replaced by another expression that mentions the filter without
    ever evaluating it, and the reader then saw an empty record while the guard stayed
    green. This shape evaluates the call eagerly, unconditionally, exactly once.
    """
    import ast

    if not isinstance(value, ast.Call) or value.keywords:
        return False
    joiner = value.func
    if not isinstance(joiner, ast.Attribute) or joiner.attr != "join":
        return False
    if not (isinstance(joiner.value, ast.Constant) and isinstance(joiner.value.value, str)):
        return False
    if len(value.args) != 1:
        return False
    call = value.args[0]
    return (
        isinstance(call, ast.Call)
        and isinstance(call.func, ast.Name)
        and call.func.id == "_unfenced_lines"
        and not call.keywords
        and len(call.args) == 1
        and isinstance(call.args[0], ast.Name)
        and call.args[0].id == "ledger_text"
    )


def _raw_ledger_reads(node):
    """Line numbers where ``node`` reads its ledger text WITHOUT the record's view.

    Extracted so the witnesses below drive THIS predicate rather than a second copy
    of it — a copy is the very defect this file keeps recording, and a witness graded
    against a private re-implementation grades nothing.
    """
    import ast

    def feeds_the_filter(call):
        return (
            isinstance(call, ast.Call)
            and isinstance(call.func, ast.Name)
            and call.func.id == "_unfenced_lines"
            and any(
                isinstance(a, ast.Name) and a.id == "ledger_text" for a in call.args
            )
        )

    # THE REBIND MUST BE UNCONDITIONAL, and a statement of the function body itself.
    # A rebind nested in an `if` used to set the line number and then exempt every
    # later read, including the branch that never filtered. Following Python's binding
    # across arbitrary control flow is one of the two open spaces the tracked rule
    # names, so this does not attempt it: one accepted shape, everything else reported.
    rebind_line = None
    for stmt in node.body:
        if not isinstance(stmt, ast.Assign):
            continue
        if not any(
            isinstance(t, ast.Name) and t.id == "ledger_text" for t in stmt.targets
        ):
            continue
        if not any(feeds_the_filter(c) for c in ast.walk(stmt.value)):
            continue
        # AND THE VALUE MUST DERIVE SOLELY FROM THE FILTER. A ternary —
        # `ledger_text = filtered if enabled else ledger_text` — puts a raw load on
        # the assignment's own line, which a line-number comparison then drops, and
        # the untaken branch reads the record unfiltered. Constructed and confirmed.
        # Rather than analyse the expression, require that every occurrence of the
        # parameter inside it is an argument to the filter.
        # ONE PERMITTED SHAPE, matched exactly. Three weaker rules were tried and each
        # was defeated by a value that mentions the filter without running it: a
        # ternary keeping the raw name, a ternary keeping a constant, and a lazy
        # comprehension the interpreter never draws from. Naming the forms to refuse
        # is a list that grows for as long as Python has syntax; naming the ONE form
        # to accept is finite, and it is the only form the readers in this module
        # actually use. Anything else must pass the filter straight through, which is
        # accepted everywhere and needs no rebind at all.
        if not _is_the_permitted_rebind(stmt.value):
            continue
        rebind_line = stmt.lineno
        break

    # THE VALUE, not the call. A reader that calls the filter and throws the result
    # away, then searches the raw name, satisfied an earlier version of this check.
    filtered_args = {
        id(a) for sub in ast.walk(node) if feeds_the_filter(sub) for a in sub.args
    }
    return [
        sub.lineno
        for sub in ast.walk(node)
        if isinstance(sub, ast.Name)
        and sub.id == "ledger_text"
        and isinstance(sub.ctx, ast.Load)
        and id(sub) not in filtered_args
        and (rebind_line is None or sub.lineno < rebind_line)
    ]


#: Every shape this predicate must judge, with the verdict it must reach. Written as
#: data so the witness cannot drift from the rule: each is a real function this
#: repository could plausibly grow, and the two that must offend are the exact two a
#: review constructed after each earlier version of the predicate went green on them.
_READER_SHAPES = (
    ("unconditional rebind", False,
     "def f(ledger_text):\n"
     "    ledger_text = '\\n'.join(_unfenced_lines(ledger_text))\n"
     "    return ledger_text.count('x')\n"),
    ("passes it straight through", False,
     "def f(ledger_text):\n"
     "    return _table_rows(_unfenced_lines(ledger_text), None)\n"),
    ("discards the filtered value", True,
     "def f(ledger_text):\n"
     "    _unfenced_lines(ledger_text)\n"
     "    return ledger_text.count('x')\n"),
    ("reads raw before the rebind", True,
     "def f(ledger_text):\n"
     "    n = ledger_text.count('x')\n"
     "    ledger_text = '\\n'.join(_unfenced_lines(ledger_text))\n"
     "    return n\n"),
    ("rebinds only on one branch", True,
     "def f(ledger_text, enabled):\n"
     "    if enabled:\n"
     "        ledger_text = '\\n'.join(_unfenced_lines(ledger_text))\n"
     "    return ledger_text.count('x')\n"),
    ("rebinds through a ternary that keeps the raw value", True,
     "def f(ledger_text, enabled):\n"
     "    ledger_text = ('\\n'.join(_unfenced_lines(ledger_text))\n"
     "                   if enabled else ledger_text)\n"
     "    return ledger_text.count('x')\n"),
    ("rebinds through a ternary whose other path validates nothing", True,
     "def f(ledger_text, enabled):\n"
     "    ledger_text = ('\\n'.join(_unfenced_lines(ledger_text))\n"
     "                   if enabled else '')\n"
     "    return ledger_text.count('x')\n"),
    ("rebinds through a short-circuit", True,
     "def f(ledger_text):\n"
     "    ledger_text = '\\n'.join(_unfenced_lines(ledger_text)) or ''\n"
     "    return ledger_text.count('x')\n"),
    ("rebinds from a comprehension nothing draws from", True,
     "def f(ledger_text):\n"
     "    ledger_text = next(\n"
     "        (''.join(_unfenced_lines(ledger_text)) for _ in ()), '')\n"
     "    return ledger_text.count('x')\n"),
    ("rebinds from a call that merely mentions the filter", True,
     "def f(ledger_text):\n"
     "    ledger_text = _pick(_unfenced_lines(ledger_text), ledger_text)\n"
     "    return ledger_text.count('x')\n"),
)


def test_every_reader_of_the_record_reads_the_records_own_view():
    """One view of this document, derived — never a second, weaker one.

    `_unfenced_lines` decided long ago that a fenced block is an illustration and
    not part of the record. Seven functions then read the RAW text anyway, and the
    consequence was measured, not theorised: fencing a real closing report changed
    nothing, so an example heading counted as the record and a real report could
    hide from the guard that certifies it.

    Renaming the seven would be an instance patch — the eighth reader is written
    next week. The population is therefore DERIVED by parsing this module: any
    module-level function that accepts `ledger_text` must route it through
    `_unfenced_lines`. There is no exemption list, because an exemption list is the
    same hand-model one level up.
    """
    import ast

    source = Path(__file__).read_text()
    tree = ast.parse(source)

    readers, offenders = [], []
    for node in tree.body:
        if not isinstance(node, ast.FunctionDef):
            continue
        if "ledger_text" not in [a.arg for a in node.args.args]:
            continue
        if node.name == "_raw_ledger_reads":
            continue
        readers.append(node.name)
        raw_loads = _raw_ledger_reads(node)
        if raw_loads:
            offenders.append(f"{node.name} (raw at line {min(raw_loads)})")

    assert len(readers) >= 7, (
        f"only {len(readers)} readers were found, so this guard is not looking at "
        "the population it claims to cover"
    )
    assert not offenders, (
        "these functions read the ledger's raw bytes instead of the record's own "
        f"unfenced view, so a fenced illustration counts as a row: {offenders}"
    )

    # NON-VACUITY, both directions, against the predicate the loop above used.
    for label, must_offend, code in _READER_SHAPES:
        node = ast.parse(code).body[0]
        assert bool(_raw_ledger_reads(node)) is must_offend, (
            f"the predicate judged {label!r} wrongly: it must "
            f"{'refuse' if must_offend else 'accept'} that shape"
        )


def test_the_record_may_not_carry_a_fence_this_reader_cannot_read():
    """Every fence form refused; the record read as the plain lines it is.

    Four reviews produced four openers an earlier filter got wrong, in both
    directions — quoted rows read as live record, and live rows blanked out of a
    window count. Learning a fifth is the move this repository's own rule warns
    against, so the record simply may not carry one. That is a claim over a closed
    alphabet: any line opening with three or more backticks or tildes is refused,
    whatever follows it.
    """
    ledger = (
        Path(__file__).resolve().parent.parent
        / "docs/architecture/ISSUE_155_AUDIT_LEDGER.md"
    )
    lines = _unfenced_lines(ledger.read_text(encoding="utf-8"))

    # The live record reads, and reads WHOLE. Asserting that one header SURVIVED
    # would leave every other line free to vanish while the claim stayed green, so
    # the comparison is exact: what comes back is the document, line for line.
    text = ledger.read_text(encoding="utf-8")
    assert lines == text.splitlines()
    assert any("| ID |" in line for line in lines)

    for label, doc in (
        ("plain three backticks", "| a |\n```\n| ROW |\n```\n"),
        ("info string with a backtick", "| a |\n```a`b\n| ROW |\n```\n"),
        ("four backticks", "| a |\n````md\n```\n| ROW |\n```\n````\n"),
        ("tilde", "| a |\n~~~md\n| ROW |\n~~~\n"),
        ("indented opener", "| a |\n   ```\n| ROW |\n   ```\n"),
    ):
        with pytest.raises(AssertionError) as excinfo:
            _unfenced_lines(doc)
        assert "may not contain" in str(excinfo.value), label

    # And a line-initial inline code span is not a fence.
    assert _unfenced_lines("`x` is code\n") == ["`x` is code"]


def _report_disagrees_with_the_rows(ledger_text):
    """Where a closing report omits residue the slice map names as that slice's.

    STRUCTURAL, at the fourth instance of one mechanism: a hand-written summary
    disagreeing with the record it summarises. The four, in order — a residue section
    reading "None" while a deferral stood on its own row and in the slice map; a gate
    table crediting a slice with three architect evaluations when it had one; a
    residue summary omitting a `not-validated` boundary verdict; and a correction row
    calling an escalation a deferral.

    ONE of those four is mechanically checkable and it is the one implemented here.
    The slice map states each slice's residue by finding id, so a report that does not
    mention what the map names is contradicting a row in the same document, and that
    is decidable.

    THE GATE-HISTORY FAMILY IS NOT, and the attempt is recorded rather than left as a
    silent gap, because a first version of this guard shipped believing it was. It
    inferred "the checkpoint table records no L3 row for this slice, so the report may
    not credit L3 with evaluations" — and that inference is wrong twice over,
    measured on this very ledger: a loop records a checkpoint only every third
    evaluation, so one or two genuine rounds leave no row at all; and loop labels are
    not consistently slice-suffixed, so slice C's thirteen-round QA loop is recorded
    under a label the lookup never sees. Its two truthful report rows would have been
    refused. Counting evaluations out of English prose is the other half of the same
    open space this repository's rule says a checker cannot cover, so this one does
    not enter it.
    """
    import re

    ledger_text = "\n".join(_unfenced_lines(ledger_text))
    problems = []

    for slice_letter in re.findall(r"^## Slice ([A-Z]) — closing report.*$", ledger_text, re.M):
        marker = next(
            h for h in re.findall(r"^## Slice [A-Z] — closing report.*$", ledger_text, re.M)
            if h.startswith(f"## Slice {slice_letter} ")
        )
        body = ledger_text[ledger_text.index(marker):]
        body = body[: body.index("\n## ")] if "\n## " in body else body

        row = next(
            (l for l in ledger_text.splitlines() if l.startswith(f"| {slice_letter} | ")), None
        )
        if not row:
            continue
        for named in re.findall(r"`([A-Z]+-155-r\d+-\d+[a-z]?)`", row):
            # The STEM, so a report citing the revision satisfies a map naming the
            # original and the other way round: they are one finding.
            stem = named.rstrip("abcdefghijklmnopqrstuvwxyz")
            if stem not in body:
                problems.append(
                    f"slice {slice_letter}'s slice map names {named} but its closing "
                    "report never mentions it"
                )
    return problems


def test_a_closing_report_agrees_with_the_rows_it_summarises():
    """The live record, then both directions against a synthetic one."""
    ledger = (
        Path(__file__).resolve().parent.parent
        / "docs/architecture/ISSUE_155_AUDIT_LEDGER.md"
    )
    assert _report_disagrees_with_the_rows(ledger.read_text(encoding="utf-8")) == []

    hdr = ("| Loop | Evaluation (window / cumulative) | SHA (+dirty) | Outcome | Rationale |\n"
           "| --- | --- | --- | --- | --- |\n"
           "| L5 closing protocol, slice Z | 1 / 1 | `x` | `CONTINUE` | r |\n\n")
    smap = "| Z | units | kind | LANDED, residue `ARCH-155-r10-03` deferred to slice F |\n\n"

    # MUST REFUSE: the map names a deferral the report never mentions. This is the
    # defect as it actually occurred — the first slice-D report said "Residue: None".
    omits = hdr + smap + "## Slice Z — closing report\n\nResidue: none.\n"
    assert any("never mentions it" in p for p in _report_disagrees_with_the_rows(omits))

    # MUST ACCEPT: the same report naming it, and naming it by its REVISION, since a
    # revision and its original are one finding.
    for citation in ("`ARCH-155-r10-03`", "`ARCH-155-r10-03a`"):
        names = hdr + smap + f"## Slice Z — closing report\n\nResidue: {citation}, deferred.\n"
        assert _report_disagrees_with_the_rows(names) == [], citation

    # AND MUST NOT REFUSE A TRUTHFUL GATE TABLE for a loop with too few evaluations to
    # have recorded a checkpoint — the inference an earlier version made, which would
    # have refused two rows of slice C's landed report.
    truthful = hdr + smap + (
        "## Slice Z — closing report\n\n| Loop | Result |\n| --- | --- |\n"
        "| `L1` Stage-1 QA | 13 rounds |\n| `L3` architect | 1 evaluation |\n\n"
        "Residue: `ARCH-155-r10-03`, deferred.\n")
    assert _report_disagrees_with_the_rows(truthful) == []


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

#: A projection is one of these calls. This IS the runtime authority for the
#: invariant — the function that mints the contracts and grants — so naming it is
#: the pin, not a hand-model of a population.
_ROOT_PROJECTORS = frozenset({"project_grants_for_root", "_project_grants_for_root"})


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


def _table_argument(call, consumer, signatures):
    """The symbol-table argument of `call`, by KEYWORD or by position.

    Reading positions only let a `symbols=` keyword call skip the check entirely
    — the shape review found, and the one a refactor reaches for first.
    """
    for keyword in call.keywords:
        if keyword.arg in consumer:
            return keyword.value
    for params in signatures:
        for name in consumer:
            if name in params:
                index = params.index(name)
                if len(call.args) > index:
                    return call.args[index]
    return None




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
                / "docs/architecture/evidence/issue-155/captures"
                / ingested_operation_capture())
    operation_xml = (captures / "component_op_tgt.xml").read_text(encoding="utf-8")
    connection_xml = (captures / "component_connection.xml").read_text(encoding="utf-8")

    seen = []
    stack = []

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
                / "docs/architecture/evidence/issue-155/captures"
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


def test_a_summary_that_attests_nothing_is_refused(tmp_path):
    """The fail-open the derivation opened, closed and graded.

    Deriving the checks from what the archive attests is right for the values and
    silent about their existence: an archive holding only a status, a SHA and an
    exit code produced no leaves, so every loop was vacuous and the row was
    accepted. Each arm is dropped individually — dropping them all at once would
    pass on the first check and prove nothing about the others.
    """
    import json

    archive = tmp_path / "wave-gate"
    (archive / "ok").mkdir(parents=True)
    row = _wave_ledger(_ROW.format(sha="abc1234", cite=", archived `wave-gate/ok`"))

    bare = {k: _PASSING_WAVE[k] for k in ("wave_sha", "status", "verdict", "exit_code")}
    _archived_summary(archive / "ok", bare)
    assert "attests nothing about that arm" in (_wave_evidence_violation(row, archive) or "")

    for arm in _REQUIRED_WAVE_EVIDENCE:
        missing = json.loads(json.dumps(_PASSING_WAVE))
        node, leaf = missing, arm.split(".")
        for step in leaf[:-1]:
            node = node[step]
        del node[leaf[-1]]
        _archived_summary(archive / "ok", missing)
        assert _wave_evidence_violation(row, archive) == (
            f"cited archive ok carries no {arm}, so it attests nothing about that arm"
        ), arm


def test_a_count_written_as_text_is_not_a_count(tmp_path):
    """The type constraint the same derivation removed.

    `"passed": "11430"` is skipped by an `isinstance(..., int)` test, so a string
    count sailed through every value check — including the zero test, which is the
    one that would otherwise have caught a summary claiming no work.
    """
    import json

    archive = tmp_path / "wave-gate"
    (archive / "ok").mkdir(parents=True)
    row = _wave_ledger(_ROW.format(sha="abc1234", cite=", archived `wave-gate/ok`"))

    stringly = json.loads(json.dumps(_PASSING_WAVE))
    stringly["suite"]["passed"] = "11037"
    _archived_summary(archive / "ok", stringly)
    assert _wave_evidence_violation(row, archive) == (
        "cited archive ok records suite.passed as '11037', which is not a count"
    )

    # ...and the flags, from the other direction: a yes-or-no recorded as a word
    # is not a yes-or-no, and the truth test would have read any non-empty string
    # as agreement.
    wordy = json.loads(json.dumps(_PASSING_WAVE))
    wordy["goldens"]["deterministic"] = "yes"
    _archived_summary(archive / "ok", wordy)
    assert _wave_evidence_violation(row, archive) == (
        "cited archive ok records goldens.deterministic as 'yes', "
        "which is not a yes-or-no"
    )


def test_a_row_that_states_no_figure_for_a_required_arm_is_refused(tmp_path):
    """The second fail-open: a comparison that finds nothing and reads it as agreement.

    For the row's actual wording neither pattern located the fingerprint count —
    the row hyphenates and separates the words the identifier joins — so an
    archive recording three cases satisfied a row claiming two. Not because the
    figures agreed, but because the search failed and failure was silence.
    """
    import json

    archive = tmp_path / "wave-gate"
    (archive / "ok").mkdir(parents=True)
    _archived_summary(archive / "ok", _PASSING_WAVE)

    # A count spelled as a WORD is exactly the shape that used to slip through.
    worded = _ROW.replace("across 2 cases", "across two cases")
    assert _wave_evidence_violation(
        _wave_ledger(worded.format(sha="abc1234", cite=", archived `wave-gate/ok`")),
        archive
    ) == (
        "the row quotes no figure for plan_fingerprint_cases, "
        "which ok attests as 2"
    )

    # ...and the row wording that DOES state it is still read correctly, so the
    # rule refuses silence rather than refusing prose.
    assert _wave_evidence_violation(
        _wave_ledger(_ROW.format(sha="abc1234", cite=", archived `wave-gate/ok`")),
        archive
    ) is None


def test_the_wave_contract_matches_every_archived_summary():
    """The pin that keeps the declared contract from drifting out of the archive.

    A declared list is only better than a derived one while it still describes
    what is actually archived. Every PASSING wave summary in the structured form
    must carry every declared arm, so a gate that stops emitting one breaks this
    rather than quietly making the guard check less.

    Two shapes are excluded, and by a property rather than by name. A refused run
    carries no arms because it ran none — demanding them would require a failure
    to report work it never did. And the earliest passing summaries record their
    arms as PROSE (`"suite": "10659 passed, 17 skipped"`) rather than as objects;
    they predate the structured form, they are frozen evidence, and rewriting
    them to satisfy a later contract would be editing the record to fit the
    check. Their count is asserted so the exclusion stays visible instead of
    silently absorbing a new non-conforming archive.
    """
    archive = _CAPTURES.parent / "wave-gate"
    summaries = sorted(archive.glob("*/summary.json"))
    assert summaries, "no archived wave summaries, so this pin proves nothing"

    legacy, refused, checked = [], [], 0
    for path in summaries:
        attested = json.loads(path.read_text())
        # THE PRE-STRUCTURED FORM records its arms as prose and predates the
        # `verdict` field entirely, so it is recognised by either mark rather than
        # by name — a name list would need editing every time one is added, which
        # is how a silent exclusion starts.
        if isinstance(attested.get("suite"), str) or "verdict" not in attested:
            legacy.append(path.parent.name)
            continue
        if attested.get("status") != "completed" or attested.get("verdict") != "pass":
            refused.append(path.parent.name)
            continue
        if not isinstance(attested.get("suite"), dict):
            legacy.append(path.parent.name)
            continue
        checked += 1
        for arm in _REQUIRED_WAVE_EVIDENCE:
            node, missing = attested, False
            for step in arm.split("."):
                if not isinstance(node, dict) or step not in node:
                    missing = True
                    break
                node = node[step]
            assert not missing, (
                f"{path.parent.name} carries no {arm}, so the declared wave "
                f"contract no longer describes what is archived"
            )

    # EVERY SUMMARY IS ACCOUNTED FOR, and each exclusion is counted rather than
    # merely allowed: a new archive that conforms to nothing would otherwise
    # simply not be checked, which is the failure this whole guard is about.
    assert checked + len(legacy) + len(refused) == len(summaries)
    # INCLUSIONS MAY GROW, exclusions may not. Every archived round adds a
    # conforming summary, so pinning the checked count exactly would make this
    # test a chore that is edited on every pass — and an assertion edited by
    # reflex stops being read. What must not move is what this guard DECLINES to
    # look at, which is why the two exclusion counts below are exact.
    assert checked >= 37, f"only {checked} structured passing summaries checked"
    assert len(refused) == 2, f"refused runs carry no arms by right: {refused}"
    assert len(legacy) == 2, (
        f"the pre-structured summaries were {legacy}; a NEW archive in a "
        f"non-conforming shape must be a decision, not a silent exclusion"
    )


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


def _superseded_row_ids(ledger_text: str) -> frozenset:
    """Every row the SUPERSESSION MAP declares to be a revision.

    The authority, rather than a prose prefix. The first version of this guard
    tested `startswith("Revision of")` and silently missed every row spelled
    "Second revision of" — two exist — so rows its own contract excluded were
    compared and counted anyway. The map is where a revision is DECLARED, and a
    revision that is not declared there is a different defect, caught elsewhere.
    """
    import re

    # THE RECORD'S OWN VIEW, not its raw bytes. A fenced illustration is not a
    # row, and every other reader of this file already goes through
    # `_unfenced_lines`; reading raw here would let an example inside a code
    # block declare a revision that does not exist.
    unfenced = "\n".join(_unfenced_lines(ledger_text))
    # THE BOUNDED MAP BLOCK, not the whole document. A mapping-shaped string
    # quoted anywhere else — inside a finding's own rationale, say — would
    # otherwise satisfy this authority and silently exempt its left-hand row from
    # SHA validation. `tests/test_wave_gate.py` already bounds the identical scan
    # for the identical reason; this reuses that shape rather than inventing a
    # second reading of the same block.
    block = re.search(r"\*\*Supersession map\*\*(.+?)(?:\n\n|\n\*|\Z)", unfenced, re.S)
    if block is None:
        return frozenset()
    return frozenset(
        re.findall(r"`([A-Z]+-155-[A-Za-z0-9-]+) → [A-Z]+-155-[A-Za-z0-9-]+`",
                   block.group(1))
    )


def _partition_affected_sha_rows():
    """Every finding row, sorted into exactly three buckets.

    A FLOOR was the wrong shape: a fixed number lets rows disappear silently up
    to the margin, and the margin grows with every appended row. Partitioning
    cannot: each row lands in exactly one bucket, and the buckets must account
    for every row parsed, so a parser regression shows up as rows in none of
    them rather than as a count that still clears a threshold.
    """
    import re
    from pathlib import Path

    ledger_path = (Path(__file__).resolve().parents[1]
                   / "docs/architecture/ISSUE_155_AUDIT_LEDGER.md")
    raw = ledger_path.read_text(encoding="utf-8")
    revisions = _superseded_row_ids(raw)
    ledger_text = "\n".join(_unfenced_lines(raw))
    reviewed = re.compile(r"reviewed `([0-9a-f]{7,40})`")

    checked, declared_revisions, no_review, all_rows = [], [], [], []
    for line in ledger_text.splitlines():
        # NINE pipes, not ten. Ten excluded the capture rows, which carry one
        # column fewer — ten real rows the independent parse sees and this loop
        # was silently dropping, which is exactly the omission this guard claims
        # to prevent. Right-anchored indexing already lands on the SHA for all
        # three row widths in this file, so only the gate was wrong.
        if not line.startswith("| ") or line.count("|") < 9:
            continue
        parts = line.split("|")
        row_id = parts[1].strip()
        if not row_id or row_id.startswith("-") or " " in row_id:
            continue
        if row_id.startswith("DC-155-"):
            continue                      # the defect-class table, not a finding
        all_rows.append(row_id)
        if row_id in revisions:
            declared_revisions.append(row_id)
            continue
        found = reviewed.search(parts[2])
        if not found:
            no_review.append(row_id)
            continue
        sha_cell = parts[-3].strip()
        if sha_cell != _TIP_FORM and not re.search(r"`[0-9a-f]{7,40}`", sha_cell):
            raise AssertionError(
                f"{row_id}: affected-SHA cell is neither the exact tip form nor a "
                f"backticked commit, so nothing can be checked about it: {sha_cell!r}"
            )
        checked.append((row_id, found.group(1), sha_cell))
    return all_rows, checked, declared_revisions, no_review


def test_the_affected_sha_cell_names_the_tree_the_defect_affected():
    """The column is the AFFECTED sha — the tree with the defect, not the fix.

    Measured across this ledger: of the non-revision rows that cite a review and
    name a literal SHA, every one names the reviewed SHA. That follows from the
    column's own name — a finding affects the tree it was found in, and the
    disposition beside it says what happened afterwards.

    The guard exists because I broke that convention twice while "correcting" it
    toward a reviewer's reading, and its first version encoded the inverted rule
    and flagged hundreds of conforming rows — which is how the convention was
    measured rather than assumed.
    """
    import re
    from pathlib import Path

    all_rows, checked, revisions, no_review = _partition_affected_sha_rows()
    assert all_rows, "no ledger rows parsed; this guard would be inert"

    # INDEPENDENTLY DERIVED, by a different parse of the same file. The previous
    # version compared the buckets against a total the same loop had built, which
    # is an identity: every accepted row was appended to the total and then to
    # exactly one bucket, and a row rejected earlier shrank both sides together.
    # It could not fail, so it could not detect the omission it was written to
    # detect — the same vacuity this guard exists to prevent, inside the guard.
    ledger = (Path(__file__).resolve().parents[1]
              / "docs/architecture/ISSUE_155_AUDIT_LEDGER.md").read_text(encoding="utf-8")
    independent = {rid for rid in re.findall(
        r"^\| ([A-Z]+-155-[A-Za-z0-9-]+) \|",
        "\n".join(_unfenced_lines(ledger)), re.M)
        if not rid.startswith("DC-155-")}   # the class table is not a finding table
    assert independent, "the independent parse found nothing; it cannot witness anything"
    missing = independent - set(all_rows)
    assert not missing, (
        f"{len(missing)} finding row(s) the independent parse sees never reached the "
        f"partition, so they escape SHA validation entirely: {sorted(missing)[:6]}"
    )
    assert len(checked) + len(revisions) + len(no_review) == len(all_rows)
    assert checked, "no rows reached the comparison; the guard would be inert"

    deviating = [(rid, sha, cell) for rid, sha, cell in checked
                 if affected_sha_cell_deviates(sha, cell)]
    assert not deviating, (
        "these rows name a SHA cell that does not include the tree their own gate "
        "cell says was reviewed, against the convention every other row follows: "
        + "; ".join(f"{r} (reviewed {s}) -> {c}" for r, s, c in deviating)
    )


def test_every_declared_revision_is_excluded_by_the_declaring_authority():
    """The exclusion comes from the supersession map, not from prose.

    Two rows are spelled "Second revision of", which a prefix test misses. They
    must still be excluded, because a revision inherits its affected SHA from the
    finding it revises while its gate cell names the later review that prompted
    it — comparing those is a category error.
    """
    _all, checked, revisions, _none = _partition_affected_sha_rows()
    assert {"CDX-155-r110-01b", "EVAL-155-13b"} <= set(revisions), (
        "the ordinal-spelled revisions are not excluded, so the authority is "
        "still being approximated by prose"
    )
    assert not ({"CDX-155-r110-01b", "EVAL-155-13b"} & {r for r, _s, _c in checked})


def test_the_affected_sha_checker_is_not_vacuous():
    """Drives the SAME checker the ledger node uses, on both verdicts."""
    reviewed = "323151f"
    assert not affected_sha_cell_deviates(reviewed, "`323151f`")
    assert not affected_sha_cell_deviates(reviewed, _TIP_FORM)
    assert not affected_sha_cell_deviates(reviewed, "`c31a2f0`..`323151f`")
    assert affected_sha_cell_deviates(reviewed, "`59af3e1`..`5d169f9`")
    assert affected_sha_cell_deviates(reviewed, "on the tip"), (
        "an inexact tip marker must not be waved through as the tip form"
    )
    assert affected_sha_cell_deviates(reviewed, "")


def test_the_affected_sha_guard_is_not_vacuous():
    """The ledger-side half of the same non-vacuity claim.

    Two nodes, deliberately: the checker-side node above drives the comparison
    directly on synthetic cells, and this one proves the guard is exercised
    against the REAL record — a checker that is correct on invented inputs and
    never reaches the ledger would satisfy the first alone.

    The two names both exist because the manifest records both identities, and a
    node identity is not something to rename away once recorded: a row that names
    a test nothing collects is the defect the manifest exists to surface.
    """
    all_rows, checked, revisions, no_review = _partition_affected_sha_rows()
    assert checked, "no ledger row reached the comparison, so the guard is inert"
    # The partition must be a real division of a real population, not three
    # empty buckets agreeing with an empty total.
    assert len(all_rows) > 500, len(all_rows)
    assert revisions, "no revision was excluded, so the exclusion path is untested"
    assert affected_sha_cell_deviates("323151f", "`59af3e1`..`5d169f9`"), (
        "the checker driven here does not reject a cell naming neither tree"
    )


def test_a_wave_row_is_checked_whatever_its_scope_is_called(tmp_path):
    """No spelling of the scope may drop the latest row out of the wave rule.

    THE NON-VACUITY WITNESS for removing the scope whitelist. The pattern listed
    `slice [A-Z]|issue-level`, and this ledger's own `ISSUE-level correction arc` row
    matched neither — so `rows[-1]` selected an OLDER row, the guard verified evidence
    closure did not rest on, and a latest row citing an archive that does not exist
    returned no violation at all. The construction below is exactly that: a passing
    row with real evidence, followed by a later row under a scope name the whitelist
    never listed, citing nothing that exists. The old pattern returns None here.
    """
    archive = tmp_path / "wave-gate"
    _archived_summary(archive / "ok", _PASSING_WAVE)
    earlier = _ROW.format(sha="abc1234", cite=", archived `wave-gate/ok`")

    for scope in ("ISSUE-level correction arc", "the closing arc", "wave 2"):
        later = earlier.replace("L4 composite wave gate, slice B",
                                f"L4 composite wave gate, {scope}", 1)
        later = later.replace("wave-gate/ok", "wave-gate/never-archived")
        assert _wave_evidence_violation(_wave_ledger(earlier, later), archive) == (
            "cited archive never-archived is absent from the checkout"), scope


def test_a_wave_summary_nothing_produced_is_not_evidence(tmp_path):
    """The summary must be an OUTPUT of the archived round, not a file beside it.

    THE NON-VACUITY WITNESS for the binding rule. Every value check in the wave rule
    reads `summary.json`; none of them could tell a summary the archiver wrote from
    one hand-placed into the directory afterwards — which is the shape that actually
    occurred, with `round.json` listing only the log and deriving a null verdict while
    the guard beside it read a confident `pass`. Each case below carries a perfectly
    valid summary and differs only in what the round record says about it.
    """
    import hashlib
    import json

    row = _wave_ledger(_ROW.format(sha="abc1234", cite=", archived `wave-gate/ok`"))
    body = json.dumps(_PASSING_WAVE)

    def archive_with(record):
        root = tmp_path / record["case"] / "wave-gate"
        (root / "ok").mkdir(parents=True)
        (root / "ok" / "summary.json").write_text(body)
        if record.get("round") is not None:
            (root / "ok" / "round.json").write_text(record["round"])
        return root

    digest = hashlib.sha256(body.encode()).hexdigest()
    bound = {"files": {"wave-gate/ok/summary.json": digest}, "verdict": "pass",
             "status": "completed", "durable_dir": "wave-gate/ok",
             "wave_sha": _PASSING_WAVE["wave_sha"],
             "logical_loop": "L4 (composite wave gate, slice B)"}

    # No record at all — the directory exists and the summary reads as a pass.
    assert _wave_evidence_violation(row, archive_with(
        {"case": "no-record", "round": None})) == "cited archive ok carries no round record"
    assert _wave_evidence_violation(row, archive_with(
        {"case": "unreadable", "round": "{not json"})) == (
        "cited archive ok has an unreadable round record")
    # THE OBSERVED SHAPE: the round lists only its log and derives no verdict, while
    # the summary beside it says `pass`.
    assert _wave_evidence_violation(row, archive_with({
        "case": "unbound",
        "round": json.dumps({**bound, "files": {"wave-gate/ok/wave.log": digest},
                             "verdict": None}),
    })) == ("cited archive ok does not bind its summary — the round record lists no "
            "wave-gate/ok/summary.json")
    # Bound, but to different bytes than the checkout holds.
    assert _wave_evidence_violation(row, archive_with({
        "case": "stale", "round": json.dumps({**bound, "files": {
            "wave-gate/ok/summary.json": "0" * 64}}),
    })) == ("cited archive ok binds a summary.json whose digest does not match the "
            "file in the checkout")
    # Bound to the right bytes, but the round itself did not derive a pass.
    assert _wave_evidence_violation(row, archive_with({
        "case": "not-a-pass", "round": json.dumps({**bound, "verdict": None}),
    })) == "cited archive ok records the round verdict None, not 'pass'"

    # A round the archiver was TOLD was failed, carrying a summary that still reads
    # `pass` — the `--status` override makes this constructible, so the record's own
    # status is checked and not inferred from the summary beside it.
    assert _wave_evidence_violation(row, archive_with({
        "case": "failed-round", "round": json.dumps({**bound, "status": "failed"}),
    })) == "cited archive ok records the round status 'failed', not 'completed'"

    # A record for a DIFFERENT round, listing this round's summary under its own path.
    assert _wave_evidence_violation(row, archive_with({
        "case": "foreign", "round": json.dumps({**bound, "durable_dir": "wave-gate/other"}),
    })) == "cited archive ok carries a round record for 'wave-gate/other'"

    # A record and a summary describing DIFFERENT trees.
    assert _wave_evidence_violation(row, archive_with({
        "case": "split-tree", "round": json.dumps({**bound, "wave_sha": "9" * 40}),
    })) == ("cited archive ok records a round on '9999999' beside a summary for "
            "'abc1234'")
    # The control: the same summary, bound the way the archiver binds it.
    assert _wave_evidence_violation(row, archive_with(
        {"case": "ok", "round": json.dumps(bound)})) is None


def test_a_loop_scope_this_rule_cannot_read_is_refused(tmp_path):
    """An unrecognised scope is a violation, never a value that compares equal.

    THE NON-VACUITY WITNESS for the fail-open inside an equality test: two scopes the
    extractor could not read both became "no scope" and matched each other, so a
    checkpoint under one spelling could be satisfied by a round archived under a
    different one entirely.
    """
    archive = tmp_path / "wave-gate"
    _archived_summary(archive / "ok", _PASSING_WAVE,
                      logical_loop="L4 (composite wave gate, wave two)")
    unreadable = _ROW.format(sha="abc1234", cite=", archived `wave-gate/ok`").replace(
        "L4 composite wave gate, slice B", "L4 composite wave gate, the closing arc", 1)
    assert _wave_evidence_violation(_wave_ledger(unreadable), archive) == (
        "the checkpoint row names a loop scope this rule cannot read: "
        "'L4 composite wave gate, the closing arc'")

    # A readable row against an unreadable ROUND is refused on the other side.
    readable = _ROW.format(sha="abc1234", cite=", archived `wave-gate/ok`")
    assert _wave_evidence_violation(_wave_ledger(readable), archive) == (
        "the cited archive names a loop scope this rule cannot read: "
        "'L4 (composite wave gate, wave two)'")


def test_a_loop_identity_that_is_not_a_string_is_malformed_evidence(tmp_path):
    """Stringifying a producer's field is trusting it, not validating it.

    THE NON-VACUITY WITNESS: an object rendered to text still CONTAINS the scope name,
    so a malformed record matched a well-formed checkpoint. The rule reads archived
    evidence, so a field of the wrong type is a defect in that evidence.
    """
    import hashlib
    import json

    archive = tmp_path / "wave-gate"
    (archive / "ok").mkdir(parents=True)
    body = json.dumps(_PASSING_WAVE)
    (archive / "ok" / "summary.json").write_text(body)
    (archive / "ok" / "round.json").write_text(json.dumps({
        "files": {"wave-gate/ok/summary.json": hashlib.sha256(body.encode()).hexdigest()},
        "verdict": "pass", "status": "completed", "durable_dir": "wave-gate/ok",
        "wave_sha": _PASSING_WAVE["wave_sha"],
        # Renders as a string containing `slice B`, which is exactly why it used to pass.
        "logical_loop": {"scope": "slice B"},
    }))
    row = _wave_ledger(_ROW.format(sha="abc1234", cite=", archived `wave-gate/ok`"))
    assert _wave_evidence_violation(row, archive) == (
        "cited archive ok records a loop identity that is not a string: dict")


def test_an_issue_level_loop_owes_its_checkpoints_like_any_other(tmp_path):
    """A loop whose scope is not a slice letter is still a loop.

    THE NON-VACUITY WITNESS for billing issue-level loops. The parser required
    `slice ([A-F])` on both sides, so an issue-level loop was billed zero evaluations,
    owed no interval, and its checkpoint rows could be deleted with the audit still
    green — the rule policing loop identity could not see one of the two scopes this
    record uses. The archive below holds three completed issue-level rounds and the
    ledger answers for none of them; under the old parser this returned None.
    """
    import json

    root = tmp_path / "wave-gate"
    root.mkdir()
    for i in range(3):
        d = root / f"wave{i}"
        d.mkdir()
        (d / "round.json").write_text(json.dumps({
            "logical_loop": "L4 (composite wave gate, issue-level)", "status": "completed"}))

    header = ("| Loop | Evaluation (window / cumulative) | SHA (+dirty) | Outcome | Rationale |\n"
              "| --- | --- | --- | --- | --- |\n")
    unanswered = header + "| L4 composite wave gate, slice D | 3 / 3 | x | `CONTINUE` | r |\n"
    assert _missing_checkpoint_violation(unanswered, "", wave_dir=root) == (
        "loop L4 of issue-level has 3 collected evaluations, so a recorded decision "
        "through 3 is owed; the record covers 0")

    # A slice row cannot answer for an issue-level loop, and vice versa: the scope is
    # half of the loop identity, not decoration on it.
    answered = header + "| L4 composite wave gate, issue-level | 3 / 3 | x | `CONTINUE` | r |\n"
    assert _missing_checkpoint_violation(answered, "", wave_dir=root) is None

    # AND THE OTHER DIRECTION, so the extractor cannot be satisfied by matching
    # everything: an issue-level row does not answer for a slice's loop.
    for d in root.iterdir():
        (d / "round.json").write_text(json.dumps({
            "logical_loop": "L4 (composite wave gate, slice E)", "status": "completed"}))
    assert _missing_checkpoint_violation(answered, "", wave_dir=root) == (
        "loop L4 of slice E has 3 collected evaluations, so a recorded decision "
        "through 3 is owed; the record covers 0")


def _tier_derivation_offenders(ledger_text):
    """Rows whose EFFECTIVE derived tier contradicts their own source label.

    The severity rule has two disjuncts, and the second is unconditional: a finding is
    critical if it lands in a critical blocking class, OR if its source gate labelled
    it P0/P1/Critical/High. Reading only the first disjunct — treating the class as
    the whole rule — puts `standard` beside a `P1` label, and the record's checkpoint
    trends and its no-critical-residue claim are computed from that cell.

    This was found once and corrected across eight rows; the correction was applied to
    the instances and no invariant was written, so the misreading recurred three more
    times. The rule is a hand-model of published policy, and its authority is the
    policy text, so the model is replaced by a derivation over every row.

    SUPERSEDED ROWS ARE EXEMPT BY CONSTRUCTION, not by an exemption list: a frozen row
    states what was believed when it was written, and correcting one means appending a
    revision. Only the LATEST revision of an id is effective, and only it is checked.
    """
    import re

    #: The labels the rule names, matched as whole words so a summary that merely
    #: contains the word "high" is not a severity label.
    critical_labels = re.compile(r"\b(P0|P1|Critical|High)\b", re.I)
    #: THE RULE'S OTHER DISJUNCT, which this guard did not read. The severity rule makes
    #: a finding critical if its SOURCE LABEL is P0/P1/Critical/High **or** if it lands
    #: in one of three blocking classes — and making only the label half executable left
    #: the class half exactly as hand-checked as it had always been, which is how
    #: nineteen standing rows came to sit in a critical-anchor class while deriving
    #: standard. A rule with two disjuncts is not enforced by a guard that reads one.
    #: The three classes the rule names, matched against the cell's LEADING value(s)
    #: rather than anywhere in it. An unanchored search read a row whose class reads
    #: "runtime behavior (CORRECTED from mutation accounting)" as mutation accounting —
    #: promoting a row on the strength of the note recording that it is NOT that class.
    CRITICAL_CLASSES = {"secrets/security", "data loss", "mutation accounting"}

    def critical_class(cell):
        head = re.split(r"[(—]", cell, 1)[0]
        # The record separates compound classes with a middle dot, which is the
        # separator the project's own class list uses; splitting only on the ASCII
        # punctuation left the second half of such a cell unread.
        return any(part.strip().lower() in CRITICAL_CLASSES
                   for part in re.split(r"[;,·]", head))

    #: A revision may say its class is UNCHANGED — an established convention in this
    #: record, used by eight standing rows. Reading the terminal cell literally then
    #: discards an ancestor's class, and an ancestor of one of those rows is classed
    #: `data loss`: a critical anchor silently dropped by the rule meant to enforce it.
    #: The effective class is the last one any row in the chain actually STATES.
    UNCHANGED = re.compile(r"^(unchanged|as above|same)\b", re.I)

    def effective_class(chain):
        for cells in reversed(chain):
            cell = cells[5].strip()
            if cell and not UNCHANGED.match(cell):
                return cell
        return ""
    #: A tier the row itself lowered THROUGH the sanctioned path. A severity-specific
    #: technical refutation is the one way a source-critical label becomes standard,
    #: and it is recorded as the disposition, so it is read from there rather than
    #: inferred from prose.
    #: THE RECORD'S OWN VOCABULARY for the one sanctioned way down, and only it. The
    #: rule permits a source-critical label to derive standard on a documented
    #: severity-specific technical refutation, so the marker is read rather than
    #: inferred: matching the ARGUMENT instead would make any sufficiently confident
    #: sentence a refutation, which is the enumeration defect wearing prose.
    refuted = re.compile(
        r"\bseverity-refuted\b|\bfinding-refuted\b|\bseverity refutation\b", re.I)

    # The record's own view — see `_unfenced_lines`. A fenced block is an
    # illustration, not a row.
    ledger_text = "\n".join(_unfenced_lines(ledger_text))

    # ROWS COME FROM THE SHARED PARSER. Three separate hand-models sat here — a local
    # id regex, hand-derived column indices, and a fallback for rows with the wrong
    # column count — and `_finding_table_rows` already owns all three, importing the
    # identifier grammar rather than restating it. Its own docstring records what a
    # local copy cost the last time: thirteen live rows fell into the gap between the
    # copy and the authority. A bolded id, which that parser normalises and a local
    # regex does not, would have been the next row to vanish from this rule.
    import sys as _sys

    _sys.path.insert(0, str(Path(__file__).resolve().parent))
    from test_wave_gate import _FINDING_ID_RE

    rows_list, malformed = _finding_table_rows(ledger_text)
    rows = dict(rows_list)

    #: THE COLUMN ORDER THIS RULE READS BY POSITION, asserted rather than assumed.
    #: The shared parser validates the column COUNT and not the order, so a reordered
    #: nine-column table still parses and these offsets would then read the blocking
    #: class as the source label — the severity rule silently reading the wrong cell.
    #: Checking the header turns that misread into a refusal.
    expected = ("ID", "Source gate", "Verbatim summary", "Original label",
                "Blocking class", "Defect class", "Derived tier", "SHA/delta",
                "Disposition")
    # EVERY table the row parser walks, not just the first one it finds. That parser
    # reads all ID-headed tables, so validating one header left any later table free to
    # order its columns differently while these offsets kept reading position four as
    # the source label.
    for head in (l for l in _unfenced_lines(ledger_text)
                 if l.startswith("|") and l.split("|")[1].strip() == "ID"):
        got = [c.strip() for c in head.split("|")[1:-1]]
        if len(got) != len(expected) or not all(
                g.startswith(e) for g, e in zip(got, expected)):
            return [("<the finding table's columns are not in the order this rule "
                     "reads>", " | ".join(expected), " | ".join(got))]

    # FAIL CLOSED ON AN EMPTY READ. Returning no offenders when no row was found meant
    # a renamed or recased table header silently disarmed the rule while its own test
    # went green — a guard that passes hardest when it can see least, which is the
    # exact shape this file has recorded against other guards four times over.
    if not rows:
        return [("<the finding table could not be read>", "", "")]

    # THE EFFECTIVE ROW comes from the DECLARED supersession chain, not from the shape
    # of the id. Ranking by suffix length let `a` and `b` tie, so whichever appeared
    # first in the file won: `ARCH-155-r10-03a` was selected over the `b` that
    # supersedes it and moves its tier, and `CDX-155-r197-09a` over `09d`. The record
    # publishes which row supersedes which; reading the id was a second model of a
    # fact the document already states.
    superseded = {}
    for line in _unfenced_lines(ledger_text):
        if not line.startswith("**Supersession map**"):
            continue
        # The map writes each pair inside ONE backtick span — `newer → older` — not a
        # span per id.
        # THE SHARED IDENTIFIER GRAMMAR, imported and never restated — the rule this
        # file keeps a structural guard for. A local `[A-Z]+` prefix was narrower than
        # the authority, which accepts `[A-Z][A-Za-z0-9]*`: a correction whose id used
        # a mixed-case prefix would be seen by the row parser, missed by this one, and
        # would then fail to supersede its own predecessor — a FALSE failure from the
        # same drift that produced a false pass two rounds earlier.
        for newer, older in re.findall(
                rf"`({_FINDING_ID_RE}) → ({_FINDING_ID_RE})`", line):
            superseded[older] = newer

    # A row the parser could not place is reported by THIS rule only when it could be
    # hiding from it — that is, when some cell is a severity label standing alone.
    # Column-count damage as such is already owned by the malformed-row guard, and
    # reporting every such row here would restate that rule and bury this one's own
    # findings under eleven pre-existing rows it has nothing to say about.
    def standing(rid):
        """The row that finally stands for `rid`, following the WHOLE chain.

        Checking only the immediate successor stopped one link in: a chain A → B → C
        left A reported whenever B was itself unusable, and — the worse direction — a
        malformed A could be laundered by a successor carrying a lesser label, because
        the successor was consulted for its existence and never for what it said. The
        chain is walked to its end, with a seen-set so a cyclic map cannot hang the
        rule, and `None` means nothing standing was found.
        """
        seen, here = set(), rid
        while here in superseded and here not in seen:
            seen.add(here)
            here = superseded[here]
        return here if here in rows else None

    # SUPERSESSION APPLIES TO MALFORMED ROWS TOO, and to the whole chain. A malformed
    # historical row corrected by a valid revision was reported before the map was
    # consulted at all, so the record could not be made green by the one move the
    # workflow allows — retain the original, append the correction. A rule that cannot
    # be satisfied by the sanctioned repair is a rule that gets worked around.
    #
    # THE SOURCE LABEL IS THE PREDECESSOR'S, ALWAYS. A raw source label is immutable,
    # so a chain is graded on the label the ORIGINAL row carries and the tier the
    # STANDING row derives. Reading both from the successor let a `P1` row be laundered
    # by a successor that simply called itself `P2`, which is the immutability rule
    # being deleted by the mechanism written to honour it.
    offenders = []
    for rid, cells in malformed:
        if not any(re.fullmatch(r"(P0|P1|Critical|High)", c.strip(), re.I)
                   for c in cells):
            continue
        end = standing(rid)
        if end is None:
            offenders.append((rid, "<malformed row carrying a severity label>",
                              " ".join(c.strip() for c in cells)[:60]))
            continue
        label = next(c.strip() for c in cells
                     if re.fullmatch(r"(P0|P1|Critical|High)", c.strip(), re.I))
        tier, disposition = rows[end][7].strip(), rows[end][9].strip()
        if not (refuted.search(disposition) or refuted.search(tier)):
            token = re.match(r"[*_\s]*(critical|standard|n/a)\b", tier, re.I)
            if token is None or token.group(1).lower() != "critical":
                offenders.append((end, label, tier[:60]))
    for rid in sorted(rows):
        # SUPPRESSED ONLY BY A ROW THAT IS ACTUALLY THERE, at the END of the chain.
        # The map named an older id and this hid it without checking the newer one had
        # been parsed, so a revision the parser never saw would silently exempt its own
        # original and neither row would be graded.
        end = standing(rid) if rid in superseded else None
        if end is not None:
            # The predecessor is answered for by the row that stands, and that row is
            # graded against THIS row's source label, because a raw label is immutable
            # and a successor cannot lower it by simply writing a smaller one.
            cells, stand = rows[rid], rows[end]
            # THE LABEL FROM THE ORIGINAL, THE CLASS FROM THE ROW THAT STANDS. A raw
            # source label is immutable, so a successor cannot lower it; a blocking
            # CLASS is a derived assignment the record may correct, so reading it from
            # the predecessor made a corrected-away class permanent and promoted rows
            # whose effective class is no longer critical at all.
            # THE LABEL FROM THE ORIGINAL, THE CLASS FROM THE CHAIN. A raw source
            # label is immutable; a blocking class is a derived assignment the record
            # may correct, and a revision may decline to restate it.
            walk, here = [], rid
            seen = set()
            while here and here not in seen:
                seen.add(here)
                if here in rows:
                    walk.append(rows[here])
                here = superseded.get(here)
            label, blocking = cells[4].strip(), effective_class(walk)
            tier, disposition = stand[7].strip(), stand[9].strip()
        else:
            cells = rows[rid]
            # The parser guarantees the column count, so these positions are the
            # table's own.
            label, blocking = cells[4].strip(), cells[5].strip()
            tier, disposition = cells[7].strip(), cells[9].strip()
        # EITHER DISJUNCT of the severity rule brings a row under it.
        if not (critical_labels.search(label) or critical_class(blocking)):
            continue
        if refuted.search(disposition) or refuted.search(tier):
            continue
        # THE LEADING TOKEN, not a substring. Every tier cell states its tier first
        # and then argues for it, and the argument routinely NAMES the other tiers —
        # the historical cells literally read "not P0/P1/Critical/High" — so a
        # substring test finds the word `critical` in exactly the rows that got the
        # derivation wrong. Unreadable is an offence, not an exemption: a tier nobody
        # can parse cannot support the record's no-critical-residue claim.
        token = re.match(r"[*_\s]*(critical|standard|n/a)\b", tier, re.I)
        if token is None or token.group(1).lower() != "critical":
            # The STANDING row is what the record is asked to correct, so a chain is
            # named once by its end rather than once per link — otherwise a predecessor
            # and its own successor are reported as two separate failures of one row.
            offenders.append((end or rid, label[:40], tier[:60]))

    # ONE ENTRY PER ROW THE RECORD MUST CHANGE, first mention kept: a chain reaches
    # this list once through its predecessor and again on its own account, and the
    # same id twice reads as two defects where the record has one.
    seen, unique = set(), []
    for entry in offenders:
        if entry[0] in seen:
            continue
        seen.add(entry[0])
        unique.append(entry)
    return unique


def test_a_source_label_derives_its_tier_and_is_not_read_around():
    """A P0/P1/Critical/High source label derives Critical, whatever the class says.

    THE STRUCTURAL FIX for a misreading this record made eleven times across four
    rounds. Every earlier correction rewrote the offending cells; none of them made
    the rule checkable, so the twelfth was only a matter of time. The record's
    no-critical-residue claim is derived from this cell, so a cell that can drift from
    the policy is a claim that can be false while every gate is green.
    """
    ledger = Path(__file__).resolve().parents[1] / (
        "docs/architecture/ISSUE_155_AUDIT_LEDGER.md")
    offenders = _tier_derivation_offenders(ledger.read_text())
    assert not offenders, (
        "these rows carry a source label the severity rule makes Critical while "
        "deriving a lesser tier, and record no severity refutation: "
        + "; ".join(f"{r} (label {l!r} -> tier {t!r})" for r, l, t in offenders)
    )


def test_the_tier_derivation_can_actually_fail():
    """THE NON-VACUITY WITNESS: the rule above must refuse the shape it exists for.

    A guard over a record that already satisfies it proves nothing — this file has
    recorded that failure repeatedly — so the historical shapes are constructed here
    and must be refused, together with every way a row legitimately carries a lesser
    tier. The later arms pin the two holes a review found in the first version of this
    rule: an id-shaped supersession ranking that could not order `a` against `b`, and
    a hand-listed set of id prefixes that omitted a prefix the record actually uses.
    """
    HEAD = (
        "| ID | Source gate + run dir + attestation | Verbatim summary | "
        "Original label | Blocking class | Defect class | "
        "Derived tier (anchor inline) | SHA/delta | Disposition |\n"
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |\n"
    )

    def row(rid, label, tier, disposition):
        return (f"| {rid} | gate | s | {label} | machine-served schemas/contracts | "
                f"DC-155-C | {tier} | `abc1234` | {disposition} |")

    def ledger(*rows, supersessions=()):
        body = HEAD + "\n".join(rows) + "\n"
        if supersessions:
            body += "\n**Supersession map** — " + ", ".join(
                f"`{n} → {o}`" for n, o in supersessions) + "\n"
        return body

    def offenders(*args, **kw):
        return [r for r, _, _ in _tier_derivation_offenders(ledger(*args, **kw))]

    # THE HISTORICAL SHAPE, verbatim in structure: a P1 label, a non-critical class,
    # and a tier that read the class as though it were the whole rule.
    assert offenders(row(
        "CDX-155-r1-01", "P1",
        "standard — anchor: source gate labelled it P1, not P0/P1/Critical/High",
        "fixed")) == ["CDX-155-r1-01"]

    for label in ("P0", "P1", "Critical", "High"):
        assert offenders(row("CDX-155-r1-01", label, "standard", "fixed")), label

    # The three legitimate shapes, none of which may be reported.
    assert not offenders(row("CDX-155-r1-01", "P1", "**critical** — anchor", "fixed"))
    assert not offenders(row("CDX-155-r1-01", "P2", "standard — anchor", "fixed"))
    assert not offenders(row(
        "CDX-155-r1-01", "P1", "standard — lowered on a documented severity refutation",
        "severity-refuted: the reviewer's severity premise was measured false"))

    # SUPERSESSION IS READ FROM THE MAP. `a` and `b` are the same length, so an
    # id-shaped ranking cannot order them and took whichever came first: here the
    # standing `b` corrects the tier and the superseded `a` does not, and reading the
    # ids instead of the map reports the row the record has already withdrawn.
    chain = (row("CDX-155-r1-01", "P1", "standard — original", "fixed"),
             row("CDX-155-r1-01a", "P1", "standard — first revision", "fixed"),
             row("CDX-155-r1-01b", "P1", "**critical** — corrected", "fixed"))
    superseded = (("CDX-155-r1-01b", "CDX-155-r1-01a"),
                  ("CDX-155-r1-01a", "CDX-155-r1-01"))
    assert not offenders(*chain, supersessions=superseded)
    # ...and the same chain with the STANDING row uncorrected is still caught, so the
    # map cannot be used to excuse a row rather than to locate it.
    broken = chain[:2] + (row("CDX-155-r1-01b", "P1", "standard — still wrong", "fixed"),)
    assert offenders(*broken, supersessions=superseded) == ["CDX-155-r1-01b"]

    # EVERY PREFIX THE RECORD USES, because membership decides and no list does. The
    # prefix list that this replaced admitted five and the record already had six.
    for prefix in ("QA", "CDX", "ARCH", "SELF", "EVAL", "WAVE"):
        rid = f"{prefix}-155-r1-01"
        assert offenders(row(rid, "P1", "standard", "fixed")) == [rid], prefix

    # A tier nobody can parse is an offence, not an exemption.
    assert offenders(row("CDX-155-r1-01", "P1", "see the note below", "fixed"))

    # THE CLASS DISJUNCT: a critical blocking class promotes a row whatever its label.
    def classed(rid, label, cls, tier, disposition="fixed"):
        return (f"| {rid} | gate | s | {label} | {cls} | DC-155-C | {tier} | `abc1234` | "
                f"{disposition} |")

    def offenders_of(*rows, supersessions=()):
        body = HEAD + "\n".join(rows) + "\n"
        if supersessions:
            body += "\n**Supersession map** — " + ", ".join(
                f"`{n} → {o}`" for n, o in supersessions) + "\n"
        return [r for r, _, _ in _tier_derivation_offenders(body)]

    for cls in ("mutation accounting", "data loss", "secrets/security"):
        assert offenders_of(classed("CDX-155-r7-01", "Low", cls, "standard")) == [
            "CDX-155-r7-01"], cls
    # A COMPOUND class separated by the middle dot the project's own list uses.
    assert offenders_of(classed(
        "CDX-155-r7-01", "Low", "runtime behavior · data loss", "standard")) == [
        "CDX-155-r7-01"]
    # The note recording that a class was corrected AWAY is not that class.
    assert not offenders_of(classed(
        "CDX-155-r7-01", "Low", "runtime behavior (CORRECTED from mutation accounting)",
        "standard"))
    # A REVISION MAY DECLINE TO RESTATE ITS CLASS, and the ancestor's still governs.
    assert offenders_of(
        classed("CDX-155-r7-01", "Low", "data loss", "standard"),
        classed("CDX-155-r7-01a", "Low", "unchanged", "standard"),
        supersessions=(("CDX-155-r7-01a", "CDX-155-r7-01"),)) == ["CDX-155-r7-01a"]
    # ...and a class the chain CORRECTS away is not resurrected from the predecessor.
    assert not offenders_of(
        classed("CDX-155-r7-01", "P2", "mutation accounting", "standard"),
        classed("CDX-155-r7-01a", "P2", "none — reclassified on measurement", "standard"),
        supersessions=(("CDX-155-r7-01a", "CDX-155-r7-01"),))

    # And the defect-class table is not graded as though it were a severity cell —
    # tested BESIDE a real finding table, because a class table on its own now fails
    # closed and would pass this arm for the wrong reason.
    CLASS_TABLE = (
        "\n| Class | Mechanism | Authority | Instances | Rule |\n"
        "| --- | --- | --- | --- | --- |\n"
        "| DC-155-C | a hand-listed enumeration | the annotations | 2 "
        "(`CDX-155-r1-01`) | High confidence, Critical to fix |\n"
    )
    assert not _tier_derivation_offenders(
        ledger(row("CDX-155-r1-01", "P2", "standard — anchor", "fixed")) + CLASS_TABLE)

    # FAIL CLOSED WHEN THE TABLE CANNOT BE READ. A renamed or recased header used to
    # disarm the rule silently, and its test passed having graded nothing at all.
    for text in ("", "no tables here at all\n", CLASS_TABLE):
        assert _tier_derivation_offenders(text) == [
            ("<the finding table could not be read>", "", "")], repr(text[:24])

    # A BOLDED ID is one the shared parser normalises and a local regex did not. The
    # standing revision below is bolded: if it were invisible to the rule, the map
    # would still hide its original and neither row would be graded.
    bolded = (row("CDX-155-r1-01", "P1", "standard — original", "fixed"),
              row("**CDX-155-r1-01a**", "P1", "standard — still wrong", "fixed"))
    assert offenders(*bolded, supersessions=(("CDX-155-r1-01a", "CDX-155-r1-01"),)) == [
        "CDX-155-r1-01a"]

    # AND A MAP ENTRY POINTING AT A ROW THAT IS NOT THERE hides nothing: the original
    # is graded, rather than exempted by a revision no reader can find.
    assert offenders(row("CDX-155-r1-01", "P1", "standard", "fixed"),
                     supersessions=(("CDX-155-r1-01a", "CDX-155-r1-01"),)) == [
        "CDX-155-r1-01"]

    # A REORDERED TABLE IS REFUSED, not read by position anyway. The shared parser
    # validates the column COUNT and not the order, so nine columns in the wrong order
    # still parse and these offsets would read the blocking class as the source label.
    swapped = ledger(row("CDX-155-r1-01", "P1", "standard", "fixed")).replace(
        "Original label | Blocking class", "Blocking class | Original label", 1)
    assert [r for r, _, _ in _tier_derivation_offenders(swapped)] == [
        "<the finding table's columns are not in the order this rule reads>"]

    # A MIXED-CASE PREFIX is accepted by the shared identifier grammar, so the map must
    # accept it too: a correction the row parser sees and the map parser does not would
    # fail to supersede its own predecessor and produce a FALSE failure.
    mixed = (row("Qa-155-r1-01", "P1", "standard — original", "fixed"),
             row("Qa-155-r1-01a", "P1", "**critical** — corrected", "fixed"))
    assert not offenders(*mixed, supersessions=(("Qa-155-r1-01a", "Qa-155-r1-01"),))

    # A MALFORMED PREDECESSOR corrected by a valid revision is not reported: the record
    # can be repaired the one way the workflow allows, by retaining the original and
    # appending the correction.
    short = "| CDX-155-r2-01 | gate | s | P1 | class | DC-155-C | standard | `abc1234` |"
    fixed_pair = ledger(short, row("CDX-155-r2-01a", "P1", "**critical** — corrected",
                                   "fixed"),
                        supersessions=(("CDX-155-r2-01a", "CDX-155-r2-01"),))
    assert not _tier_derivation_offenders(fixed_pair), _tier_derivation_offenders(fixed_pair)
    # ...and the same malformed row with NO standing correction still is. A
    # well-formed row rides along so the table is readable and the fail-closed
    # sentinel — which owns the "no rows at all" case — is not what answers here.
    alone = ledger(short, row("CDX-155-r1-01", "P2", "standard — anchor", "fixed"))
    assert [r for r, _, _ in _tier_derivation_offenders(alone)] == ["CDX-155-r2-01"]

    # A SOURCE LABEL CANNOT BE LAUNDERED BY A SUCCESSOR. The raw label is immutable, so
    # a chain is graded on the ORIGINAL row's label and the STANDING row's tier; taking
    # both from the successor let a `P1` predecessor be answered by a revision that
    # simply called itself `P2`, deleting the immutability rule through the mechanism
    # written to honour it.
    laundered = (row("CDX-155-r3-01", "P1", "standard — original", "fixed"),
                 row("CDX-155-r3-01a", "P2", "standard — relabelled", "fixed"))
    assert offenders(*laundered,
                     supersessions=(("CDX-155-r3-01a", "CDX-155-r3-01"),)) == [
        "CDX-155-r3-01a"]

    # THE WHOLE CHAIN, not one link. A → B → C where only C stands: checking the
    # immediate successor alone reported A whenever B was itself unusable.
    three = (row("CDX-155-r4-01", "P1", "standard — original", "fixed"),
             row("CDX-155-r4-01b", "P1", "**critical** — corrected", "fixed"))
    assert not offenders(*three, supersessions=(("CDX-155-r4-01a", "CDX-155-r4-01"),
                                                ("CDX-155-r4-01b", "CDX-155-r4-01a")))

    # A CYCLIC MAP terminates rather than hanging the rule.
    cyc = (row("CDX-155-r5-01", "P1", "standard", "fixed"),
           row("CDX-155-r5-01a", "P1", "standard", "fixed"))
    assert offenders(*cyc, supersessions=(("CDX-155-r5-01a", "CDX-155-r5-01"),
                                          ("CDX-155-r5-01", "CDX-155-r5-01a")))

    # A SECOND finding table with a different column order is refused, not read by
    # position: the row parser walks every ID-headed table, so validating only the
    # first left later rows graded through the wrong offsets.
    second = (ledger(row("CDX-155-r1-01", "P2", "standard — anchor", "fixed"))
              + "\n" + HEAD.replace("Original label | Blocking class",
                                    "Blocking class | Original label", 1)
              + row("CDX-155-r6-01", "P1", "standard", "fixed") + "\n")
    assert [r for r, _, _ in _tier_derivation_offenders(second)] == [
        "<the finding table's columns are not in the order this rule reads>"]


def _closing_row_without_covering_evidence(ledger_text, archive_dir):
    """Whether the LATEST closing checkpoint cites evidence naming the tree it closes.

    THE ORDERING AXIS OF `DC-155-G`, MADE EXECUTABLE. That class was first answered
    procedurally — write the checkpoint after the validation it governs — and the
    procedure has now failed four times, most recently in the very row that corrected
    the previous failure. A rule about WHEN a decision may be written cannot be
    enforced by remembering to write it later; the second axis of the same class was
    made executable for exactly this reason and has not recurred since.

    So the ordering rule is restated as a property of the ARTIFACT rather than of the
    author's sequence: a row that records a closing outcome must cite an archived round
    that exists in the checkout and that names the SHA the row itself names. A decision
    written ahead of its evidence cannot satisfy that, because the evidence is not there
    yet — which is the difference between a procedure and an invariant.

    SCOPED TO THE LATEST closing row, for the reason already recorded against this
    class: twenty-two early rounds were never archived and their output no longer
    exists. That is a limitation of the record, not a waiver, and it is why the rule
    binds the row closure actually rests on.
    """
    # The record's own view — see `_unfenced_lines`. A fenced block is an
    # illustration, not a row.
    ledger_text = "\n".join(_unfenced_lines(ledger_text))
    import hashlib
    import json
    import re

    #: THE OUTCOMES THAT END A LOOP, read from the OUTCOME CELL and nowhere else.
    #: Matching the whole row let a `CONTINUE` whose rationale quotes `CLOSE-CLEAN`
    #: be taken for the closing row — and hide the real one — while the two deferral
    #: outcomes, which also end a loop, were skipped because only one spelling was
    #: searched for. Both halves are the same mistake: reading a field from prose.
    ENDING = ("CLOSE-CLEAN", "DEFER-STANDARD-AND-CLOSE", "DEFER-STANDARD-AND-PROCEED")

    closing = []
    for row in (_checkpoint_rows(ledger_text) or []):
        cells = row.split("|")
        if len(cells) < 6:
            continue
        outcome = cells[4]
        # A WITHDRAWN close is not a close. Two such rows already exist in this record,
        # and matching the outcome token alone would let the newest of them displace the
        # decision that actually stands — or make a decision the record explicitly took
        # back look current.
        if re.search(r"WITHDRAWN|SUPERSED", outcome, re.I):
            continue
        if any(o in outcome for o in ENDING):
            closing.append(cells)
    if not closing:
        return None
    # The LAST such row is the standing one: revisions are appended, never edited in
    # place, so a superseded closing row is always followed by the row that replaces it.
    cells = closing[-1]
    loop, sha_cell, rationale = cells[1].strip(), cells[3], cells[5]

    # THE SHA COLUMN, not the row. Collecting every backticked SHA meant a rationale
    # mentioning a reviewed ANCESTOR — which these rationales routinely do, since they
    # explain what moved since the last gate — satisfied a row whose own tree was never
    # reviewed. That is the close-on-parent defect this rule exists to refuse, reading
    # the parent out of the sentence that named it.
    shas = re.findall(r"`([0-9a-f]{7,40})`", sha_cell)
    if not shas:
        return "the latest closing checkpoint names no SHA in its SHA column"
    # ONE closing tree. Accepting any token in the cell recreated the close-on-parent
    # defect inside the designated column: a cell naming an unreviewed tip AND a
    # reviewed ancestor passed on the ancestor. A row closes over exactly one tree, so
    # a cell naming two is ambiguous rather than generous.
    distinct = {s for s in shas}
    if len(distinct) > 1 and not all(
            a.startswith(b) or b.startswith(a) for a in distinct for b in distinct):
        return (f"the latest closing checkpoint names more than one tree in its SHA "
                f"column: {sorted(distinct)}")
    cited = re.findall(r"`((?:commit-reviews|wave-gate)/[A-Za-z0-9._-]+)`",
                       sha_cell + rationale)
    if not cited:
        return "the latest closing checkpoint cites no archived evidence"

    root = Path(archive_dir)
    #: Which loop each archived round was billed to. Commit-review rounds record it in
    #: the archive index; wave rounds record it in their own round file.
    billed, billed_status = {}, {}
    index = root / "index.jsonl"
    if index.is_file():
        for raw in index.read_text().splitlines():
            if not raw.strip():
                continue
            try:
                entry = json.loads(raw)
            except ValueError:
                continue
            billed[str(entry.get("durable_dir", ""))] = str(entry.get("logical_loop", ""))
            billed_status[str(entry.get("durable_dir", ""))] = entry.get("status")

    #: A LOOP IDENTITY IS PURPOSE PLUS SCOPE, and this record holds ten distinct L2
    #: scopes and eight L4 ones — so comparing the number alone let one loop's gate
    #: discharge another's, which is the same substitution the number was added to
    #: prevent. Compared canonically, because the record spells one scope two ways.
    def identity(text):
        return re.sub(r"[^a-z0-9]+", "", text.lower())

    want = identity(loop)
    covered, why = [], []
    for name in cited:
        d = root / name
        if not d.is_dir():
            return f"cited archive {name} is absent from the checkout"
        if name.startswith("wave-gate/"):
            summary = d / "summary.json"
            if not summary.is_file():
                why.append(f"{name} carries no summary")
                continue
            # THE ROUND RECORD IS THE AUTHORITY, and `_wave_evidence_violation`
            # already says why: a summary is evidence only when the round that
            # produced it binds and hashes it. Reading the summary here and the
            # round only for its loop reintroduced, in a second rule, the exact
            # unbound-summary defect the first one exists to refuse.
            record_path = d / "round.json"
            if not record_path.is_file():
                why.append(f"{name} carries no round record")
                continue
            record = json.loads(record_path.read_text())
            round_loop = str(record.get("logical_loop", ""))
            key = f"{name}/summary.json"
            listed = (record.get("files") or {}).get(key)
            if listed != hashlib.sha256(summary.read_bytes()).hexdigest():
                why.append(f"{name} does not bind its summary")
                continue
            attested = json.loads(summary.read_text())
            if (record.get("status") != "completed" or record.get("verdict") != "pass"
                    or attested.get("status") != "completed"
                    or attested.get("verdict") != "pass"):
                why.append(f"{name} is not a passing run")
                continue
            recorded = str(record.get("wave_sha", ""))
            if recorded != str(attested.get("wave_sha", "")):
                why.append(f"{name} records a round and a summary for different trees")
                continue
        else:
            # COMPLETION, NOT COMMENCEMENT. `start-head` is written when the review is
            # STARTED, so reading it counted a failed or abandoned round as coverage —
            # and this record holds exactly such an archive, carrying a failed phase, a
            # start head, and no reviewed SHA at all. The collector writes
            # `last-reviewed-sha` only on a completed round and `teardown` only once the
            # daemon is proven gone, so those two are what a covered tree is read from.
            # COMPLETION AS THE COLLECTOR DEFINES IT, not as two filenames. The
            # recovery reader counts a round complete only when `last-reviewed-sha` is
            # a regular file whose content EQUALS the recorded `start-head`, and the
            # collector writes `teardown` only once the daemon is proven gone — so a
            # stale or placeholder sidecar beside a failed index row is not a
            # completed round, however present the files are.
            reviewed, teardown = d / "last-reviewed-sha", d / "teardown"
            head = d / "start-head"
            if not (reviewed.is_file() and teardown.is_file() and head.is_file()):
                why.append(f"{name} records no completed, torn-down round")
                continue
            recorded = reviewed.read_text().strip()
            if recorded != head.read_text().strip():
                why.append(f"{name} reviewed a tree other than the one it started on")
                continue
            if teardown.read_text().strip() != "confirmed stopped":
                why.append(f"{name} has no confirmed teardown")
                continue
            round_loop = billed.get(name, "")
            if str(billed_status.get(name, "")) != "completed":
                why.append(f"{name} is indexed as {billed_status.get(name) or 'unknown'}")
                continue
        # THE CHECKPOINT'S OWN LOOP. A wave pass cannot discharge a commit-review
        # closure and a review billed elsewhere cannot either: loops are distinct gates,
        # so evidence from one says nothing about whether the other ever ran.
        # The row names the loop; the archive names the loop AND the delta it was
        # billed to, so one canonical identity must CONTAIN the other from the start.
        # That still separates this record's ten L2 scopes from one another, which a
        # bare `L2` never did, and its eight L4 scopes likewise.
        # AN ABSENT IDENTITY MATCHES NOTHING. The empty string is a prefix of every
        # string, so a round recording no loop at all satisfied every scoped
        # checkpoint — a containment test failing open on the one input that carries
        # no information. Both sides must actually name a loop.
        got = identity(round_loop)
        if not got or not want:
            why.append(f"{name} records no loop identity" if not got
                       else "the checkpoint names no loop identity")
            continue
        if not (got.startswith(want) or want.startswith(got)):
            why.append(f"{name} was billed to {round_loop.strip() or 'no loop'!r}")
            continue
        covered.append(recorded)

    for sha in shas:
        if any(r.startswith(sha) for r in covered if r):
            return None
    return (f"the latest closing checkpoint names {shas[0]} and no cited evidence of "
            f"its own loop covers it: {sorted(why) or sorted({r[:7] for r in covered})}")


def test_a_closing_checkpoint_cites_evidence_for_the_tree_it_closes():
    """A closing decision must be supported by evidence naming that same tree.

    The executable form of the ordering rule. Recorded against this record itself, so
    a closing row written before its gates exist fails here rather than being caught,
    one round later, by a reviewer reading the row's own rationale against its outcome.
    """
    root = Path(__file__).resolve().parents[1] / "docs/architecture"
    violation = _closing_row_without_covering_evidence(
        (root / "ISSUE_155_AUDIT_LEDGER.md").read_text(),
        root / "evidence/issue-155")
    assert violation is None, f"the closing checkpoint is not covered: {violation}"


def test_the_closing_evidence_rule_can_actually_fail(tmp_path):
    """THE NON-VACUITY WITNESS: every shape a procedure failed to prevent.

    The first version of this rule was written to make the ordering axis executable
    and reproduced, inside itself, three defects this record already carries: reading
    a field out of prose, trusting a marker written before the work rather than after
    it, and accepting evidence from a loop other than the one owed. Each has an arm.
    """
    import json

    import hashlib

    wave_body = json.dumps({"wave_sha": "1111111aaaa", "status": "completed",
                            "verdict": "pass"})
    for wave in ("w1", "wunbound"):
        d = tmp_path / "wave-gate" / wave
        d.mkdir(parents=True)
        (d / "summary.json").write_text(wave_body)
        record = {"logical_loop": "L4 (composite wave gate, issue-level)",
                  "status": "completed", "verdict": "pass", "wave_sha": "1111111aaaa"}
        if wave == "w1":
            # BOUND the way the archiver binds it; the other is the shape the wave
            # rule already refuses, kept here so this rule refuses it too.
            record["files"] = {"wave-gate/w1/summary.json":
                               hashlib.sha256(wave_body.encode()).hexdigest()}
        (d / "round.json").write_text(json.dumps(record))
    for name, done in (("c1", True), ("cfailed", False)):
        d = tmp_path / "commit-reviews" / name
        d.mkdir(parents=True)
        # Both rounds record a START. Only the completed one records a reviewed tree
        # and a confirmed teardown — which is the whole difference the rule reads.
        (d / "start-head").write_text("1111111aaaa\n")
        if done:
            (d / "last-reviewed-sha").write_text("1111111aaaa\n")
            (d / "teardown").write_text("confirmed stopped\n")
        else:
            (d / "phase").write_text("failed\n")
    (tmp_path / "index.jsonl").write_text("\n".join(json.dumps(r) for r in (
        {"durable_dir": "commit-reviews/c1", "status": "completed",
         "logical_loop": "L2 (Stage-2 Codex commit review, issue-level)"},
        {"durable_dir": "commit-reviews/cfailed", "status": "failed",
         "logical_loop": "L2 (Stage-2 Codex commit review, issue-level)"},
        # A round whose SIDECARS look complete while the index records a failure —
        # the shape that made file presence an unsafe reading of completion.
        {"durable_dir": "commit-reviews/cstale", "status": "failed",
         "logical_loop": "L2 (Stage-2 Codex commit review, issue-level)"},
    )) + "\n")
    stale = tmp_path / "commit-reviews" / "cstale"
    stale.mkdir(parents=True)
    for n, v in (("start-head", "1111111aaaa"), ("last-reviewed-sha", "1111111aaaa"),
                 ("teardown", "confirmed stopped")):
        (stale / n).write_text(v + "\n")

    header = ("| Loop | Evaluation (window / cumulative) | SHA (+dirty) | Outcome | Rationale |\n"
              "| --- | --- | --- | --- | --- |\n")

    def ledger(sha_cell, outcome="`CLOSE-CLEAN`", rationale="closed", loop=None):
        return header + (f"| {loop or 'L2 Stage-2 Codex commit review, issue-level'} | "
                         f"1 / 1 | {sha_cell} | {outcome} | {rationale} |\n")

    def check(*a, **kw):
        return _closing_row_without_covering_evidence(ledger(*a, **kw), tmp_path)

    GOOD = "`1111111`, archived `commit-reviews/c1`"

    # The control: a completed review of this loop, naming this tree.
    assert check(GOOD) is None

    # THE HISTORICAL SHAPE — the decision names the corrected tree while its evidence
    # names the parent.
    assert check("`2222222`, archived `commit-reviews/c1`") == (
        "the latest closing checkpoint names 2222222 and no cited evidence of its own "
        "loop covers it: ['1111111']")

    # A START MARKER IS NOT COMPLETION. This round began on the right tree and failed;
    # the record holds exactly such an archive.
    assert check("`1111111`, archived `commit-reviews/cfailed`") == (
        "the latest closing checkpoint names 1111111 and no cited evidence of its own "
        "loop covers it: ['commit-reviews/cfailed records no completed, torn-down round']")

    # ANOTHER LOOP'S GATE DISCHARGES NOTHING. A wave pass on the same tree cannot
    # stand in for the commit review an L2 checkpoint owes — an earlier version of
    # this witness asserted precisely that as a POSITIVE case.
    assert check("`1111111`, archived `wave-gate/w1`") == (
        "the latest closing checkpoint names 1111111 and no cited evidence of its own "
        'loop covers it: ["wave-gate/w1 was billed to \'L4 (composite wave gate, '
        "issue-level)'\"]")
    # ...and the same archive DOES discharge an L4 checkpoint.
    assert check("`1111111`, archived `wave-gate/w1`",
                 loop="L4 composite wave gate, issue-level") is None

    # THE SHA COMES FROM ITS OWN COLUMN. A rationale naming the reviewed ancestor —
    # which these rationales routinely do — must not satisfy a row whose tree was
    # never reviewed.
    assert check("`2222222`, archived `commit-reviews/c1`",
                 rationale="the delta since `1111111`") == (
        "the latest closing checkpoint names 2222222 and no cited evidence of its own "
        "loop covers it: ['1111111']")

    # THE OUTCOME COMES FROM ITS OWN CELL. A `CONTINUE` whose rationale quotes the
    # closing outcome is not a closing row, and must not displace the real one.
    assert check("`2222222`", outcome="`CONTINUE`",
                 rationale="not yet `CLOSE-CLEAN`") is None
    # BOTH deferral outcomes end a loop and are checked like a close.
    for ending in ("`DEFER-STANDARD-AND-CLOSE`", "`DEFER-STANDARD-AND-PROCEED`"):
        assert check("`2222222`, archived `commit-reviews/c1`", outcome=ending), ending

    # SIDECARS THAT LOOK COMPLETE BESIDE AN INDEX THAT SAYS FAILED. Every file the
    # earlier rule read is present and well-formed; only the round's own recorded
    # status says otherwise, and it is the one that decides.
    assert check("`1111111`, archived `commit-reviews/cstale`") == (
        "the latest closing checkpoint names 1111111 and no cited evidence of its own "
        "loop covers it: ['commit-reviews/cstale is indexed as failed']")

    # A REVIEWED TREE THAT IS NOT THE ONE THE ROUND STARTED ON is not a completed
    # round either — the recovery reader defines completion as those two agreeing.
    (tmp_path / "commit-reviews" / "c1" / "last-reviewed-sha").write_text("3333333cccc\n")
    assert "reviewed a tree other than the one it started on" in check(
        "`1111111`, archived `commit-reviews/c1`")
    (tmp_path / "commit-reviews" / "c1" / "last-reviewed-sha").write_text("1111111aaaa\n")

    # A TEARDOWN THAT WAS NEVER CONFIRMED leaves the round unaccounted for.
    (tmp_path / "commit-reviews" / "c1" / "teardown").write_text("maybe\n")
    assert "has no confirmed teardown" in check("`1111111`, archived `commit-reviews/c1`")
    (tmp_path / "commit-reviews" / "c1" / "teardown").write_text("confirmed stopped\n")

    # A WAVE SUMMARY THE ROUND DOES NOT BIND is not evidence here either — the same
    # rule the wave check makes, rather than a second weaker copy of it.
    assert "does not bind its summary" in check(
        "`1111111`, archived `wave-gate/wunbound`",
        loop="L4 composite wave gate, issue-level")

    # A CELL NAMING TWO TREES is ambiguous, not satisfied by whichever one has
    # evidence — the close-on-parent defect moved inside the designated column.
    assert check("`deadbee` based on `1111111`, archived `commit-reviews/c1`") == (
        "the latest closing checkpoint names more than one tree in its SHA column: "
        "['1111111', 'deadbee']")

    # A WITHDRAWN close does not displace the decision that stands.
    withdrawn = (header
                 + "| L2 Stage-2 Codex commit review, issue-level | 1 / 1 | "
                   "`1111111`, archived `commit-reviews/c1` | `CLOSE-CLEAN` | stands |\n"
                 + "| L2 Stage-2 Codex commit review, issue-level | 2 / 2 | `2222222` | "
                   "`DEFER-STANDARD-AND-CLOSE` — **WITHDRAWN** | taken back |\n")
    assert _closing_row_without_covering_evidence(withdrawn, tmp_path) is None

    # AN ARCHIVE RECORDING NO LOOP AT ALL satisfies nothing: the empty string is a
    # prefix of every string, so the one input carrying no information was the one
    # input that matched everything.
    (tmp_path / "index.jsonl").write_text(json.dumps(
        {"durable_dir": "commit-reviews/c1", "status": "completed"}) + "\n")
    assert check("`1111111`, archived `commit-reviews/c1`") == (
        "the latest closing checkpoint names 1111111 and no cited evidence of its own "
        "loop covers it: ['commit-reviews/c1 records no loop identity']")

    # An archive nobody can find is not evidence.
    assert check("`1111111`, archived `commit-reviews/gone`") == (
        "cited archive commit-reviews/gone is absent from the checkout")
    assert check("`1111111`, closed on judgement") == (
        "the latest closing checkpoint cites no archived evidence")
