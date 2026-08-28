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
import shutil
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
    per-run attribution, and the replay verdict falls back to unverified.
    """
    from boomi_mcp.connector_replay.ingest import classify
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
    # ...so the verdict degrades rather than being minted from a guessed order.
    assert classify(partial, "DELETE")[1] is RetrySafetyV1.UNVERIFIED
    # And the untimed run must not have been promoted to first, which is what the
    # superseded ordering did and what made the served execution id wrong.
    assert [r.label for r in partial.runs] == ["run1", "run2"]


def test_two_runs_sharing_one_timestamp_are_not_an_order(tmp_path):
    """A tie establishes no sequence either."""
    from boomi_mcp.connector_replay.ingest import classify
    from boomi_mcp.connector_replay.models import RetrySafetyV1

    dst = tmp_path / "tied-chronology"
    shutil.copytree(_CAPTURES / "cap155-e5-delete-attested", dst)
    records = sorted(dst.glob("*execution_record*.json"))
    shared = _first_time(json.loads(records[0].read_text()))
    assert shared

    for record in records:
        payload = json.loads(record.read_text())
        _set_time(payload, shared)
        record.write_text(json.dumps(payload))

    tied = summarize(dst, "DELETE")
    assert all(run.counterparty_status is None for run in tied.runs)
    assert classify(tied, "DELETE")[1] is RetrySafetyV1.UNVERIFIED
