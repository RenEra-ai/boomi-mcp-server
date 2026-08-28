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
    """STRUCTURAL: the set is pinned to the module's own reads, not hand-kept.

    Every key this module pulls off a platform connector record must be reconciled
    between two archived copies of that record, or a served observation can vary by
    filename. Hand-listing the set let `connectorType` fall out — the family
    reconciliation reads it — so the list is derived here and compared.
    """
    import ast

    from boomi_mcp.connector_replay import capture as module

    source = Path(module.__file__).read_text()
    tree = ast.parse(source)

    # The readers: every function that takes a platform record and asks it for a
    # named key. Found by shape — `<name>.get("literal")` where the receiver is a
    # record or a row — rather than by a list of function names.
    receivers = {"record", "row", "seen", "r", "a", "b"}
    read_keys = {
        node.args[0].value
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "get"
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id in receivers
        and node.args
        and isinstance(node.args[0], ast.Constant)
        and isinstance(node.args[0].value, str)
    }
    # Keys read through a loop over a literal tuple — `for field in ("a", "b"):
    # row.get(field)` — are reads too, and missing them is how this derivation
    # would have passed while the set was still wrong.
    for node in ast.walk(tree):
        if not isinstance(node, ast.For) or not isinstance(node.target, ast.Name):
            continue
        if not isinstance(node.iter, (ast.Tuple, ast.List)):
            continue
        literals = [
            e.value for e in node.iter.elts
            if isinstance(e, ast.Constant) and isinstance(e.value, str)
        ]
        if not literals or len(literals) != len(node.iter.elts):
            continue
        reads_the_variable = any(
            isinstance(inner, ast.Call)
            and isinstance(inner.func, ast.Attribute)
            and inner.func.attr == "get"
            and isinstance(inner.func.value, ast.Name)
            and inner.func.value.id in receivers
            and inner.args
            and isinstance(inner.args[0], ast.Name)
            and inner.args[0].id == node.target.id
            for inner in ast.walk(node)
        )
        if reads_the_variable:
            read_keys.update(literals)

    # `id` is the reconciliation KEY, not a reconciled value: two copies that
    # disagree on it are two different rows, which is not a conflict.
    read_keys.discard("id")

    assert read_keys, "the derivation found no reads, so it would pass vacuously"
    assert set(module._CORRELATED_ROW_FIELDS) == read_keys, (
        "the reconciled field set and the module's reads have drifted: "
        f"only reconciled {sorted(set(module._CORRELATED_ROW_FIELDS) - read_keys)}, "
        f"only read {sorted(read_keys - set(module._CORRELATED_ROW_FIELDS))}"
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
