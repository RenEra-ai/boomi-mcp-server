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
    from boomi_mcp.connector_replay.ingest import _output_observation
    from boomi_mcp.connector_replay.models import OutputObservationV1

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
                record["size"] = 0
                record["successCount"] = 0
                record["errorCount"] = 0
                touched = True
        if touched:
            path.write_text(json.dumps(payload))

    zeroed = summarize(dst, "PATCH")
    assert zeroed.connector_response_bytes == 0
    assert zeroed.connector_documents == 0
    assert _output_observation(zeroed) is OutputObservationV1.NO_OUTPUT_OBSERVED

    # CONTROL: the execution's own counts are untouched and still non-zero, which
    # is exactly why the process-level read could not see this.
    assert any((r.outbound_documents or 0) > 0 for r in zeroed.runs)

    # CONTROL, the other way: unmodified, the same capture still reports output.
    intact = summarize(_CAPTURES / _SOURCE, "PATCH")
    assert _output_observation(intact) is OutputObservationV1.RETURN_DOCUMENTS_RECEIVED


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
