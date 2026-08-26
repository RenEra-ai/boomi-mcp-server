"""Issue #155: the gate-round archiver, and the one rule it exists to obey.

The archiver's whole job is producing evidence the repository's archive scanner
will accept. A mode that writes an archive that scanner refuses is worse than no
mode at all — the failure surfaces at the gate, long after the round it was
meant to evidence has been torn down.

Run with PYTHONPATH=src (the editable-install .pth is stale):
    PYTHONPATH=src .venv/bin/python -m pytest tests/test_archive_gate_round.py
"""

import json
import subprocess
import sys
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "archive_gate_round.py"


def _archive(tmp_path, run_dir, kind, extra=()):
    root = tmp_path / "repo" / "docs" / "architecture" / "evidence" / "issue-999"
    root.mkdir(parents=True, exist_ok=True)
    index = root / "index.jsonl"
    if not index.exists():
        index.write_text(json.dumps({
            "generated_at": "x", "issue": 999, "schema_version": 1, "source_tip": "abc",
        }) + "\n")
    result = subprocess.run(
        [sys.executable, str(_SCRIPT), "--issue", "999", "--kind", kind,
         "--run-dir", str(run_dir), "--logical-loop", "L", "--repo",
         str(tmp_path / "repo"), *extra],
        capture_output=True, text=True,
    )
    return result, root


def _commit_review_run(tmp_path, name="cdx-review.AAAAAA", reviewed="bbb222"):
    run = tmp_path / name
    run.mkdir()
    (run / "baseline").write_text("aaa111\n")
    (run / "start-head").write_text("bbb222\n")
    (run / "last-reviewed-sha").write_text(reviewed + "\n")
    (run / "teardown").write_text("confirmed stopped\n")
    (run / "start.json").write_text(json.dumps({"threadId": "t-1", "socket": "/s"}) + "\n")
    (run / "review.json").write_text('{"findings":[]}\n')
    # Never archived: a machine-local path and poll bookkeeping.
    (run / "cache-dir").write_text("/local/path\n")
    (run / "missing-probes").write_text("0\n")
    return run


def _wave_run(tmp_path):
    run = tmp_path / "wave.AAAAAA"
    run.mkdir()
    (run / "wave.log").write_text("wave ok\n")
    (run / "summary.json").write_text('{"status":"completed","wave_sha":"abc"}\n')
    return run


def test_a_wave_gate_round_is_archived_but_never_indexed(tmp_path):
    """CDX round 1 P2, and the reason is a contract with the archive scanner.

    That scanner reads `start.json` for every indexed row and takes its
    `threadId` as the round's identity, accepting only the two collectors that
    produce one. A wave-gate run has no daemon and so no thread — indexing it
    wrote a row the scanner refuses, which would have failed the very gate the
    archive exists to evidence.
    """
    result, root = _archive(tmp_path, _wave_run(tmp_path), "wave-gate")
    assert result.returncode == 0, result.stderr

    rows = [json.loads(line) for line in (root / "index.jsonl").read_text().splitlines()]
    assert len(rows) == 1, rows          # the header, and nothing else
    assert "issue" in rows[0]

    # The evidence still exists and is still checksummed — what it does not get
    # is a row claiming a collector attested it.
    archived = root / "wave-gate" / "wave.AAAAAA"
    assert (archived / "wave.log").is_file()
    assert (archived / "round.json").is_file()
    sums = (root / "SHA256SUMS").read_text()
    assert "wave-gate/wave.AAAAAA/wave.log" in sums
    assert "wave-gate/wave.AAAAAA/round.json" in sums


def test_a_commit_review_round_IS_indexed(tmp_path):
    """The control: the mode that has an attestation still records one.

    Without this, the test above would pass on an archiver that indexed nothing.
    """
    result, root = _archive(tmp_path, _commit_review_run(tmp_path), "commit-review")
    assert result.returncode == 0, result.stderr

    rows = [json.loads(line) for line in (root / "index.jsonl").read_text().splitlines()]
    assert len(rows) == 2, rows
    row = rows[1]
    assert row["collector"] == "commit-review-collect"
    assert row["status"] == "completed"
    assert row["reviewed_sha"] == "bbb222"


@pytest.mark.parametrize("excluded", ["cache-dir", "missing-probes"])
def test_the_machine_local_sidecars_are_never_archived(tmp_path, excluded):
    """A path only this machine has makes the record irreproducible elsewhere."""
    result, root = _archive(tmp_path, _commit_review_run(tmp_path), "commit-review")
    assert result.returncode == 0, result.stderr
    assert not (root / "commit-reviews" / "cdx-review.AAAAAA" / excluded).exists()


def test_a_round_whose_reviewed_sha_disagrees_is_not_recorded_as_completed(tmp_path):
    """A failed round must never shrink the next review's scope.

    Completion means the recorded reviewed SHA equals the SHA the daemon started
    on. A mismatch is a failed round, and laundering it into a completion is how
    an unreviewed commit becomes the next anchor.
    """
    run = _commit_review_run(tmp_path, reviewed="ccc999")
    result, root = _archive(tmp_path, run, "commit-review")
    assert result.returncode == 0, result.stderr
    row = [json.loads(l) for l in (root / "index.jsonl").read_text().splitlines()][1]
    assert row["status"] != "completed"
    assert row["reviewed_sha"] is None
