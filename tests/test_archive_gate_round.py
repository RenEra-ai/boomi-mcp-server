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


def _gate_run(tmp_path, prompt_bytes=b"the attested prompt\n", attest=True):
    """A gate run directory plus the prompt directory the seam keeps separate."""
    import hashlib

    run = tmp_path / "cdx-gate-review.AAAAAA"
    run.mkdir()
    (run / "start.json").write_text(json.dumps({"threadId": "t-9", "socket": "/s"}) + "\n")
    (run / "review.md").write_text("a review\n")
    digest = hashlib.sha256(prompt_bytes).hexdigest()
    (run / "attestation.json").write_text(json.dumps({
        "gate": "review", "status": "completed",
        **({"promptSha256": digest} if attest else {}),
    }) + "\n")

    prompts = tmp_path / "cdx-gate-prompts.BBBBBB"
    prompts.mkdir()
    (prompts / "prompt").write_bytes(prompt_bytes)
    return run, prompts


def _archive_gate(tmp_path, run, prompts):
    root = tmp_path / "repo" / "docs" / "architecture" / "evidence" / "issue-999"
    root.mkdir(parents=True, exist_ok=True)
    index = root / "index.jsonl"
    if not index.exists():
        index.write_text(json.dumps({
            "generated_at": "x", "issue": 999, "schema_version": 1, "source_tip": "abc",
        }) + "\n")
    args = [sys.executable, str(_SCRIPT), "--issue", "999", "--kind", "architect-review",
            "--run-dir", str(run), "--logical-loop", "L3",
            "--repo", str(tmp_path / "repo")]
    if prompts is not None:
        args += ["--prompts", str(prompts)]
    return subprocess.run(args, capture_output=True, text=True), root


def test_a_gate_round_with_the_attested_prompt_is_archived(tmp_path):
    """The positive control — without it the refusals below could be vacuous."""
    run, prompts = _gate_run(tmp_path)
    result, root = _archive_gate(tmp_path, run, prompts)
    assert result.returncode == 0, result.stderr
    assert (root / "architect-reviews" / run.name / "prompts" / "prompt").is_file()
    assert len((root / "index.jsonl").read_text().splitlines()) == 2


def test_a_gate_round_with_no_prompt_is_REFUSED_before_anything_is_recorded(tmp_path):
    """The prompt is required evidence, and the refusal must come FIRST.

    The collector sidecars alone make the copy non-empty, so a missing or
    mistyped prompt directory previously exited 0 and recorded an archive the
    repository's own scanner then rejected — by which time the source run may be
    gone and the prompt unrecoverable. The archive and the index row must not
    exist after a refusal.
    """
    run, _ = _gate_run(tmp_path)
    result, root = _archive_gate(tmp_path, run, tmp_path / "nowhere")
    assert result.returncode == 1, result.stdout
    assert "no prompt" in result.stderr
    assert not (root / "architect-reviews" / run.name).exists()
    assert len((root / "index.jsonl").read_text().splitlines()) == 1


def test_a_gate_round_whose_prompt_is_not_THE_attested_one_is_REFUSED(tmp_path):
    """Present-but-wrong is the more dangerous case than absent.

    An archive holding some other prompt still satisfies "a prompts directory
    exists", and would stand as evidence that the gate judged something it never
    saw. The attested digest is the only thing that distinguishes them.
    """
    run, _ = _gate_run(tmp_path, prompt_bytes=b"the attested prompt\n")
    other = tmp_path / "other-prompts"
    other.mkdir()
    (other / "prompt").write_bytes(b"a completely different prompt\n")

    result, root = _archive_gate(tmp_path, run, other)
    assert result.returncode == 1, result.stdout
    assert "not the attested" in result.stderr
    assert not (root / "architect-reviews" / run.name).exists()


def test_an_unattested_round_still_requires_a_prompt_to_be_present(tmp_path):
    """With no attested digest the archiver cannot compare — but absence is still
    a refusal, so a round with no attestation cannot smuggle in an empty one."""
    run, _ = _gate_run(tmp_path, attest=False)
    result, root = _archive_gate(tmp_path, run, tmp_path / "nowhere")
    assert result.returncode == 1, result.stdout
    assert not (root / "architect-reviews" / run.name).exists()
