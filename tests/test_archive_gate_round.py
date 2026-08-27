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

_REPO = Path(__file__).resolve().parent.parent
_SCRIPT = _REPO / "scripts" / "archive_gate_round.py"
_ARCHITECT_ROUNDS = _REPO / "docs/architecture/evidence/issue-155/architect-reviews"


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
    # The REAL collector schema — nested under `prompt`, not a flat key. The
    # first version of this fixture invented `promptSha256`, which is what
    # `collect` prints to stdout, and that invention hid a guard that was inert
    # on every real round. `test_the_fixture_matches_a_real_attestation` below
    # pins this shape against an attestation the repository actually archived.
    (run / "attestation.json").write_text(json.dumps({
        "schema": 1, "gate": "review",
        "turn": {"status": "completed", "kind": "turn"},
        "parsedVerdict": "NO ISSUES",
        **({"prompt": {"actualSha256": digest, "allowedSha256": [digest],
                       "verified": True}} if attest else {}),
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


def test_the_fixture_matches_a_real_attestation_rather_than_an_invented_one():
    """The guard above is only as good as the shape this fixture claims.

    It previously claimed a flat `promptSha256`, which is what the collector
    prints to STDOUT — not what it writes to the attestation FILE, where the
    digest is nested under `prompt`. The guard read the flat key, was therefore
    inert on every real round, and this fixture's invention is what kept the
    tests green. A review caught it; the suite could not have.

    So the fixture is pinned against an attestation the repository actually
    archived. If the collector's schema moves, this fails and the guard gets
    revisited, instead of the fixture quietly describing a world of its own.
    """
    real = sorted(
        _ARCHITECT_ROUNDS.glob("*/attestation.json")
    )
    assert real, "no archived attestation to pin against — this test would be vacuous"

    on_disk = json.loads(real[0].read_text())
    assert "prompt" in on_disk, sorted(on_disk)
    assert "actualSha256" in on_disk["prompt"], sorted(on_disk["prompt"])
    assert "promptSha256" not in on_disk, (
        "the attestation file now carries a flat digest too — the guard should "
        "read whichever the collector actually writes, not both by accident"
    )

    # ...and the fixture this module builds must have the same shape.
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        run, _prompts = _gate_run(Path(tmp))
        fixture = json.loads((run / "attestation.json").read_text())
    assert set(fixture["prompt"]) >= {"actualSha256"}, sorted(fixture["prompt"])
    assert "promptSha256" not in fixture

    # Every path the row mapping resolves must resolve in the fixture too. The
    # first version of this fixture omitted the verdict and only the invariant
    # noticed — a fixture that is a SUBSET of the producer is the same defect as
    # one that invents a field, just quieter.
    module = _archiver_module()
    missing = {
        name: path for name, path in module.ARCHITECT_ROW_PATHS.items()
        if module._dig(fixture, path) in (None, "")
    }
    assert missing == {}, missing


def test_a_prompts_path_holding_only_directories_is_REFUSED(tmp_path):
    """The parent supplied one level too high — non-empty, but hashing nothing.

    `--prompts` pointed at a directory that CONTAINS the prompt directory makes
    the collection non-empty while yielding no file to hash, so a check counting
    entries accepted it. Git carries no empty directories either, so the durable
    evidence would not survive a clone — the archive would claim a prompt that
    literally cannot be there.
    """
    run, prompts = _gate_run(tmp_path)
    parent = tmp_path / "one-level-too-high"
    (parent / "cdx-gate-prompts.BBBBBB").mkdir(parents=True)

    result, root = _archive_gate(tmp_path, run, parent)
    assert result.returncode == 1, result.stdout
    assert "no prompt" in result.stderr
    assert not (root / "architect-reviews" / run.name).exists()


def test_the_guard_is_not_inert_against_a_REAL_attestation(tmp_path):
    """The failure mode the review found: right shape, wrong field, always None.

    Built from an attestation the repository actually archived rather than from
    this module's fixture, so it fails if the guard ever again reads a key the
    collector does not write. A wrong prompt directory must be REFUSED even when
    the attestation is the real article.
    """
    real = sorted(
        _ARCHITECT_ROUNDS.glob("*/attestation.json")
    )
    assert real, "no archived attestation — this test would be vacuous"

    run = tmp_path / "cdx-gate-review.REALATT"
    run.mkdir()
    (run / "start.json").write_text(json.dumps({"threadId": "t-real", "socket": "/s"}) + "\n")
    (run / "review.md").write_text("a review\n")
    (run / "attestation.json").write_text(real[0].read_text())

    wrong = tmp_path / "wrong-prompts"
    wrong.mkdir()
    (wrong / "prompt").write_bytes(b"not the prompt the gate actually ran\n")

    result, root = _archive_gate(tmp_path, run, wrong)
    assert result.returncode == 1, result.stdout
    assert "not the attested" in result.stderr
    assert not (root / "architect-reviews" / run.name).exists()


def _real_attestation():
    """An attestation this repository actually archived — never a fixture.

    Everything below is built from the producer's own bytes, because every
    defect this module has covered so far came from a check whose expectation I
    wrote myself: a flat digest key the collector does not emit, a fixture that
    invented that key, and a guard that read a schema its own consumer had been
    reading correctly all along.
    """
    real = sorted(_ARCHITECT_ROUNDS.glob("*/attestation.json"))
    assert real, "no archived attestation — every test using this would be vacuous"
    return json.loads(real[0].read_text())


def _archiver_module():
    import importlib.util

    spec = importlib.util.spec_from_file_location("_archiver_under_test", _SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_every_architect_row_path_resolves_in_a_REAL_attestation():
    """The sibling sweep's pin — and what would have caught the original.

    Three row fields were read from keys the collector does not write, so the
    single architect round archived before the sweep carries a null prompt
    digest and a null reviewed sha. Nothing failed, because a null reads exactly
    like "the collector had nothing to say".

    Each mapped path is therefore resolved against a real attestation and
    required to produce a value. A path that stops resolving — the schema moved,
    or someone wrote the lookup from memory again — fails here rather than being
    archived as a null.
    """
    module = _archiver_module()
    attestation = _real_attestation()

    unresolved = {
        name: path for name, path in module.ARCHITECT_ROW_PATHS.items()
        if module._dig(attestation, path) in (None, "")
    }
    assert unresolved == {}, unresolved

    # ...and the flat spellings the broken version read must NOT resolve, or the
    # assertion above would hold for both and prove nothing about which is read.
    for flat in ("promptSha256", "reviewedSha", "head", "status", "verdict"):
        assert flat not in attestation, (
            "the attestation now carries {0} at top level too — decide which "
            "spelling is authoritative instead of letting both work".format(flat)
        )


def test_the_mapping_covers_what_the_row_needs_and_nothing_it_invents():
    """Coverage stated against the authority's case set, not against itself.

    The scanner reads seven facts out of an attestation: teardown, turn.status,
    parsedVerdict, artifact.path, artifact.sha256, start.threadId and
    prompt.actualSha256. The index row needs four of them; the other three are
    bound by the scanner at validation time, so they are covered rather than
    skipped. This pins that split so a field cannot be quietly dropped from the
    row on the grounds that "the scanner checks it" when the scanner does not.
    """
    module = _archiver_module()
    attestation = _real_attestation()

    assert set(module.ARCHITECT_ROW_PATHS) == {
        "gate", "prompt_sha256", "status", "verdict"
    }, sorted(module.ARCHITECT_ROW_PATHS)
    # The three the SCANNER binds, present in the real attestation, so the claim
    # "the scanner covers them" is checkable here and not merely asserted.
    for path in (("teardown",), ("artifact", "sha256"), ("start", "threadId")):
        assert module._dig(attestation, path), path

    # `reviewed_sha` is deliberately absent from the mapping and this is the
    # fact that justifies it: no spelling of a reviewed sha exists to read. That
    # is worth stating explicitly, because it means the constant carrying the
    # null has NO behaviour a mutation can distinguish — swapping it back for a
    # probe of the invented keys produces the identical row. What is testable is
    # the premise, so the premise is what is tested.
    for absent in ("reviewedSha", "head", "reviewed_sha", "sha"):
        assert absent not in attestation, absent


@pytest.mark.parametrize("mutate,label", [
    (lambda a: a.pop("prompt"), "prompt block renamed away"),
    (lambda a: a["prompt"].pop("actualSha256"), "digest key dropped"),
    (lambda a: a["prompt"].update(actualSha256=None), "digest present but null"),
    (lambda a: a.update(prompt="9c5d0467"), "prompt block is a string, not a dict"),
    (lambda a: a["turn"].pop("status"), "turn status dropped"),
])
def test_an_attestation_the_archiver_cannot_READ_is_refused(tmp_path, mutate, label):
    """NON-VACUITY: the exact cases the new invariant excludes.

    Every one of these previously archived at exit 0 WITH THE WRONG PROMPT,
    because an unreadable digest was treated the same as an absent attestation —
    the identical fail-open the flat-key read produced, one layer further in. An
    attested round nobody can place is worse than an unattested one, so it now
    refuses.

    The string case is also the crash QA found: `attestation.get("prompt") or {}`
    took a truthy non-dict as a mapping and raised AFTER the destination was
    created, leaving debris that then blocked the operator's corrected retry.
    """
    attestation = _real_attestation()
    mutate(attestation)

    run = tmp_path / "cdx-gate-review.MUTANT"
    run.mkdir()
    (run / "start.json").write_text(json.dumps({"threadId": "t-m", "socket": "/s"}) + "\n")
    (run / "review.md").write_text("a review\n")
    (run / "attestation.json").write_text(json.dumps(attestation) + "\n")

    prompts = tmp_path / "prompts-that-are-wrong"
    prompts.mkdir()
    (prompts / "prompt").write_bytes(b"not the prompt the gate ran\n")

    result, root = _archive_gate(tmp_path, run, prompts)
    assert result.returncode == 1, (label, result.stdout)
    assert "Traceback" not in result.stderr, (label, result.stderr)
    assert not (root / "architect-reviews" / run.name).exists(), label

    # ...and the destination is CLEAN, so the corrected retry is not blocked by
    # the refusal that preceded it. This is the half QA-A11 measured as broken.
    good = tmp_path / "prompts-correct"
    good.mkdir()
    (good / "prompt").write_bytes(b"whatever\n")
    retry, _ = _archive_gate(tmp_path, run, good)
    assert "refusing to overwrite" not in retry.stderr, (label, retry.stderr)


def test_an_UNMUTATED_real_attestation_still_archives(tmp_path):
    """The control the refusal battery needs: it is not refusing everything.

    Uses the real archived prompt bytes, which ARE in the tree beside the
    attestation, so the positive half is measured rather than assumed absent.
    """
    round_dir = sorted(_ARCHITECT_ROUNDS.glob("*/attestation.json"))[0].parent

    run = tmp_path / "cdx-gate-review.CONTROL"
    run.mkdir()
    (run / "start.json").write_text(json.dumps({"threadId": "t-c", "socket": "/s"}) + "\n")
    (run / "review.md").write_text("a review\n")
    (run / "attestation.json").write_text((round_dir / "attestation.json").read_text())

    prompts = tmp_path / "prompts-real"
    prompts.mkdir()
    for src in sorted((round_dir / "prompts").iterdir()):
        (prompts / src.name).write_bytes(src.read_bytes())

    result, root = _archive_gate(tmp_path, run, prompts)
    assert result.returncode == 0, result.stderr
    archived = root / "architect-reviews" / run.name
    assert (archived / "attestation.json").is_file()

    row = json.loads((root / "index.jsonl").read_text().splitlines()[-1])
    attested = json.loads((round_dir / "attestation.json").read_text())
    assert row["prompt_sha256"] == attested["prompt"]["actualSha256"]
    assert row["status"] == attested["turn"]["status"]
    assert row["verdict"] == attested["parsedVerdict"]
    assert row["reviewed_sha"] is None


def _rederive(tmp_path):
    return subprocess.run(
        [sys.executable, str(_SCRIPT), "--issue", "999", "--rederive-index",
         "--repo", str(tmp_path / "repo")],
        capture_output=True, text=True,
    )


def test_a_stale_index_row_is_recomputed_from_the_archived_bytes(tmp_path):
    """Why this mode exists: correcting a derivation strands every existing row.

    The attestation-path fix turned the one archived architect row's null prompt
    digest into the real one. Nothing would have reported the disagreement — the
    archive scanner does not compare these row fields against the attestation,
    which is precisely how a null sat there unnoticed in the first place. So the
    index is re-derivable on demand rather than written once and trusted.
    """
    run, prompts = _gate_run(tmp_path)
    result, root = _archive_gate(tmp_path, run, prompts)
    assert result.returncode == 0, result.stderr

    index = root / "index.jsonl"
    header, line = index.read_text().splitlines()
    good = json.loads(line)
    assert good["prompt_sha256"], "the positive path did not record a digest"

    stale = dict(good, prompt_sha256=None, verdict="NO ISSUES AT ALL", status="completed")
    index.write_text(header + "\n" + json.dumps(stale, sort_keys=True) + "\n")

    assert _rederive(tmp_path).returncode == 0
    assert json.loads(index.read_text().splitlines()[1]) == good

    # ...and the caller-supplied facts that are NOT in the bytes survive it.
    assert good["logical_loop"] == "L3"
    assert good["source_run_dir"] == str(run)


def test_rederiving_never_promotes_a_failed_round_into_a_completion(tmp_path):
    """A status the caller overrode is not recoverable from the archived bytes.

    A round recorded failed or timed out looks, in its own files, much like one
    that completed — that is exactly why the collector refuses to write the
    completion marker for it. Re-deriving must not quietly manufacture the
    completion the collector declined to record.
    """
    run, prompts = _gate_run(tmp_path)
    result, root = _archive_gate(tmp_path, run, prompts)
    assert result.returncode == 0, result.stderr

    index = root / "index.jsonl"
    header, line = index.read_text().splitlines()
    failed = dict(json.loads(line), status="failed")
    index.write_text(header + "\n" + json.dumps(failed, sort_keys=True) + "\n")

    assert _rederive(tmp_path).returncode == 0
    assert json.loads(index.read_text().splitlines()[1])["status"] == "failed"


def test_rederive_does_not_need_the_flags_that_name_a_single_round(tmp_path):
    """It re-derives every row, so demanding --kind/--run-dir would be a lie."""
    run, prompts = _gate_run(tmp_path)
    assert _archive_gate(tmp_path, run, prompts)[0].returncode == 0

    assert _rederive(tmp_path).returncode == 0
    # ...and the ordinary path still requires them, rather than silently
    # archiving nothing when one is forgotten.
    bare = subprocess.run(
        [sys.executable, str(_SCRIPT), "--issue", "999", "--repo", str(tmp_path / "repo")],
        capture_output=True, text=True,
    )
    assert bare.returncode == 2, bare.stdout
    assert "--rederive-index" in bare.stderr
