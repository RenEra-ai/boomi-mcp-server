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


def _archive_gate(tmp_path, run, prompts, accept_new=False):
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
    if accept_new:
        args += ["--accept-new"]
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


def _reseal(root):
    """Recompute the index's digest into the manifest after a test edits it.

    The archiver now refuses an archive that already differs from its own
    manifest, which is correct — that is external damage, and the repo's archive
    scanner rejects it too. But it means a test that simulates a stale row by
    writing `index.jsonl` directly has to leave the archive CONSISTENT, or it is
    exercising the damage check rather than the behaviour it means to pin.
    """
    import hashlib

    index, sums = root / "index.jsonl", root / "SHA256SUMS"
    digest = hashlib.sha256(index.read_bytes()).hexdigest()
    sums.write_text("\n".join(
        (digest + "  index.jsonl") if line.endswith("  index.jsonl") else line
        for line in sums.read_text().splitlines() if line) + "\n")


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
    _reseal(root)

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
    _reseal(root)

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


@pytest.mark.parametrize("junk,label", [
    (["9c5d0467"], "a list — unhashable in the membership test"),
    (12345, "an integer — not subscriptable in the refusal message"),
    ({"sha": "9c5d0467"}, "an object — unhashable too"),
    (True, "a bool, which is not a digest however it compares"),
])
def test_a_non_STRING_attested_digest_refuses_instead_of_raising(tmp_path, junk, label):
    """Resolved is not the same fact as usable.

    `_dig` answers "is there a value here", and every one of these has one — so
    each passed the resolution check and then raised deeper in, AFTER the
    destination directory existed. The exception skipped the discard, and the
    leftover then tripped the overwrite refusal, so the operator's corrected
    retry was blocked. That is the identical poisoned-destination failure this
    same batch already fixed once for a malformed prompt block, which is why the
    check is now a type check and not a presence check.
    """
    attestation = _real_attestation()
    attestation["prompt"]["actualSha256"] = junk

    run = tmp_path / "cdx-gate-review.JUNKDIG"
    run.mkdir()
    (run / "start.json").write_text(json.dumps({"threadId": "t-j", "socket": "/s"}) + "\n")
    (run / "review.md").write_text("a review\n")
    (run / "attestation.json").write_text(json.dumps(attestation) + "\n")

    prompts = tmp_path / "prompts"
    prompts.mkdir()
    (prompts / "prompt").write_bytes(b"some prompt\n")

    result, root = _archive_gate(tmp_path, run, prompts)
    assert result.returncode == 1, (label, result.stdout)
    assert "Traceback" not in result.stderr, (label, result.stderr)
    assert not (root / "architect-reviews" / run.name).exists(), label

    retry, _ = _archive_gate(tmp_path, run, prompts)
    assert "refusing to overwrite" not in retry.stderr, (label, retry.stderr)


def test_rederiving_an_index_this_script_did_not_write_is_REFUSED(tmp_path):
    """The destructive case, built from a REAL foreign archive's field names.

    Issues 152, 153, 171 and 175 carry row fields no derivation here produces.
    Replacing those rows with this script's narrower shape would discard what
    their original producer recorded, irreversibly, and `--issue` takes any
    number — one typo away. So an unrecognised field declines the whole run
    rather than silently narrowing the archive.
    """
    run, prompts = _gate_run(tmp_path)
    result, root = _archive_gate(tmp_path, run, prompts)
    assert result.returncode == 0, result.stderr

    index = root / "index.jsonl"
    header, line = index.read_text().splitlines()
    foreign = dict(json.loads(line), ordinal=7, ledger_cited=True,
                   scope_provenance="collector", reconciled_disposition="fixed")
    index.write_text(header + "\n" + json.dumps(foreign, sort_keys=True) + "\n")
    _reseal(root)

    result = _rederive(tmp_path)
    assert result.returncode != 0, result.stdout
    for field in ("ordinal", "ledger_cited", "scope_provenance", "reconciled_disposition"):
        assert field in result.stderr, field
    # ...and the index is untouched, which is the whole point of refusing.
    assert json.loads(index.read_text().splitlines()[1]) == foreign


def test_rederiving_never_blanks_a_value_it_cannot_source(tmp_path):
    """Twenty-four architect rows elsewhere carry a reviewed sha this cannot read.

    The attestation carries no reviewed sha, so a row this script CREATES gets
    null. That is a fact about the derivation, not about the field — other
    producers populate it. A re-derivation that treated its own inability to
    read a value as evidence the value is absent would erase all of them.
    """
    run, prompts = _gate_run(tmp_path)
    result, root = _archive_gate(tmp_path, run, prompts)
    assert result.returncode == 0, result.stderr

    index = root / "index.jsonl"
    header, line = index.read_text().splitlines()
    populated = dict(json.loads(line), reviewed_sha="a" * 40)
    index.write_text(header + "\n" + json.dumps(populated, sort_keys=True) + "\n")
    _reseal(root)

    assert _rederive(tmp_path).returncode == 0
    after = json.loads(index.read_text().splitlines()[1])
    assert after["reviewed_sha"] == "a" * 40
    # ...while a field it CAN source is still corrected in the same pass.
    assert after["prompt_sha256"] == populated["prompt_sha256"]


def test_rederiving_does_not_resurrect_a_sha_the_bytes_derive_as_ABSENT(tmp_path):
    """Null is a VERDICT on a commit-review row, not an absence.

    The collector withholds the reviewed-sha marker for a round that failed, so
    that a failed round can never silently shrink the next review's scope. A
    blanket "never replace a value with null" — which is how the architect rows
    were first protected — also protected THIS null, resurrecting a sha onto a
    round the bytes say did not complete and restating a completion the
    collector deliberately declined to record.

    So the exemption is per KIND: an architect row's reviewed sha is unsourceable
    here and is left alone; a commit-review row's is sourced, including its null.
    """
    run = tmp_path / "cdx-review.FAILED1"
    run.mkdir()
    (run / "start.json").write_text(json.dumps({"threadId": "t-f"}) + "\n")
    (run / "baseline").write_text("b" * 40 + "\n")
    (run / "start-head").write_text("c" * 40 + "\n")
    (run / "dirty").write_text("false\n")
    (run / "scope").write_text("auto\n")
    (run / "phase").write_text("failed\n")
    (run / "review.json").write_text(json.dumps({"scope": "auto"}) + "\n")

    root = tmp_path / "repo" / "docs" / "architecture" / "evidence" / "issue-999"
    root.mkdir(parents=True, exist_ok=True)
    (root / "index.jsonl").write_text(json.dumps(
        {"generated_at": "x", "issue": 999, "schema_version": 1, "source_tip": "abc"}) + "\n")
    result = subprocess.run(
        [sys.executable, str(_SCRIPT), "--issue", "999", "--kind", "commit-review",
         "--run-dir", str(run), "--logical-loop", "L2", "--repo", str(tmp_path / "repo")],
        capture_output=True, text=True)
    assert result.returncode == 0, result.stderr

    index = root / "index.jsonl"
    header, line = index.read_text().splitlines()
    row = json.loads(line)
    assert row["status"] == "failed" and row["reviewed_sha"] is None

    # A stale row carrying a sha from before the round was known to have failed.
    index.write_text(header + "\n" + json.dumps(
        dict(row, reviewed_sha="d" * 40), sort_keys=True) + "\n")
    _reseal(root)

    assert _rederive(tmp_path).returncode == 0
    after = json.loads(index.read_text().splitlines()[1])
    assert after["reviewed_sha"] is None, after
    assert after["status"] == "failed"


def test_a_richer_value_under_a_SHARED_key_is_refused_not_narrowed(tmp_path):
    """Key-set equality is not shape equality.

    The first version of this guard compared only top-level key names, so a
    producer recording a richer value under a key this derivation also produces
    passed straight through and was silently replaced by the narrower one. The
    never-subtract rule cannot catch it either — a narrowed value is not null.
    """
    run, prompts = _gate_run(tmp_path)
    result, root = _archive_gate(tmp_path, run, prompts)
    assert result.returncode == 0, result.stderr

    index = root / "index.jsonl"
    header, line = index.read_text().splitlines()
    row = json.loads(line)
    richer = dict(row, files={k: {"sha256": v, "bytes": 12, "mode": "100644"}
                              for k, v in row["files"].items()})
    index.write_text(header + "\n" + json.dumps(richer, sort_keys=True) + "\n")
    _reseal(root)

    result = _rederive(tmp_path)
    assert result.returncode != 0, result.stdout
    assert "narrow" in result.stderr and "files" in result.stderr
    assert json.loads(index.read_text().splitlines()[1]) == richer


def test_rederiving_an_unchanged_archive_leaves_its_CHECKSUMS_alone(tmp_path):
    """The one path here that writes outside the index.

    The checksum manifest covers archived FILES, none of which a re-derivation
    changes. Its ordering is not stable, so regenerating it on an archive with
    nothing to fix moves real bytes for no reason — measured on three archives
    this slice does not own.
    """
    run, prompts = _gate_run(tmp_path)
    result, root = _archive_gate(tmp_path, run, prompts)
    assert result.returncode == 0, result.stderr

    sums = root / "SHA256SUMS"

    # SCRAMBLE the order first. Comparing bytes alone was vacuous: this archive
    # is small enough that a regeneration reproduces the same ordering, so the
    # test passed even with the guard removed. A regeneration writes in sorted
    # order, so a deliberately reversed file survives only if nothing wrote.
    scrambled = b"".join(
        l + b"\n" for l in reversed(sums.read_bytes().split(b"\n")) if l
    )
    sums.write_bytes(scrambled)

    out = _rederive(tmp_path)
    assert out.returncode == 0
    assert "0 index row(s)" in out.stdout
    assert "untouched" in out.stdout
    assert sums.read_bytes() == scrambled, "the checksum manifest was rewritten"


def test_an_index_whose_first_line_is_a_ROW_is_refused(tmp_path):
    """A header-less index would otherwise lose its first round at exit 0."""
    run, prompts = _gate_run(tmp_path)
    result, root = _archive_gate(tmp_path, run, prompts)
    assert result.returncode == 0, result.stderr

    index = root / "index.jsonl"
    _header, line = index.read_text().splitlines()
    index.write_text(line + "\n")
    _reseal(root)

    out = _rederive(tmp_path)
    assert out.returncode != 0, out.stdout
    assert "no header line" in out.stderr
    assert index.read_text().strip() == line


def test_a_missing_or_empty_index_says_what_to_do(tmp_path):
    """Actionable, not a raw traceback — no damage either way, but readable."""
    (tmp_path / "repo" / "docs" / "architecture" / "evidence" / "issue-999").mkdir(parents=True)
    missing = _rederive(tmp_path)
    assert missing.returncode != 0
    assert "no index to re-derive" in missing.stderr
    assert "Traceback" not in missing.stderr

    (tmp_path / "repo" / "docs" / "architecture" / "evidence" / "issue-999"
     / "index.jsonl").write_text("")
    empty = _rederive(tmp_path)
    assert empty.returncode != 0
    assert "is empty" in empty.stderr
    assert "Traceback" not in empty.stderr


@pytest.mark.parametrize("blank", ["   ", "\t", "\n", "   "])
def test_a_whitespace_only_attestation_fact_is_not_RESOLVED(tmp_path, blank):
    """`not "   "` is False, so a blank fact passed the non-empty test."""
    attestation = _real_attestation()
    attestation["parsedVerdict"] = blank

    run = tmp_path / "cdx-gate-review.BLANKFACT"
    run.mkdir()
    (run / "start.json").write_text(json.dumps({"threadId": "t-b", "socket": "/s"}) + "\n")
    (run / "review.md").write_text("a review\n")
    (run / "attestation.json").write_text(json.dumps(attestation) + "\n")

    prompts = tmp_path / "prompts"
    prompts.mkdir()
    (prompts / "prompt").write_bytes(b"some prompt\n")

    result, root = _archive_gate(tmp_path, run, prompts)
    assert result.returncode == 1, result.stdout
    assert "parsedVerdict" in result.stderr or "verdict" in result.stderr
    assert not (root / "architect-reviews" / run.name).exists()


def test_every_field_derived_as_a_CONSTANT_null_is_listed_unsourceable():
    """Derive the exemption list from the derivation, do not trust the list.

    Writing a hardcoded null is not sourcing a value — it is having nothing to
    say — so any such field must be exempt from re-derivation or it will erase
    whatever a real producer recorded there. Hand-listing which fields those are
    is the same hand-model this module has now been burned by three times, so
    the list is checked against the code that produces the rows.

    The check builds a round where every SOURCEABLE field has a value. Anything
    still null afterwards is a constant, and must be listed.
    """
    module = _archiver_module()

    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        root = base / "evidence"
        (root / "commit-reviews").mkdir(parents=True)
        run = root / "commit-reviews" / "cdx-review.CONST1"
        run.mkdir()
        sha = "e" * 40
        (run / "start.json").write_text(json.dumps({"threadId": "t", "startedAt": "now"}) + "\n")
        (run / "baseline").write_text("b" * 40 + "\n")
        (run / "start-head").write_text(sha + "\n")
        (run / "last-reviewed-sha").write_text(sha + "\n")   # completed
        (run / "dirty").write_text("false\n")
        (run / "scope").write_text("auto\n")
        (run / "teardown").write_text("confirmed stopped\n")
        (run / "review.json").write_text(json.dumps({"scope": "auto"}) + "\n")

        row = module.derive_row("commit-review", Path("/tmp/src"), run, root, "L2", None, None)

    constant_nulls = sorted(k for k, v in row.items() if v is None)
    listed = set(module.UNSOURCEABLE_BY_KIND.get("commit-review", ()))
    assert set(constant_nulls) <= listed, {
        "derived_as_null_but_not_exempt": sorted(set(constant_nulls) - listed),
        "this_field_would_erase_a_real_producers_value": True,
    }
    # ...and the exemption is not vacuous — this row really does carry one.
    assert constant_nulls, "no constant null found; the check proves nothing"


def test_a_recorded_verdict_on_a_commit_review_row_survives_rederivation(tmp_path):
    """The measured case: six rows in a closed issue's archive.

    Sweeping the real archives to verify the refusal, the run modified two of
    them, and issue 180's commit-review rows each lost a verdict of "clean" or
    "findings". The key-set guard could not see it — `verdict` exists in both
    shapes — and the sourced-null rule actively preferred the null, because a
    hardcoded None is indistinguishable from a computed one at the call site.
    """
    run = tmp_path / "cdx-review.VERDICT1"
    run.mkdir()
    sha = "f" * 40
    (run / "start.json").write_text(json.dumps({"threadId": "t-v"}) + "\n")
    (run / "baseline").write_text("b" * 40 + "\n")
    (run / "start-head").write_text(sha + "\n")
    (run / "last-reviewed-sha").write_text(sha + "\n")
    (run / "dirty").write_text("false\n")
    (run / "scope").write_text("auto\n")
    (run / "teardown").write_text("confirmed stopped\n")
    (run / "review.json").write_text(json.dumps({"scope": "auto"}) + "\n")

    root = tmp_path / "repo" / "docs" / "architecture" / "evidence" / "issue-999"
    root.mkdir(parents=True, exist_ok=True)
    (root / "index.jsonl").write_text(json.dumps(
        {"generated_at": "x", "issue": 999, "schema_version": 1, "source_tip": "abc"}) + "\n")
    assert subprocess.run(
        [sys.executable, str(_SCRIPT), "--issue", "999", "--kind", "commit-review",
         "--run-dir", str(run), "--logical-loop", "L2", "--repo", str(tmp_path / "repo")],
        capture_output=True, text=True).returncode == 0

    index = root / "index.jsonl"
    header, line = index.read_text().splitlines()
    recorded = dict(json.loads(line), verdict="findings")
    index.write_text(header + "\n" + json.dumps(recorded, sort_keys=True) + "\n")
    _reseal(root)

    assert _rederive(tmp_path).returncode == 0
    assert json.loads(index.read_text().splitlines()[1])["verdict"] == "findings"


def _noncanonical_but_consistent(root):
    """Rewrite the index in a bytewise-noncanonical form and make sums agree.

    This is the realistic state after someone hand-edits a row and regenerates
    the manifest: the archive is internally CONSISTENT, just not in the form
    this script would emit.
    """
    index, sums = root / "index.jsonl", root / "SHA256SUMS"
    lines = [l for l in index.read_text().splitlines() if l.strip()]
    scrambled = [lines[0]] + [
        json.dumps({k: json.loads(l)[k] for k in reversed(list(json.loads(l)))})
        for l in lines[1:]
    ]
    index.write_text("\n\n".join(scrambled))          # blank lines, no trailing newline

    import hashlib

    digest = hashlib.sha256(index.read_bytes()).hexdigest()
    rows = [l for l in sums.read_text().splitlines() if l]
    sums.write_text("\n".join(
        (digest + "  index.jsonl") if l.endswith("  index.jsonl") else l for l in rows) + "\n")
    return index.read_bytes(), sums.read_bytes()


def test_canonicalising_the_index_also_refreshes_the_CHECKSUMS(tmp_path):
    """Bytes moved and rows did not — two different questions.

    A semantically identical but bytewise-noncanonical index is still rewritten
    in canonical form, so gating the checksum refresh on the ROW count left the
    manifest describing bytes that were no longer there. Reproduced before
    fixing: exit 0, and the archive scanner rejects the result.
    """
    run, prompts = _gate_run(tmp_path)
    result, root = _archive_gate(tmp_path, run, prompts)
    assert result.returncode == 0, result.stderr

    before_index, before_sums = _noncanonical_but_consistent(root)

    out = _rederive(tmp_path)
    assert out.returncode == 0, out.stderr
    assert (root / "index.jsonl").read_bytes() != before_index, "the index was not canonicalised"
    assert (root / "SHA256SUMS").read_bytes() != before_sums, "the manifest was left stale"

    import hashlib

    listed = {}
    for line in (root / "SHA256SUMS").read_text().splitlines():
        if line:
            digest, name = line.split("  ", 1)
            listed[name] = digest
    assert listed["index.jsonl"] == hashlib.sha256(
        (root / "index.jsonl").read_bytes()).hexdigest(), "manifest disagrees with the index"

    # ...and it does not claim to have re-derived a row, because none moved.
    assert "0 index row(s)" in out.stdout
    assert "canonical form" in out.stdout


def test_an_already_canonical_index_is_not_rewritten_at_all(tmp_path):
    """The other half: no bytes move, so neither file is touched.

    Without this the fix above could be satisfied by simply always regenerating,
    which is the behaviour a previous round removed for moving real bytes in
    archives this slice does not own.
    """
    run, prompts = _gate_run(tmp_path)
    result, root = _archive_gate(tmp_path, run, prompts)
    assert result.returncode == 0, result.stderr

    index, sums = root / "index.jsonl", root / "SHA256SUMS"
    scrambled_sums = b"".join(
        l + b"\n" for l in reversed(sums.read_bytes().split(b"\n")) if l
    )
    sums.write_bytes(scrambled_sums)
    before_index = index.read_bytes()

    out = _rederive(tmp_path)
    assert out.returncode == 0, out.stderr
    assert index.read_bytes() == before_index
    assert sums.read_bytes() == scrambled_sums, "the manifest was rewritten for nothing"
    assert "untouched" in out.stdout


def test_a_file_a_ROW_references_cannot_vanish_unreported(tmp_path):
    """A deleted file, caught by the pre-flight before anything is written.

    Two layers see this — the archive-level accounting and the row's own file
    map — and which one reports it matters. The pre-flight runs FIRST, so the
    index is still byte-untouched when the refusal happens, and restoring the
    file makes the run simply retryable. When the row-level check reported it,
    the index had already been rewritten, and the next run then saw an
    already-canonical index, decided there was nothing to do, and exited 0 with
    a stale manifest digest.
    """
    run, prompts = _gate_run(tmp_path)
    result, root = _archive_gate(tmp_path, run, prompts)
    assert result.returncode == 0, result.stderr

    victim = root / "architect-reviews" / run.name / "review.md"
    assert victim.is_file()
    victim.unlink()

    _noncanonical_but_consistent(root)
    _reseal(root)
    before = (root / "index.jsonl").read_bytes()

    out = _rederive(tmp_path)
    assert out.returncode != 0, out.stdout
    assert "no longer on disk" in out.stderr
    assert "review.md" in out.stderr
    assert "Traceback" not in out.stderr
    assert (root / "index.jsonl").read_bytes() == before, \
        "a refused run must leave the index untouched, or it is not retryable"

    # ...and once the file is restored, the run really does succeed.
    victim.write_text("a review\n")
    _reseal(root)
    retry = _rederive(tmp_path)
    assert retry.returncode == 0, retry.stderr


def test_a_file_NO_ROW_references_cannot_vanish_unreported_either(tmp_path):
    """The outer layer, and the one that actually needed building.

    Most of what an archive holds is referenced by no index row at all — in the
    issue-155 archive, most files — so for those the checksum manifest is the
    ONLY record that they were ever here. Rebuilding it from disk absorbed their
    disappearance silently: a scanner rejection of listed-but-absent became a
    manifest that simply no longer listed them, at exit 0. And because this
    tool's own closing line says to `git add` the directory, which stages
    deletions, the erasure would be durable rather than recoverable.

    The row-level check above cannot see these files. This one can.
    """
    run, prompts = _gate_run(tmp_path)
    result, root = _archive_gate(tmp_path, run, prompts)
    assert result.returncode == 0, result.stderr

    # A capture: archived evidence that no index row references.
    unreferenced = root / "captures" / "cap-155-x" / "readback.xml"
    unreferenced.parent.mkdir(parents=True)
    unreferenced.write_text("<component/>\n")

    second = tmp_path / "cdx-gate-review.LISTIT"
    second.mkdir()
    for name in ("start.json", "attestation.json", "review.md"):
        (second / name).write_text((run / name).read_text())
    # A capture arrived outside this script, so listing it is now something the
    # operator ASKS for. Without the flag this refuses — checked here, because
    # the silent absorption it replaces is exactly how an unlisted file used to
    # become listed evidence.
    refused, _ = _archive_gate(tmp_path, second, prompts)
    assert refused.returncode != 0, refused.stdout
    assert "never listed" in refused.stderr and "readback.xml" in refused.stderr

    relist, _ = _archive_gate(tmp_path, second, prompts, accept_new=True)
    assert relist.returncode == 0, relist.stderr
    assert "ACCEPTED into the manifest" in relist.stdout
    assert "readback.xml" in relist.stdout
    assert "captures/cap-155-x/readback.xml" in (root / "SHA256SUMS").read_text()

    unreferenced.unlink()

    _noncanonical_but_consistent(root)
    out = _rederive(tmp_path)
    assert out.returncode != 0, out.stdout
    assert "no longer on disk" in out.stderr
    assert "readback.xml" in out.stderr
    assert "only record" in out.stderr
    assert "Traceback" not in out.stderr


def test_a_STRAY_file_is_reported_rather_than_recorded_as_evidence(tmp_path):
    """The other direction: an unlisted file must not be quietly promoted.

    A re-derivation adds no file, so a name on disk that the manifest never
    listed arrived from somewhere else. Listing it would record it as archived
    evidence on this tool's authority, which it does not have.
    """
    run, prompts = _gate_run(tmp_path)
    result, root = _archive_gate(tmp_path, run, prompts)
    assert result.returncode == 0, result.stderr

    (root / "architect-reviews" / run.name / "unreviewed.md").write_text("not evidence\n")

    _noncanonical_but_consistent(root)
    out = _rederive(tmp_path)
    assert out.returncode != 0, out.stdout
    assert "unreviewed.md" in out.stderr
    # Refused as an unaccounted ARCHIVE difference by the pre-flight, not as a
    # narrowing and not by the row-level check. Nothing about a re-derivation
    # adds a file, so a name on disk the manifest never listed came from outside
    # this tool — and saying so is what sends the operator to the right place.
    assert "never listed" in out.stderr
    assert "narrow" not in out.stderr


def test_ARCHIVING_a_round_may_add_names_but_still_never_loses_one(tmp_path):
    """`grow` is not `anything goes` — the two callers differ in one direction.

    Archiving legitimately adds the round's own files. It must still refuse if a
    name vanishes, because nothing about copying a new round removes an old one.
    """
    run, prompts = _gate_run(tmp_path)
    first, root = _archive_gate(tmp_path, run, prompts)
    assert first.returncode == 0, first.stderr

    (root / "architect-reviews" / run.name / "review.md").unlink()

    second_run = tmp_path / "cdx-gate-review.SECOND"
    second_run.mkdir()
    for name in ("start.json", "attestation.json", "review.md"):
        (second_run / name).write_text((run / name).read_text())
    out, _ = _archive_gate(tmp_path, second_run, prompts)
    assert out.returncode != 0, out.stdout
    assert "no longer on disk" in out.stderr


def test_a_CRLF_index_is_canonicalised_rather_than_compared_as_text(tmp_path):
    """`read_text()` translates newlines, so CRLF compared equal to LF.

    The gate claimed to compare bytes and did not, leaving a CRLF index
    un-canonicalised at exit 0 while reporting nothing to do.
    """
    run, prompts = _gate_run(tmp_path)
    result, root = _archive_gate(tmp_path, run, prompts)
    assert result.returncode == 0, result.stderr

    index = root / "index.jsonl"
    index.write_bytes(index.read_bytes().replace(b"\n", b"\r\n"))
    import hashlib

    sums = root / "SHA256SUMS"
    digest = hashlib.sha256(index.read_bytes()).hexdigest()
    sums.write_text("\n".join(
        (digest + "  index.jsonl") if l.endswith("  index.jsonl") else l
        for l in sums.read_text().splitlines() if l) + "\n")

    out = _rederive(tmp_path)
    assert out.returncode == 0, out.stderr
    assert b"\r\n" not in index.read_bytes(), "the CRLF index was left un-canonicalised"
    listed = {}
    for line in sums.read_text().splitlines():
        if line:
            d, n = line.split("  ", 1)
            listed[n] = d
    assert listed["index.jsonl"] == hashlib.sha256(index.read_bytes()).hexdigest()


def test_a_file_whose_BYTES_changed_under_the_same_path_is_refused(tmp_path):
    """The dimension the name-set comparison could not see.

    Comparing only which names are listed let an archived file be rewritten in
    place and pass straight through — and the manifest rebuild then recorded its
    NEW digest, converting a hash mismatch the archive scanner would have caught
    into evidence that looks valid. For a capture no index row references, that
    manifest entry is the only thing that would ever have disagreed.
    """
    run, prompts = _gate_run(tmp_path)
    result, root = _archive_gate(tmp_path, run, prompts)
    assert result.returncode == 0, result.stderr

    tampered = root / "architect-reviews" / run.name / "review.md"
    original = tampered.read_bytes()
    tampered.write_text("a review that says something else entirely\n")

    _noncanonical_but_consistent(root)
    _reseal(root)
    out = _rederive(tmp_path)
    assert out.returncode != 0, out.stdout
    assert "its bytes have changed" in out.stderr
    assert "review.md" in out.stderr

    # The manifest still records the ORIGINAL digest — the tamper was not
    # certified — and restoring the bytes makes the run retryable.
    import hashlib

    listed = {}
    for line in (root / "SHA256SUMS").read_text().splitlines():
        if line:
            digest, name = line.split("  ", 1)
            listed[name] = digest
    assert listed[f"architect-reviews/{run.name}/review.md"] == \
        hashlib.sha256(original).hexdigest()

    tampered.write_bytes(original)
    _reseal(root)
    assert _rederive(tmp_path).returncode == 0


def test_ARCHIVING_refuses_before_the_destination_exists(tmp_path):
    """Raising after the round directory exists makes the failure permanent.

    The overwrite refusal then blocks the corrected run, so an operator who
    repairs exactly what the check complained about still cannot proceed. The
    check runs before anything is created, so the repair is enough.
    """
    run, prompts = _gate_run(tmp_path)
    first, root = _archive_gate(tmp_path, run, prompts)
    assert first.returncode == 0, first.stderr

    victim = root / "architect-reviews" / run.name / "review.md"
    original = victim.read_bytes()
    victim.unlink()

    second = tmp_path / "cdx-gate-review.AFTERDMG"
    second.mkdir()
    for name in ("start.json", "attestation.json", "review.md"):
        (second / name).write_text((run / name).read_text())

    out, _ = _archive_gate(tmp_path, second, prompts)
    assert out.returncode != 0, out.stdout
    assert "no longer on disk" in out.stderr
    assert not (root / "architect-reviews" / second.name).exists(), \
        "the destination was created before the check — the failure is now permanent"

    # Repairing what the check named is sufficient: the same command succeeds.
    victim.write_bytes(original)
    retry, _ = _archive_gate(tmp_path, second, prompts)
    assert retry.returncode == 0, retry.stderr
    assert (root / "architect-reviews" / second.name / "review.md").is_file()


def test_a_PRE_EXISTING_unlisted_file_is_not_laundered_by_archiving_a_round(tmp_path):
    """The exemption that could not admit what it was written for.

    The pre-flight ran before the new round's directory existed, so nothing it
    could see had been created by that invocation — yet it exempted `appeared`
    names while archiving, on the reasoning that a new round brings new files.
    The only names the exemption could actually admit were ones that predated
    the run, and the manifest rewrite then recorded them as valid evidence:
    exactly the on-disk-but-unlisted state the accounting exists to catch.
    """
    run, prompts = _gate_run(tmp_path)
    first, root = _archive_gate(tmp_path, run, prompts)
    assert first.returncode == 0, first.stderr

    # Arrived from somewhere else, before this next round is archived.
    intruder = root / "commit-reviews" / "cdx-review.NOTMINE" / "review.json"
    intruder.parent.mkdir(parents=True)
    intruder.write_text(json.dumps({"scope": "auto"}) + "\n")

    second = tmp_path / "cdx-gate-review.NEXTONE"
    second.mkdir()
    for name in ("start.json", "attestation.json", "review.md"):
        (second / name).write_text((run / name).read_text())

    out, _ = _archive_gate(tmp_path, second, prompts)
    assert out.returncode != 0, out.stdout
    assert "never listed" in out.stderr
    assert "cdx-review.NOTMINE/review.json" in out.stderr
    assert "cdx-review.NOTMINE/review.json" not in (root / "SHA256SUMS").read_text()
    assert not (root / "architect-reviews" / second.name).exists()


def test_accepting_a_foreign_file_is_RECORDED_not_silent(tmp_path):
    """Absorbing one silently is how unaccounted evidence becomes accounted.

    The flow is real — most files in a live archive are captures that arrived
    outside this script — so the flag exists. What it must not be is quiet: the
    accepted names are printed, so the act lands in whatever record the operator
    is keeping rather than only in the manifest it changes.
    """
    run, prompts = _gate_run(tmp_path)
    first, root = _archive_gate(tmp_path, run, prompts)
    assert first.returncode == 0, first.stderr

    capture = root / "captures" / "cap-x" / "readback.xml"
    capture.parent.mkdir(parents=True)
    capture.write_text("<component/>\n")

    second = tmp_path / "cdx-gate-review.WITHFLAG"
    second.mkdir()
    for name in ("start.json", "attestation.json", "review.md"):
        (second / name).write_text((run / name).read_text())

    out, _ = _archive_gate(tmp_path, second, prompts, accept_new=True)
    assert out.returncode == 0, out.stderr
    assert "ACCEPTED into the manifest, not produced by this round" in out.stdout
    assert "captures/cap-x/readback.xml" in out.stdout
    assert "captures/cap-x/readback.xml" in (root / "SHA256SUMS").read_text()

    # The flag admits an ARRIVAL. It must not also excuse a disappearance.
    (root / "architect-reviews" / run.name / "review.md").unlink()
    third = tmp_path / "cdx-gate-review.THIRD"
    third.mkdir()
    for name in ("start.json", "attestation.json", "review.md"):
        (third / name).write_text((run / name).read_text())
    lost, _ = _archive_gate(tmp_path, third, prompts, accept_new=True)
    assert lost.returncode != 0, lost.stdout
    assert "no longer on disk" in lost.stderr


@pytest.mark.parametrize("bad,why", [
    ("../ESCAPED", "escapes the kind directory"),
    ("../../ESCAPED2", "escapes the issue archive entirely"),
    ("a/b", "a nested path, not a name"),
    ("..", "the parent itself"),
    ("", "empty"),
])
def test_a_NAME_that_is_a_PATH_is_refused(tmp_path, bad, why):
    """Joined unchecked, this wrote rounds outside the archive at exit 0.

    `../ESCAPED` landed outside its kind directory; `../../ESCAPED2` landed
    outside the issue archive altogether, covered by NO manifest line while an
    index row still claimed it. A completion claim citing that round would never
    be captured by a `git add` of the issue's evidence directory — which is the
    single thing this archive exists to make checkable. An absolute name wrote
    nine files outside the repository before failing on something unrelated.
    """
    run, prompts = _gate_run(tmp_path)
    root = tmp_path / "repo" / "docs" / "architecture" / "evidence" / "issue-999"
    root.mkdir(parents=True, exist_ok=True)
    (root / "index.jsonl").write_text(json.dumps({
        "generated_at": "x", "issue": 999, "schema_version": 1, "source_tip": "abc",
    }) + "\n")

    result = subprocess.run(
        [sys.executable, str(_SCRIPT), "--issue", "999", "--kind", "architect-review",
         "--run-dir", str(run), "--logical-loop", "L3", "--repo", str(tmp_path / "repo"),
         "--prompts", str(prompts), "--name", bad],
        capture_output=True, text=True)

    assert result.returncode == 2, (why, result.stdout)
    assert "single directory name" in result.stderr
    escaped = [p for p in (tmp_path / "repo").rglob("ESCAPED*")]
    assert escaped == [], (why, escaped)
    assert (root / "index.jsonl").read_text().count("\n") == 1, "a row was written"


def test_a_name_that_IS_a_single_component_still_works(tmp_path):
    """The control: the wave-gate rounds in this repo are archived under one."""
    run, prompts = _gate_run(tmp_path)
    root = tmp_path / "repo" / "docs" / "architecture" / "evidence" / "issue-999"
    root.mkdir(parents=True, exist_ok=True)
    (root / "index.jsonl").write_text(json.dumps({
        "generated_at": "x", "issue": 999, "schema_version": 1, "source_tip": "abc",
    }) + "\n")

    result = subprocess.run(
        [sys.executable, str(_SCRIPT), "--issue", "999", "--kind", "architect-review",
         "--run-dir", str(run), "--logical-loop", "L3", "--repo", str(tmp_path / "repo"),
         "--prompts", str(prompts), "--name", "renamed-round"],
        capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert (root / "architect-reviews" / "renamed-round" / "review.md").is_file()


def test_a_failed_archive_is_ROLLED_BACK_and_the_retry_succeeds(tmp_path):
    """Inconsistent AND unretryable was the measured outcome of any failed write.

    The destination existed afterwards, so the overwrite refusal blocked the
    identical command even once the operator had fixed exactly what the error
    named. A read-only manifest is the cheapest way to produce that failure.
    """
    run, prompts = _gate_run(tmp_path)
    first, root = _archive_gate(tmp_path, run, prompts)
    assert first.returncode == 0, first.stderr

    index_before = (root / "index.jsonl").read_bytes()
    sums = root / "SHA256SUMS"
    sums.chmod(0o444)

    second = tmp_path / "cdx-gate-review.WILLFAIL"
    second.mkdir()
    for name in ("start.json", "attestation.json", "review.md"):
        (second / name).write_text((run / name).read_text())

    out, _ = _archive_gate(tmp_path, second, prompts)
    try:
        assert out.returncode != 0, out.stdout
        assert "rolled back" in out.stderr
        assert (root / "index.jsonl").read_bytes() == index_before, "the row survived"
        assert not (root / "architect-reviews" / second.name).exists(), \
            "the partial round survived and now blocks the retry"
    finally:
        sums.chmod(0o644)

    # Fix exactly what the message named; the identical command now succeeds.
    retry, _ = _archive_gate(tmp_path, second, prompts)
    assert retry.returncode == 0, retry.stderr
    assert (root / "architect-reviews" / second.name / "review.md").is_file()


def test_a_file_inside_an_ARCHIVED_round_is_not_absorbable(tmp_path):
    """The two derived artifacts have different refresh scopes.

    The manifest is rewritten over every file; the index only appends the new
    row. So a file that appears inside an already-archived round could be listed
    as evidence while that round's own row still denies it — and a later
    re-derivation then refuses on the disagreement with nothing the accept flag
    can do about it. Captures live outside round directories; anything inside
    one belongs to its round.
    """
    run, prompts = _gate_run(tmp_path)
    first, root = _archive_gate(tmp_path, run, prompts)
    assert first.returncode == 0, first.stderr

    late = root / "architect-reviews" / run.name / "late-sidecar.json"
    late.write_text(json.dumps({"arrived": "after the round was archived"}) + "\n")

    second = tmp_path / "cdx-gate-review.NEXT2"
    second.mkdir()
    for name in ("start.json", "attestation.json", "review.md"):
        (second / name).write_text((run / name).read_text())

    out, _ = _archive_gate(tmp_path, second, prompts, accept_new=True)
    assert out.returncode != 0, out.stdout
    assert "inside an already archived round" in out.stderr
    assert "late-sidecar.json" in out.stderr
    assert "late-sidecar.json" not in (root / "SHA256SUMS").read_text()

    # And the way out that the message names actually works.
    late.unlink()
    retry, _ = _archive_gate(tmp_path, second, prompts)
    assert retry.returncode == 0, retry.stderr
