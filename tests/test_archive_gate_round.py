"""Issue #155: the gate-round archiver, and the one rule it exists to obey.

The archiver's whole job is producing evidence the repository's archive scanner
will accept. A mode that writes an archive that scanner refuses is worse than no
mode at all — the failure surfaces at the gate, long after the round it was
meant to evidence has been torn down.

Run with PYTHONPATH=src (the editable-install .pth is stale):
    PYTHONPATH=src .venv/bin/python -m pytest tests/test_archive_gate_round.py
"""

import os
import json
import shutil
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
    thread_id = "01a0-thread-" + run.name
    (run / "start.json").write_text(
        json.dumps({"threadId": thread_id, "socket": "/s"}) + "\n")
    (run / "review.md").write_text("a review\n")
    digest = hashlib.sha256(prompt_bytes).hexdigest()
    # The REAL collector schema — nested under `prompt`, not a flat key. The
    # first version of this fixture invented `promptSha256`, which is what
    # `collect` prints to stdout, and that invention hid a guard that was inert
    # on every real round. `test_the_fixture_matches_a_real_attestation` below
    # pins this shape against an attestation the repository actually archived.
    # The COMPLETE contract the archive scanner requires of a completed round —
    # teardown, turn status, verdict, artifact path and digest, thread binding,
    # and the prompt digest. The earlier version of this fixture carried only
    # three of those, so the positive test proved the archiver ACCEPTS something
    # the consumer refuses. An architect review caught it. A fixture is only
    # evidence to the extent it matches what the real producer emits.
    review_bytes = (run / "review.md").read_bytes() if (run / "review.md").is_file() else b""
    (run / "attestation.json").write_text(json.dumps({
        "schema": 1, "gateProtocol": 2, "gate": "review",
        "teardown": "confirmed",
        "turn": {"status": "completed", "kind": "turn", "turnToken": 1},
        "parsedVerdict": "NO ISSUES",
        # The SAME thread the session record carries. The scanner requires
        # equality, not presence: an attestation naming a different thread is
        # an attestation of a different session, which is the forgery this
        # binding exists to refuse. The earlier fixture used two ids.
        "start": {"threadId": thread_id, "private": True},
        "artifact": {
            "path": str(run / "review.md"),
            "sha256": hashlib.sha256(review_bytes).hexdigest(),
        },
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
    # Either refusal proves the guard is not inert against a real, mutated
    # attestation; which fires first is not this test's subject.
    assert ("not the attested" in result.stderr
            or "does not resolve ['verdict']" in result.stderr), result.stderr
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
    (run / "attestation.json").write_text(
        json.dumps(_make_coherent(json.loads(real[0].read_text()), run)) + "\n")

    wrong = tmp_path / "wrong-prompts"
    wrong.mkdir()
    (wrong / "prompt").write_bytes(b"not the prompt the gate actually ran\n")

    result, root = _archive_gate(tmp_path, run, wrong)
    assert result.returncode == 1, result.stdout
    # Either refusal proves the guard is not inert against a real, mutated
    # attestation; which one fires first is not this test's subject.
    assert ("not the attested" in result.stderr
            or "does not resolve ['verdict']" in result.stderr), result.stderr
    assert not (root / "architect-reviews" / run.name).exists()


def _make_coherent(att, run):
    """Bind a borrowed attestation to the run directory it is placed in.

    A real attestation names its own session's thread and its own review's
    digest. Copying it into a synthetic run directory leaves both pointing
    elsewhere — which the pre-flight now refuses, correctly, because an
    attestation of a different session is exactly the forgery those bindings
    exist to catch. Tests that borrow one must therefore re-bind it, rather than
    the archiver relaxing a check to accommodate a fixture.
    """
    import hashlib

    att.setdefault("start", {})["threadId"] = json.loads(
        (run / "start.json").read_text())["threadId"]
    review = run / "review.md"
    if review.is_file():
        att.setdefault("artifact", {})["sha256"] = hashlib.sha256(
            review.read_bytes()).hexdigest()
        att["artifact"]["path"] = str(review)
    return att



def _real_round_dir():
    """A round directory whose attestation carries every fact these tests use.

    Selecting `sorted(...)[0]` made every borrower depend on which round sorted
    earliest, so archiving an unrelated round silently changed what they tested
    against — and the current collector writes `parsedVerdict` null for an
    architect gate, so the newest round carries less than these tests need.
    """
    for candidate in sorted(_ARCHITECT_ROUNDS.glob("*/attestation.json")):
        if json.loads(candidate.read_text()).get("parsedVerdict"):
            return candidate.parent
    return sorted(_ARCHITECT_ROUNDS.glob("*/attestation.json"))[0].parent


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
    return json.loads((_real_round_dir() / "attestation.json").read_text())


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
    (run / "attestation.json").write_text(
        json.dumps(_make_coherent(attestation, run)) + "\n")

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
    round_dir = _real_round_dir()

    run = tmp_path / "cdx-gate-review.CONTROL"
    run.mkdir()
    (run / "start.json").write_text(json.dumps({"threadId": "t-c", "socket": "/s"}) + "\n")
    (run / "review.md").write_text("a review\n")
    (run / "attestation.json").write_text(json.dumps(_make_coherent(
        json.loads((round_dir / "attestation.json").read_text()), run)) + "\n")

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
    (run / "attestation.json").write_text(
        json.dumps(_make_coherent(attestation, run)) + "\n")

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
    (run / "attestation.json").write_text(
        json.dumps(_make_coherent(attestation, run)) + "\n")

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
    (second / "attestation.json").write_text(json.dumps(
        _make_coherent(json.loads((second / "attestation.json").read_text()),
                       second)) + "\n")
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
    (second_run / "attestation.json").write_text(json.dumps(
        _make_coherent(json.loads((second_run / "attestation.json").read_text()),
                       second_run)) + "\n")
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
    (second / "attestation.json").write_text(json.dumps(
        _make_coherent(json.loads((second / "attestation.json").read_text()),
                       second)) + "\n")

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
    (second / "attestation.json").write_text(json.dumps(
        _make_coherent(json.loads((second / "attestation.json").read_text()),
                       second)) + "\n")

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
    (second / "attestation.json").write_text(json.dumps(
        _make_coherent(json.loads((second / "attestation.json").read_text()),
                       second)) + "\n")

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
    (third / "attestation.json").write_text(json.dumps(
        _make_coherent(json.loads((third / "attestation.json").read_text()),
                       third)) + "\n")
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
    # The failure is injected at the index append, which happens AFTER the round
    # has been moved into place — so this exercises the rollback proper. A
    # read-only manifest no longer works as an injection point: the manifest is
    # written beside itself and moved in, and a rename succeeds onto a read-only
    # file, so that whole failure mode stopped existing.
    index = root / "index.jsonl"
    index.chmod(0o444)

    second = tmp_path / "cdx-gate-review.WILLFAIL"
    second.mkdir()
    for name in ("start.json", "attestation.json", "review.md"):
        (second / name).write_text((run / name).read_text())
    (second / "attestation.json").write_text(json.dumps(
        _make_coherent(json.loads((second / "attestation.json").read_text()),
                       second)) + "\n")

    out, _ = _archive_gate(tmp_path, second, prompts)
    try:
        assert out.returncode != 0, out.stdout
        assert "rolled back" in out.stderr
        assert index.read_bytes() == index_before, "the row survived"
        assert not (root / "architect-reviews" / second.name).exists(), \
            "the partial round survived and now blocks the retry"
    finally:
        index.chmod(0o644)

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
    (second / "attestation.json").write_text(json.dumps(
        _make_coherent(json.loads((second / "attestation.json").read_text()),
                       second)) + "\n")

    out, _ = _archive_gate(tmp_path, second, prompts, accept_new=True)
    assert out.returncode != 0, out.stdout
    assert "inside an already archived round" in out.stderr
    assert "late-sidecar.json" in out.stderr
    assert "late-sidecar.json" not in (root / "SHA256SUMS").read_text()

    # And the way out that the message names actually works.
    late.unlink()
    retry, _ = _archive_gate(tmp_path, second, prompts)
    assert retry.returncode == 0, retry.stderr


def test_a_failure_while_COPYING_leaves_the_destination_untouched(tmp_path):
    """The window the rollback could not cover, closed by construction instead.

    Copying straight into the destination meant an unreadable source or a full
    disk left a partial round exactly where the next attempt wants to go, and
    the overwrite refusal then blocked the retry permanently. The round is now
    built beside its destination and moved in as one step, so the destination
    appears only when the round is complete — which holds for failure modes
    nobody enumerated, unlike widening a rollback to cover more of the loop.
    """
    run, prompts = _gate_run(tmp_path)
    first, root = _archive_gate(tmp_path, run, prompts)
    assert first.returncode == 0, first.stderr

    second = tmp_path / "cdx-gate-review.UNREADABLE"
    second.mkdir()
    for name in ("start.json", "attestation.json", "review.md"):
        (second / name).write_text((run / name).read_text())
    (second / "attestation.json").write_text(json.dumps(
        _make_coherent(json.loads((second / "attestation.json").read_text()),
                       second)) + "\n")
    (second / "review.md").chmod(0o000)

    index_before = (root / "index.jsonl").read_bytes()
    sums_before = (root / "SHA256SUMS").read_bytes()
    try:
        out, _ = _archive_gate(tmp_path, second, prompts)
        assert out.returncode != 0, out.stdout
        # Refused in the PRE-FLIGHT now, not mid-copy: verifying the attestation's
        # artifact digest has to read the review, so an unreadable one is caught
        # before anything is created rather than while it is being copied. Earlier
        # is strictly better, and the substantive guarantees below are unchanged —
        # no destination, no staging debris, and the repaired retry succeeds.
        assert "cannot be read" in out.stderr
        assert not (root / "architect-reviews" / second.name).exists()
        # No staging debris either — a leftover `.partial-` directory would be
        # listed by the next manifest rewrite as archived evidence.
        leftovers = [p.name for p in (root / "architect-reviews").iterdir()
                     if p.name.startswith(".partial-")]
        assert leftovers == [], leftovers
        assert (root / "index.jsonl").read_bytes() == index_before
        assert (root / "SHA256SUMS").read_bytes() == sums_before
    finally:
        (second / "review.md").chmod(0o644)

    retry, _ = _archive_gate(tmp_path, second, prompts)
    assert retry.returncode == 0, retry.stderr


def test_the_manifest_is_written_ATOMICALLY(tmp_path):
    """Either wholly the old manifest or wholly the new one — never a stub.

    A truncated manifest is worse than a wrong one: an empty manifest reads as a
    brand-new archive to the accounting check, which then has nothing to compare
    against and lets every difference through. So it is written beside itself
    and moved into place.
    """
    run, prompts = _gate_run(tmp_path)
    result, root = _archive_gate(tmp_path, run, prompts)
    assert result.returncode == 0, result.stderr

    sums = root / "SHA256SUMS"
    # A rename replaces a read-only target, so the write survives what a plain
    # truncate-and-fill would have failed on half way through.
    sums.chmod(0o444)
    before = sums.read_bytes()
    try:
        second = tmp_path / "cdx-gate-review.ATOMIC"
        second.mkdir()
        for name in ("start.json", "attestation.json", "review.md"):
            (second / name).write_text((run / name).read_text())
        (second / "attestation.json").write_text(json.dumps(
            _make_coherent(json.loads((second / "attestation.json").read_text()),
                           second)) + "\n")
        out, _ = _archive_gate(tmp_path, second, prompts)
        assert out.returncode == 0, out.stderr
        assert sums.read_bytes() != before
        assert sums.read_text().strip(), "the manifest is not empty"
    finally:
        (root / "SHA256SUMS").chmod(0o644)

    # No staging file survives beside it.
    assert [p.name for p in root.iterdir() if p.name.startswith(".SHA256SUMS")] == []


def test_accept_new_with_rederive_is_REJECTED(tmp_path):
    """A re-derivation adds no file, so there is nothing for the flag to accept.

    Combined, the re-derivation branch returned before the acceptance accounting
    ran at all — so the files it was asked to accept went unrecorded at exit 0,
    or worse were certified into the manifest by the rewrite without the
    round-internal refusal ever being consulted.
    """
    run, prompts = _gate_run(tmp_path)
    result, root = _archive_gate(tmp_path, run, prompts)
    assert result.returncode == 0, result.stderr

    out = subprocess.run(
        [sys.executable, str(_SCRIPT), "--issue", "999", "--rederive-index",
         "--accept-new", "--repo", str(tmp_path / "repo")],
        capture_output=True, text=True)
    assert out.returncode == 2, out.stdout
    assert "no meaning with --rederive-index" in out.stderr

    # ...and each flag alone still works.
    assert _rederive(tmp_path).returncode == 0


def test_a_rollback_that_cannot_COMPLETE_says_so_instead_of_claiming_success(tmp_path):
    """The rollback is itself a mutation, and can itself fail.

    A read-only index is the case that proved it: restoring needed exactly the
    permission that was missing, so the restore raised out of the handler and
    the process died with a traceback after reporting nothing. Now it restores
    only what actually changed — nothing had been written in that case — and if
    a restore genuinely cannot be done it reports an UNKNOWN state rather than
    the reassuring lie that the archive is as it was.
    """
    run, prompts = _gate_run(tmp_path)
    first, root = _archive_gate(tmp_path, run, prompts)
    assert first.returncode == 0, first.stderr

    index = root / "index.jsonl"
    index_before = index.read_bytes()
    index.chmod(0o444)

    second = tmp_path / "cdx-gate-review.ROLLFAIL"
    second.mkdir()
    for name in ("start.json", "attestation.json", "review.md"):
        (second / name).write_text((run / name).read_text())
    (second / "attestation.json").write_text(json.dumps(
        _make_coherent(json.loads((second / "attestation.json").read_text()),
                       second)) + "\n")

    try:
        out, _ = _archive_gate(tmp_path, second, prompts)
        assert out.returncode != 0, out.stdout
        assert "Traceback" not in out.stderr, "the rollback raised out of its handler"
        # The round HAD been moved into place before the append failed, so the
        # rollback had exactly one thing to undo and undoes it. The index needed
        # no restore — the append never wrote — and attempting one anyway is
        # what used to raise, because it needed the permission that was missing.
        assert "rolled back ['the round directory']" in out.stderr
        assert "UNKNOWN" not in out.stderr, "it should not claim an unknown state here"
        assert index.read_bytes() == index_before
        assert not (root / "architect-reviews" / second.name).exists()
    finally:
        index.chmod(0o644)

    retry, _ = _archive_gate(tmp_path, second, prompts)
    assert retry.returncode == 0, retry.stderr


def test_an_archive_with_NO_INDEX_is_refused_rather_than_bootstrapped(tmp_path):
    """Three behaviours in three rounds, and only the third is right.

    Originally this crashed with a file-not-found, orphaning the round and
    blocking the retry — a regression I introduced. I then "fixed" it by letting
    the append create the index, and asserted only that the command exited 0.
    It did, and produced an archive nothing can read: the first line of an index
    is its HEADER, carrying a schema version and a source tip, and a round row
    sitting there fails the repository's scanner and is explicitly refused by
    this script's own re-derivation. My witness had checked for success instead
    of for a usable result, which is why it passed.

    An archive skeleton is created deliberately. Inventing a source tip here
    would be fabricating provenance, so a missing index is refused.
    """
    run, prompts = _gate_run(tmp_path)
    root = tmp_path / "repo" / "docs" / "architecture" / "evidence" / "issue-888"
    root.mkdir(parents=True)          # no index.jsonl: not a skeleton

    def archive():
        return subprocess.run(
            [sys.executable, str(_SCRIPT), "--issue", "888", "--kind",
             "architect-review", "--run-dir", str(run), "--logical-loop", "L3",
             "--repo", str(tmp_path / "repo"), "--prompts", str(prompts)],
            capture_output=True, text=True)

    result = archive()
    assert result.returncode == 1, result.stdout
    assert "not bootstrapped by its first round" in result.stderr
    assert "Traceback" not in result.stderr
    assert not (root / "architect-reviews").exists(), "a round was written anyway"

    # With a real skeleton it archives, AND the result is usable — which is the
    # assertion the earlier version of this test was missing.
    (root / "index.jsonl").write_text(json.dumps({
        "generated_at": "x", "issue": 888, "schema_version": 1, "source_tip": "abc",
    }) + "\n")
    ok = archive()
    assert ok.returncode == 0, ok.stderr
    header = json.loads((root / "index.jsonl").read_text().splitlines()[0])
    assert "schema_version" in header and "source_tip" in header
    assert "durable_dir" not in header, "the round row became the header"

    rederived = subprocess.run(
        [sys.executable, str(_SCRIPT), "--issue", "888", "--rederive-index",
         "--repo", str(tmp_path / "repo")],
        capture_output=True, text=True)
    assert rederived.returncode == 0, rederived.stderr


@pytest.mark.parametrize("bad", ["999/../issue-888", "999/../../../../..", "abc", "9 9"])
def test_the_ISSUE_is_a_number_not_a_path(tmp_path, bad):
    """The twin of the name check, joined by the same operator.

    This script's own comment already warned the issue number is accepted as
    given and the hazard is one typo away. It was: a traversing issue wrote
    eleven paths outside the repository at exit 0.
    """
    run, prompts = _gate_run(tmp_path)
    result = subprocess.run(
        [sys.executable, str(_SCRIPT), "--issue", bad, "--kind", "architect-review",
         "--run-dir", str(run), "--logical-loop", "L3", "--repo", str(tmp_path / "repo"),
         "--prompts", str(prompts)],
        capture_output=True, text=True)
    assert result.returncode == 2, result.stdout
    assert "must be a number" in result.stderr
    assert list((tmp_path / "repo").rglob("issue-888")) == []


def test_a_name_the_MANIFEST_cannot_record_is_refused(tmp_path):
    """One refusal replacing three separate encoding problems.

    A newline ends a manifest record mid-name, so the manifest could never be
    parsed again; a decomposed-unicode name is stored differently by git, so the
    manifest and the git index disagree; a symlink is hashed through but recorded
    by its own path. Encoding around each in turn is sanitising an unbounded
    space. The input is bounded instead, and the limit is stated.
    """
    run, prompts = _gate_run(tmp_path)
    result, root = _archive_gate(tmp_path, run, prompts)
    assert result.returncode == 0, result.stderr

    newline_named = root / "captures" / "with\nnewline.txt"
    newline_named.parent.mkdir(parents=True, exist_ok=True)
    newline_named.write_text("x\n")

    out = _rederive(tmp_path)
    assert out.returncode != 0, out.stdout
    assert "line separator" in out.stderr
    newline_named.unlink()

    # NFD: the same characters git would store in composed form.
    import unicodedata

    nfd = root / "captures" / unicodedata.normalize("NFD", "café.txt")
    nfd.write_text("x\n")
    out = _rederive(tmp_path)
    assert out.returncode != 0, out.stdout
    assert "decomposed" in out.stderr
    nfd.unlink()

    # A symlink, which the manifest describes by the link rather than the target.
    link = root / "captures" / "link.txt"
    link.symlink_to(root / "index.jsonl")
    out = _rederive(tmp_path)
    assert out.returncode != 0, out.stdout
    assert "symlink" in out.stderr
    link.unlink()

    # ...and with all three gone it works again.
    _reseal(root)
    assert _rederive(tmp_path).returncode == 0


def test_an_archive_with_NO_manifest_still_accounts_for_what_is_in_it(tmp_path):
    """`if not listed: return` treated every manifest-less archive as a bootstrap.

    A real bootstrap has nothing in it but its own index, so anything else
    present arrived from somewhere this script cannot vouch for — and skipping
    the check meant the very next write recorded it as evidence.
    """
    run, prompts = _gate_run(tmp_path)
    result, root = _archive_gate(tmp_path, run, prompts)
    assert result.returncode == 0, result.stderr
    (root / "SHA256SUMS").unlink()

    out = _rederive(tmp_path)
    assert out.returncode != 0, out.stdout
    assert "no manifest yet" in out.stderr


def test_a_refusal_does_not_remove_a_directory_it_did_not_create(tmp_path):
    """Removing an empty directory because a refusal happened to find it empty."""
    run, prompts = _gate_run(tmp_path)
    first, root = _archive_gate(tmp_path, run, prompts)
    assert first.returncode == 0, first.stderr

    # Empty the kind directory but KEEP it, so it is a directory that existed
    # before the refusing run and holds nothing. That is the exact shape whose
    # removal is indistinguishable from tidying up after oneself.
    shutil.rmtree(root / "architect-reviews" / run.name)
    assert (root / "architect-reviews").is_dir()
    assert not any((root / "architect-reviews").iterdir())

    # ...and leave the archive CONSISTENT with that, or the pre-flight refuses
    # before the rollback is ever reached and the test proves nothing about it.
    header = (root / "index.jsonl").read_text().splitlines()[0]
    (root / "index.jsonl").write_text(header + "\n")
    import hashlib

    (root / "SHA256SUMS").write_text(
        hashlib.sha256((root / "index.jsonl").read_bytes()).hexdigest()
        + "  index.jsonl\n")

    index = root / "index.jsonl"
    index.chmod(0o444)                     # force a failure after the rename
    second = tmp_path / "cdx-gate-review.WILLREFUSE"
    second.mkdir()
    for name in ("start.json", "attestation.json", "review.md"):
        (second / name).write_text((run / name).read_text())
    (second / "attestation.json").write_text(json.dumps(
        _make_coherent(json.loads((second / "attestation.json").read_text()),
                       second)) + "\n")

    try:
        out, _ = _archive_gate(tmp_path, second, prompts)
        assert out.returncode != 0, out.stdout
        assert (root / "architect-reviews").is_dir(), \
            "a directory this run never created was removed by its rollback"
        assert not (root / "architect-reviews" / second.name).exists()
    finally:
        index.chmod(0o644)


@pytest.mark.parametrize("sep,name", [
    ("\v", "vertical tab"), ("\f", "form feed"), ("\x1c", "file separator"),
    ("\x1d", "group separator"), ("\x1e", "record separator"),
    ("\x85", "NEL"), (" ", "line separator"), (" ", "paragraph separator"),
])
def test_EVERY_separator_splitlines_breaks_on_is_refused(tmp_path, sep, name):
    """Eight the hand-written check did not know about.

    It tested carriage return and newline. The manifest is read back with
    `str.splitlines()`, which also breaks on all of these — so each produced a
    manifest that parsed into more records than it had files, at exit 0. The
    check now asks `splitlines()` itself rather than comparing against a list,
    because the reader IS the authority on what breaks the reader, and a list
    beside it is a copy that drifts. This test exists to prove the derivation
    covers the cases the list missed.
    """
    run, prompts = _gate_run(tmp_path)
    result, root = _archive_gate(tmp_path, run, prompts)
    assert result.returncode == 0, result.stderr

    offender = root / "captures" / f"bad{sep}name.txt"
    offender.parent.mkdir(parents=True, exist_ok=True)
    try:
        offender.write_text("x\n")
    except (OSError, ValueError):
        pytest.skip(f"the filesystem rejects {name} in a name")

    out = _rederive(tmp_path)
    assert out.returncode != 0, (name, out.stdout)
    assert "line separator" in out.stderr, name


def test_a_failed_PUBLICATION_leaves_no_staging_behind(tmp_path):
    """A rename can fail too, and it sat outside the guarded cleanup.

    Left there, a failed publication stranded a populated staging directory
    inside the archive, which the next run then refuses as unaccounted —
    recreating exactly the retry-blocked state that building beside the
    destination was introduced to remove.
    """
    run, prompts = _gate_run(tmp_path)
    first, root = _archive_gate(tmp_path, run, prompts)
    assert first.returncode == 0, first.stderr

    second = tmp_path / "cdx-gate-review.PUBFAIL"
    second.mkdir()
    for name in ("start.json", "attestation.json", "review.md"):
        (second / name).write_text((run / name).read_text())
    (second / "attestation.json").write_text(json.dumps(
        _make_coherent(json.loads((second / "attestation.json").read_text()),
                       second)) + "\n")

    # The publication specifically — not an earlier step. A read-only kind
    # directory fails at staging creation and never reaches the rename, so it
    # cannot control this guard; the failure has to be injected AT the rename.
    # That is also the shape the review named: the destination appearing between
    # the existence check and the move.
    module = _archiver_module()
    kind = root / "architect-reviews"

    def exploding_rename(src, dst):
        raise OSError(5, "Input/output error")

    real_rename, module.os.rename = module.os.rename, exploding_rename
    argv = sys.argv
    sys.argv = ["archive_gate_round.py", "--issue", "999", "--kind",
                "architect-review", "--run-dir", str(second), "--logical-loop",
                "L3", "--repo", str(tmp_path / "repo"), "--prompts", str(prompts)]
    try:
        rc = module.main()
    finally:
        module.os.rename = real_rename
        sys.argv = argv

    assert rc == 1, rc
    leftovers = [p.name for p in kind.iterdir() if p.name.startswith(".partial-")]
    assert leftovers == [], leftovers
    assert not (kind / second.name).exists()

    # ...and the ordinary command still succeeds afterwards.
    retry, _ = _archive_gate(tmp_path, second, prompts)
    assert retry.returncode == 0, retry.stderr


def test_a_publication_failure_whose_CLEANUP_also_fails_reports_unknown(tmp_path):
    """Two adjacent failure paths, one of which asserted what the other verified.

    Suppressing the removal's own errors and then reporting that nothing was
    left is the shape the rollback below was already corrected for: a surviving
    staging directory is refused as unaccounted by the next run, so an operator
    told "nothing was left" would be looking anywhere but at the thing blocking
    them. Both failures have to be injected — no filesystem-level manipulation
    reaches the rename at all — which is precisely why this needs a control
    rather than a reading.
    """
    run, prompts = _gate_run(tmp_path)
    first, root = _archive_gate(tmp_path, run, prompts)
    assert first.returncode == 0, first.stderr

    second = tmp_path / "cdx-gate-review.BOTHFAIL"
    second.mkdir()
    for name in ("start.json", "attestation.json", "review.md"):
        (second / name).write_text((run / name).read_text())
    (second / "attestation.json").write_text(json.dumps(
        _make_coherent(json.loads((second / "attestation.json").read_text()),
                       second)) + "\n")

    module = _archiver_module()

    def exploding_rename(src, dst):
        raise OSError(5, "Input/output error")

    def useless_rmtree(path, ignore_errors=False, **kw):
        return None                      # the cleanup silently accomplishes nothing

    real_rename, module.os.rename = module.os.rename, exploding_rename
    real_rmtree, module.shutil.rmtree = module.shutil.rmtree, useless_rmtree
    argv = sys.argv
    sys.argv = ["archive_gate_round.py", "--issue", "999", "--kind",
                "architect-review", "--run-dir", str(second), "--logical-loop",
                "L3", "--repo", str(tmp_path / "repo"), "--prompts", str(prompts)]
    import io
    import contextlib

    err = io.StringIO()
    try:
        with contextlib.redirect_stderr(err):
            rc = module.main()
    finally:
        module.os.rename = real_rename
        module.shutil.rmtree = real_rmtree
        sys.argv = argv

    assert rc == 1, rc
    message = err.getvalue()
    assert "UNKNOWN STATE" in message, message
    assert "Nothing was left" not in message, "it claimed a cleanup it did not achieve"

    leftover = [p for p in (root / "architect-reviews").iterdir()
                if p.name.startswith(".partial-")]
    assert leftover, "the fixture failed to leave the debris this test is about"
    for path in leftover:
        real_rmtree(path, ignore_errors=True)


def test_only_a_PROVEN_absence_counts_as_a_completed_cleanup(tmp_path):
    """An existence test is not a verification under the faults it checks for.

    Measured on the interpreters this repository runs: `Path.exists()` re-raises
    for EACCES and EIO — exactly what a failed cleanup reports — so calling it
    inside the failure handler escapes that handler without its diagnostic. And
    it follows symlinks, so a DANGLING symlink answers False while the directory
    entry remains for the next run's scan to trip over. Only `lstat` raising
    ENOENT means gone.
    """
    module = _archiver_module()

    # A dangling symlink: the case `exists()` gets backwards.
    dangling = tmp_path / ".partial-dangling"
    dangling.symlink_to(tmp_path / "nowhere-at-all")
    assert dangling.exists() is False, "premise: exists() reports it absent"
    assert module._confirmed_removed(dangling) is False, \
        "a directory entry that is still there is not a completed cleanup"

    # A path that genuinely is not there.
    assert module._confirmed_removed(tmp_path / "never-existed") is True

    # An error that is not ENOENT must not read as removed. A path under a
    # non-searchable directory produces EACCES rather than ENOENT.
    locked = tmp_path / "locked"
    locked.mkdir()
    (locked / "inside").write_text("x\n")
    locked.chmod(0o000)
    try:
        verdict = module._confirmed_removed(locked / "inside")
    finally:
        locked.chmod(0o755)
    assert verdict is False, "an unreadable path is unknown, not removed"


def test_a_dangling_staging_symlink_is_reported_not_claimed_clean(tmp_path):
    """End to end: the publication handler must not claim a cleanup it cannot prove."""
    run, prompts = _gate_run(tmp_path)
    first, root = _archive_gate(tmp_path, run, prompts)
    assert first.returncode == 0, first.stderr

    second = tmp_path / "cdx-gate-review.DANGLE"
    second.mkdir()
    for name in ("start.json", "attestation.json", "review.md"):
        (second / name).write_text((run / name).read_text())
    (second / "attestation.json").write_text(json.dumps(
        _make_coherent(json.loads((second / "attestation.json").read_text()),
                       second)) + "\n")

    module = _archiver_module()
    kind = root / "architect-reviews"

    def exploding_rename(src, dst):
        raise OSError(5, "Input/output error")

    def symlink_leaving_rmtree(path, ignore_errors=False, **kw):
        # Removes the directory but leaves a dangling entry in its place —
        # the shape `exists()` reports as absent.
        real_rmtree(path, ignore_errors=True)
        Path(path).symlink_to(Path(path).parent / "gone")

    real_rename, module.os.rename = module.os.rename, exploding_rename
    real_rmtree = shutil.rmtree
    module.shutil.rmtree = symlink_leaving_rmtree
    argv = sys.argv
    sys.argv = ["archive_gate_round.py", "--issue", "999", "--kind",
                "architect-review", "--run-dir", str(second), "--logical-loop",
                "L3", "--repo", str(tmp_path / "repo"), "--prompts", str(prompts)]
    import io
    import contextlib

    err = io.StringIO()
    try:
        with contextlib.redirect_stderr(err):
            rc = module.main()
    finally:
        module.os.rename = real_rename
        module.shutil.rmtree = real_rmtree
        sys.argv = argv

    assert rc == 1, rc
    assert "UNKNOWN STATE" in err.getvalue(), err.getvalue()
    assert "Nothing was left" not in err.getvalue()
    for leftover in [p for p in kind.iterdir() if p.name.startswith(".partial-")]:
        leftover.unlink()


def test_the_ROLLBACK_also_confirms_its_removal_rather_than_assuming_it(tmp_path):
    """The sibling of the publication check, swept for the same reason.

    Here the round IS published and the index append then fails, so the rollback
    proper runs. If its removal silently accomplishes nothing, the command must
    report an unknown archive state — not list the round directory among the
    things it rolled back. Assuming a removal worked is how the earlier version
    of the publication handler came to claim a cleanup it had not achieved.
    """
    run, prompts = _gate_run(tmp_path)
    first, root = _archive_gate(tmp_path, run, prompts)
    assert first.returncode == 0, first.stderr

    second = tmp_path / "cdx-gate-review.ROLLNOOP"
    second.mkdir()
    for name in ("start.json", "attestation.json", "review.md"):
        (second / name).write_text((run / name).read_text())
    (second / "attestation.json").write_text(json.dumps(
        _make_coherent(json.loads((second / "attestation.json").read_text()),
                       second)) + "\n")

    module = _archiver_module()
    index = root / "index.jsonl"
    index.chmod(0o444)                       # the append fails, after publication

    real_rmtree = shutil.rmtree
    module.shutil.rmtree = lambda path, ignore_errors=False, **kw: None

    argv = sys.argv
    sys.argv = ["archive_gate_round.py", "--issue", "999", "--kind",
                "architect-review", "--run-dir", str(second), "--logical-loop",
                "L3", "--repo", str(tmp_path / "repo"), "--prompts", str(prompts)]
    import io
    import contextlib

    err = io.StringIO()
    try:
        with contextlib.redirect_stderr(err):
            rc = module.main()
    finally:
        module.shutil.rmtree = real_rmtree
        sys.argv = argv
        index.chmod(0o644)

    assert rc == 1, rc
    message = err.getvalue()
    assert "UNKNOWN STATE" in message, message
    assert "rolled back ['the round directory']" not in message, \
        "it listed a removal it did not achieve"
    real_rmtree(root / "architect-reviews" / second.name, ignore_errors=True)


def test_a_copy_failure_whose_cleanup_also_fails_reports_unknown(tmp_path):
    """The sibling I missed when the publication path was corrected.

    Round 13's finding was exactly this shape — a cleanup asserted rather than
    verified — and the fix swept the publication path and the rollback while
    leaving the copy path, which is the one an architect review then reproduced
    by fault injection. A surviving staging directory blocks the next preflight,
    so telling the operator nothing was written sends them the wrong way.
    """
    run, prompts = _gate_run(tmp_path)
    first, root = _archive_gate(tmp_path, run, prompts)
    assert first.returncode == 0, first.stderr

    second = tmp_path / "cdx-gate-review.COPYFAIL"
    second.mkdir()
    for name in ("start.json", "attestation.json", "review.md"):
        (second / name).write_text((run / name).read_text())
    (second / "attestation.json").write_text(json.dumps(
        _make_coherent(json.loads((second / "attestation.json").read_text()),
                       second)) + "\n")

    module = _archiver_module()
    kind = root / "architect-reviews"

    def exploding_copy(src, dst, *a, **kw):
        raise OSError(5, "Input/output error")

    real_copy, module.shutil.copy2 = module.shutil.copy2, exploding_copy
    real_rmtree = shutil.rmtree
    module.shutil.rmtree = lambda p, ignore_errors=False, **kw: None   # cleanup accomplishes nothing

    argv = sys.argv
    sys.argv = ["archive_gate_round.py", "--issue", "999", "--kind",
                "architect-review", "--run-dir", str(second), "--logical-loop",
                "L3", "--repo", str(tmp_path / "repo"), "--prompts", str(prompts)]
    import io
    import contextlib

    err = io.StringIO()
    try:
        with contextlib.redirect_stderr(err):
            rc = module.main()
    finally:
        module.shutil.copy2 = real_copy
        module.shutil.rmtree = real_rmtree
        sys.argv = argv

    assert rc == 1, rc
    message = err.getvalue()
    assert "UNKNOWN STATE" in message, message
    assert "Nothing was written" not in message, "it claimed a cleanup it did not achieve"
    for leftover in [p for p in kind.iterdir() if p.name.startswith(".partial-")]:
        real_rmtree(leftover, ignore_errors=True)


def test_an_UNPARSEABLE_attestation_is_not_an_absent_one(tmp_path):
    """Collapsing the two archived a malformed round at exit 0.

    The presence guard saw None for invalid JSON, concluded the round was simply
    unattested, and let it through with a null gate and status — output the
    downstream scanner refuses, reported as success by the producer, with the
    source run possibly discarded on the strength of it.
    """
    run, prompts = _gate_run(tmp_path)
    (run / "attestation.json").write_text('{"gate": "review", "turn": {trunc')

    result, root = _archive_gate(tmp_path, run, prompts)
    assert result.returncode == 1, result.stdout
    assert "cannot be used" in result.stderr
    assert "not an absent file" in result.stderr
    assert not (root / "architect-reviews" / run.name).exists()

    # ...and a genuinely ABSENT attestation still archives, so the refusal is
    # about unreadability rather than about attestation being required.
    (run / "attestation.json").unlink()
    ok, _ = _archive_gate(tmp_path, run, prompts)
    assert ok.returncode == 0, ok.stderr


@pytest.mark.parametrize("body,why", [
    ("null", "syntactically valid JSON that parses to nothing usable"),
    ("[]", "a list where an object is required"),
    ('"a string"', "a bare string"),
    ("123", "a bare number"),
    ("{oops", "invalid JSON"),
])
def test_a_sidecar_that_parses_to_NOTHING_USABLE_is_refused(tmp_path, body, why):
    """Absent, unreadable, and readable-but-useless are three different states.

    The first fix here collapsed the first two and missed the third: `null` is
    valid JSON, so it parsed cleanly to nothing and slipped a guard that only
    tested for a parse FAILURE. All of these are equally unusable and all are
    refused before anything is created.
    """
    run, prompts = _gate_run(tmp_path)
    (run / "attestation.json").write_text(body)

    result, root = _archive_gate(tmp_path, run, prompts)
    assert result.returncode == 1, (why, result.stdout)
    assert "cannot be used" in result.stderr, why
    assert not (root / "architect-reviews" / run.name).exists(), why
    assert "Traceback" not in result.stderr, why


def test_an_unreadable_START_json_is_refused_before_publication(tmp_path):
    """The sentinel leaked past the attestation guard into the row builder.

    It is truthy on purpose — a falsey sentinel would let `or {}` swap it for an
    empty mapping and archive a corrupt round as an empty one — so the row
    builder's `start.get(...)` kept it and raised. That builder runs AFTER the
    round is published, which is exactly where a failure is hardest to retry out
    of. The refusal is now in the pre-flight, before anything is created.
    """
    run = tmp_path / "cdx-review.BADSTART"
    run.mkdir()
    (run / "start.json").write_text("{not json at all")
    (run / "baseline").write_text("b" * 40 + "\n")
    (run / "start-head").write_text("c" * 40 + "\n")
    (run / "last-reviewed-sha").write_text("c" * 40 + "\n")
    (run / "dirty").write_text("false\n")
    (run / "scope").write_text("auto\n")
    (run / "teardown").write_text("confirmed stopped\n")
    (run / "review.json").write_text(json.dumps({"scope": "auto"}) + "\n")

    root = tmp_path / "repo" / "docs" / "architecture" / "evidence" / "issue-999"
    root.mkdir(parents=True, exist_ok=True)
    (root / "index.jsonl").write_text(json.dumps({
        "generated_at": "x", "issue": 999, "schema_version": 1, "source_tip": "abc",
    }) + "\n")

    result = subprocess.run(
        [sys.executable, str(_SCRIPT), "--issue", "999", "--kind", "commit-review",
         "--run-dir", str(run), "--logical-loop", "L2", "--repo", str(tmp_path / "repo")],
        capture_output=True, text=True)
    assert result.returncode == 1, result.stdout
    assert "cannot be used" in result.stderr
    assert "start.json" in result.stderr
    assert "Traceback" not in result.stderr
    assert not (root / "commit-reviews" / run.name).exists(), \
        "the round was published before the failure — the retry is now blocked"


def test_the_FIXTURE_satisfies_the_same_contract_the_archive_scanner_enforces():
    """The positive test is only meaningful if its fixture is consumer-valid.

    An architect review found this module's fixture carried three of the seven
    facts the archive scanner requires of a completed round — so the positive
    test proved the archiver accepts something the consumer refuses, which is
    the opposite of what a positive test is for. This pins the fixture against
    the contract directly, and against a REAL archived attestation, so the two
    cannot drift apart again in either direction.
    """
    module = _archiver_module()
    required = module.CONSUMER_REQUIRES["architect-review"]["attestation.json"]

    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        run, _prompts = _gate_run(Path(tmp))
        fixture = json.loads((run / "attestation.json").read_text())
    missing = ["/".join(fp) for fp in required if module._dig(fixture, fp) in (None, "")]
    assert missing == [], missing

    # ...and the same contract holds against an attestation actually archived.
    real = json.loads(sorted(_ARCHITECT_ROUNDS.glob("*/attestation.json"))[0].read_text())
    absent = ["/".join(fp) for fp in required if module._dig(real, fp) in (None, "")]
    assert absent == [], absent


@pytest.mark.parametrize("drop", [
    ("teardown",), ("turn", "status"), ("parsedVerdict",),
    ("artifact", "sha256"), ("start", "threadId"),
])
def test_an_attestation_MISSING_a_consumer_required_fact_is_refused(tmp_path, drop):
    """Parsing to an object is a weaker contract than the consumer's.

    Each of these attestations is perfectly valid JSON and would have archived at
    exit 0, then been rejected downstream — the producer reporting success for
    evidence its consumer refuses, which is the defect this whole file exists to
    prevent.
    """
    run, prompts = _gate_run(tmp_path)
    att = json.loads((run / "attestation.json").read_text())
    node = att
    for key in drop[:-1]:
        node = node[key]
    node.pop(drop[-1])
    (run / "attestation.json").write_text(json.dumps(att) + "\n")

    result, root = _archive_gate(tmp_path, run, prompts)
    assert result.returncode == 1, (drop, result.stdout)
    # EITHER refusal is correct: the round IS refused, which is the property.
    # Dropping `parsedVerdict` now trips the verdict-resolution check first,
    # because the current collector writes that field null for an architect gate
    # and the verdict is derived from the artifact instead — a round carrying one
    # nowhere is still refused. Pinning one wording made this about which refusal
    # won the race rather than about refusing.
    assert ("the archive scanner requires" in result.stderr
            or "does not resolve ['verdict']" in result.stderr), (result.stderr, drop)
    # The dropped fact is NAMED, under whichever spelling the refusal uses: the
    # consumer-contract refusal prints the dotted path, the verdict-resolution
    # refusal prints the row field. Both identify what is missing, which is what
    # an operator needs; requiring one spelling made this a test of which check
    # ran rather than of whether the gap was reported.
    assert ("/".join(drop) in result.stderr
            or drop[-1] in result.stderr
            or "verdict" in result.stderr), (result.stderr, drop)
    assert not (root / "architect-reviews" / run.name).exists(), drop


def test_a_DIRECTORY_at_a_sidecar_path_is_not_an_absent_sidecar(tmp_path):
    """`is_file()` answers no for a directory, which sent it down the absent branch."""
    run, prompts = _gate_run(tmp_path)
    (run / "attestation.json").unlink()
    (run / "attestation.json").mkdir()

    result, root = _archive_gate(tmp_path, run, prompts)
    assert result.returncode == 1, result.stdout
    assert "is a directory" in result.stderr
    assert not (root / "architect-reviews" / run.name).exists()


def test_the_checksum_staging_file_is_actually_REMOVED_not_merely_probed(tmp_path):
    """A verification is not a substitute for the action it verifies.

    Replacing the unchecked delete with the confirmation helper alone removed the
    deletion entirely: that helper only probes. The partial manifest then survived
    every failure and was refused as unaccounted by the next run — strictly worse
    than the unchecked delete it replaced.
    """
    module = _archiver_module()
    victim = tmp_path / ".SHA256SUMS.partial-test"
    victim.write_text("partial\n")
    assert victim.exists()

    assert module._remove_confirmed(victim) is True
    assert not victim.exists(), "the helper probed but did not delete"

    # ...and it removes a DIRECTORY too, which is the shape it was written for.
    d = tmp_path / ".partial-dir"
    (d / "inner").mkdir(parents=True)
    (d / "inner" / "f").write_text("x")
    assert module._remove_confirmed(d) is True
    assert not d.exists()


def test_an_attestation_naming_a_DIFFERENT_session_is_refused(tmp_path):
    """Presence is not agreement, and the scanner requires agreement.

    An attestation can carry every field the consumer names and still be
    rejected, because the consumer checks that they MATCH the round: the thread
    must be the archived session's thread and the artifact digest must be the
    archived review's digest. A presence-only pre-flight publishes exactly that
    round — and this module's own fixture was carrying two different thread ids
    when an architect review pointed it out.
    """
    run, prompts = _gate_run(tmp_path)
    att = json.loads((run / "attestation.json").read_text())
    att["start"]["threadId"] = "01a0-some-other-session"
    (run / "attestation.json").write_text(json.dumps(att) + "\n")

    result, root = _archive_gate(tmp_path, run, prompts)
    assert result.returncode == 1, result.stdout
    assert "not the archived session's thread" in result.stderr
    assert not (root / "architect-reviews" / run.name).exists()


def test_an_attestation_whose_artifact_digest_is_WRONG_is_refused(tmp_path):
    """The other half of the same agreement, checked the same way."""
    run, prompts = _gate_run(tmp_path)
    att = json.loads((run / "attestation.json").read_text())
    att["artifact"]["sha256"] = "0" * 64
    (run / "attestation.json").write_text(json.dumps(att) + "\n")

    result, root = _archive_gate(tmp_path, run, prompts)
    assert result.returncode == 1, result.stdout
    assert "does not match the review it archives" in result.stderr
    assert not (root / "architect-reviews" / run.name).exists()


def test_a_failed_manifest_REPLACE_leaves_no_partial_behind(tmp_path):
    """Exercises the CALL SITE, not the helper.

    The first witness for this checked that the removal helper deletes — which it
    does — while the defect was that `write_sums` called the PROBE instead. A
    mutant swapping them back passed that test untouched. So this drives the real
    failure path: the staging manifest is written, the replace fails, and nothing
    may be left behind for the next run to refuse as unaccounted.
    """
    run, prompts = _gate_run(tmp_path)
    first, root = _archive_gate(tmp_path, run, prompts)
    assert first.returncode == 0, first.stderr

    module = _archiver_module()
    real_replace = module.os.replace

    def exploding_replace(src, dst):
        raise OSError(5, "Input/output error")

    module.os.replace = exploding_replace
    try:
        try:
            module.write_sums(root)
        except OSError:
            pass                      # the failure itself is the point
    finally:
        module.os.replace = real_replace

    leftovers = [p.name for p in root.iterdir() if p.name.startswith(".SHA256SUMS.partial")]
    assert leftovers == [], leftovers


@pytest.mark.parametrize("break_it,expect", [
    (lambda run, att: (run / "start.json").unlink(),
     "the session record carries no thread identifier"),
    (lambda run, att: (run / "start.json").write_text(json.dumps({"socket": "/s"}) + "\n"),
     "the session record carries no thread identifier"),
    (lambda run, att: att["start"].pop("threadId"),
     # Caught by the presence check first — earlier, and still a refusal.
     "the archive scanner requires"),
    (lambda run, att: (run / "review.md").unlink(),
     "absent or not a regular file"),
    (lambda run, att: att["artifact"].pop("sha256"),
     "the archive scanner requires"),
    (lambda run, att: att["artifact"].pop("path"),
     "the archive scanner requires"),
    (lambda run, att: att["artifact"].update(path="/somewhere/else/review.md"),
     "names something other than this round's review"),
])
def test_a_MISSING_binding_side_is_refused_not_skipped(tmp_path, break_it, expect):
    """Absence skipped the check, and absence is the case the consumer rejects.

    The first version of these bindings compared two values only when BOTH were
    truthy, so removing either side silently satisfied it. The archive scanner
    reads the session record unconditionally and requires the identifiers to be
    present AND equal — so a check that excuses absence enforces a coincidence
    rather than a binding, and publishes exactly the round the consumer refuses.
    """
    run, prompts = _gate_run(tmp_path)
    att = json.loads((run / "attestation.json").read_text())
    break_it(run, att)
    (run / "attestation.json").write_text(json.dumps(att) + "\n")

    result, root = _archive_gate(tmp_path, run, prompts)
    assert result.returncode == 1, (expect, result.stdout)
    assert expect in result.stderr, (expect, result.stderr)
    assert not (root / "architect-reviews" / run.name).exists(), expect


def test_a_verdict_outside_the_protocol_enum_is_not_a_verdict(tmp_path):
    """`VERDICT: BANANA` was merely truthy, so the round archived as completed.

    The gate prompt that binds the architect session permits exactly two
    verdicts. Anything else on that final line — a truncated turn, a paraphrase,
    a model that answered in prose — is a turn that did not conclude, and a row
    recording it names a verdict no reader can act on and no gate defines.
    """
    module = _archiver_module()
    ARCHITECT_VERDICTS = module.ARCHITECT_VERDICTS
    architect_verdict_from_artifact = module.architect_verdict_from_artifact

    run = tmp_path / "round"
    run.mkdir()

    for verdict in ARCHITECT_VERDICTS:
        (run / "review.md").write_text("findings\n\nVERDICT: %s\n" % verdict)
        assert architect_verdict_from_artifact(run, {}) == verdict

    for bogus in ("BANANA", "no issues", "ISSUES  FOUND", "", "MAYBE"):
        (run / "review.md").write_text("findings\n\nVERDICT: %s\n" % bogus)
        assert architect_verdict_from_artifact(run, {}) is None, bogus

    # BOTH branches are bound by the enum. An earlier version exempted the
    # attestation on the reasoning that the collector had already parsed it —
    # but the collector's shared rule can yield `UNCLEAR`, which the gate prompt
    # does not define, and a constraint applied to one of two paths producing the
    # same field constrains neither.
    (run / "review.md").write_text("findings\n\nVERDICT: NO ISSUES\n")
    assert architect_verdict_from_artifact(run, {"parsedVerdict": "NO ISSUES"}) == "NO ISSUES"
    for bogus in ("UNCLEAR", "BANANA", "no issues", ""):
        # Falls through to the artifact, which here carries a valid one...
        assert architect_verdict_from_artifact(run, {"parsedVerdict": bogus}) == "NO ISSUES", bogus
    # ...and when neither source is well-formed, there is no verdict at all.
    (run / "review.md").write_text("findings\n\nVERDICT: BANANA\n")
    assert architect_verdict_from_artifact(run, {"parsedVerdict": "UNCLEAR"}) is None


def test_an_undecodable_artifact_yields_no_verdict_rather_than_raising(tmp_path):
    """It runs AFTER the round has been published, outside the rename's rollback.

    The digest preflight hashes raw bytes and never decodes them, so invalid
    UTF-8 reaches this read intact. Raising here left a durable directory nothing
    accounted for — which the next run refuses as unaccounted, blocking the very
    retry that would fix it. Returning None routes it into the caller's existing
    unresolved-verdict path, which removes what it just published.
    """
    architect_verdict_from_artifact = _archiver_module().architect_verdict_from_artifact

    run = tmp_path / "round"
    run.mkdir()
    (run / "review.md").write_bytes(b"findings\n\nVERDICT: NO ISSUES\n\xff\xfe invalid")

    assert architect_verdict_from_artifact(run, {}) is None

    # NON-VACUITY: the same bytes decoded would have carried a valid verdict, so
    # this asserts the decode failure and not merely a missing line.
    assert b"VERDICT: NO ISSUES" in (run / "review.md").read_bytes()


def test_the_index_row_carries_the_validated_verdict_not_the_raw_one(tmp_path):
    """The enum gated admission and then played no part in what was archived.

    `derive_row` took the attestation's raw value whenever it was truthy, so an
    attestation carrying `UNCLEAR` beside an artifact carrying a valid verdict
    passed preflight through the fallback and still wrote `UNCLEAR` into the
    index — which the downstream scanner then trusts. A constraint that gates the
    gate but not the record is a constraint on the gate only.
    """
    import json

    module = _archiver_module()
    run = tmp_path / "round"
    run.mkdir()
    (run / "review.md").write_text("findings\n\nVERDICT: NO ISSUES\n")

    # The contradictory pair the finding names.
    assert module.architect_verdict_from_artifact(
        run, {"parsedVerdict": "UNCLEAR"}) == "NO ISSUES"

    # ...and when neither source is well-formed there is no verdict to write.
    (run / "review.md").write_text("findings\n\nVERDICT: BANANA\n")
    assert module.architect_verdict_from_artifact(
        run, {"parsedVerdict": "UNCLEAR"}) is None


def test_the_archiver_stages_what_its_manifest_names(tmp_path):
    """A write that creates a manifest/index mismatch must not leave it behind.

    THE NON-VACUITY WITNESS for staging atomically. The archiver used to print a
    NEXT line asking the caller to stage what it had just written, while its own
    `SHA256SUMS` already NAMED those files — so any caller who read the line and
    did something else left the checkout asserting evidence it did not carry, and a
    fresh clone failed proving it. That is not hypothetical: it happened, and the
    review that caught it is `CDX-155-r235-01`.

    A warning that must be obeyed every time to hold an invariant is the invariant
    unenforced. Here the writer that creates the inconsistency ends it in the same
    step, and this asserts the files are in the INDEX rather than merely on disk.
    """
    import subprocess

    repo = tmp_path / "repo"
    (repo / "docs" / "architecture" / "evidence" / "issue-999").mkdir(parents=True)
    (repo / "docs" / "architecture" / "evidence" / "issue-999" / "index.jsonl").write_text(
        json.dumps({"generated_at": "x", "issue": 999, "schema_version": 1,
                    "source_tip": "abc"}) + "\n")
    for args in (["init", "-q"], ["config", "user.email", "t@t"],
                 ["config", "user.name", "t"]):
        subprocess.run(["git", "-C", str(repo), *args], check=True,
                       capture_output=True)
    (repo / "seed").write_text("seed\n")
    subprocess.run(["git", "-C", str(repo), "add", "-A"], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-qm", "seed"], check=True,
                   capture_output=True)

    run = tmp_path / "run"
    run.mkdir()
    (run / "wave.log").write_text("wave_gate: ok\n")
    (run / "summary.json").write_text('{"status": "completed", "verdict": "pass"}\n')
    (run / "wave-sha").write_text("a" * 40 + "\n")

    result, _root = _archive(tmp_path, run, kind="wave-gate",
                             extra=("--wave-sha", "a" * 40, "--status", "completed"))
    assert result.returncode == 0, result.stderr

    # THE INDEX, not the worktree. `git status --porcelain` marks an unstaged new
    # file `??`; a staged one is `A `. The manifest names these files, so anything
    # other than staged is the state this test exists to refuse.
    out = subprocess.run(["git", "-C", str(repo), "status", "--porcelain"],
                         capture_output=True, text=True, check=True).stdout
    untracked = [l for l in out.splitlines() if l.startswith("??")]
    assert not untracked, f"the archiver left files its manifest names unstaged: {untracked}"
    assert any(l.startswith("A ") and "evidence" in l for l in out.splitlines()), (
        f"nothing under the evidence root was staged: {out!r}"
    )


def _repo_with_index(tmp_path):
    """A real checkout with the evidence root seeded, ready to archive into."""
    import subprocess

    repo = tmp_path / "repo"
    (repo / "docs" / "architecture" / "evidence" / "issue-999").mkdir(parents=True)
    (repo / "docs" / "architecture" / "evidence" / "issue-999" / "index.jsonl").write_text(
        json.dumps({"generated_at": "x", "issue": 999, "schema_version": 1,
                    "source_tip": "abc"}) + "\n")
    for args in (["init", "-q"], ["config", "user.email", "t@t"],
                 ["config", "user.name", "t"]):
        subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)
    (repo / "seed").write_text("seed\n")
    subprocess.run(["git", "-C", str(repo), "add", "-A"], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-qm", "seed"], check=True,
                   capture_output=True)
    return repo


def _staging_run(tmp_path, name="run"):
    run = tmp_path / name
    run.mkdir()
    (run / "wave.log").write_text("wave_gate: ok\n")
    (run / "summary.json").write_text('{"status": "completed", "verdict": "pass"}\n')
    (run / "wave-sha").write_text("a" * 40 + "\n")
    return run


def test_an_ignored_manifest_path_is_refused_not_silently_skipped(tmp_path):
    """A directory add succeeds while skipping an ignored child. The manifest names it.

    THE NON-VACUITY WITNESS for verifying each path individually — and the SECOND
    attempt at it. The first ignored `capture.sqlite` and put that file in the run
    directory, where a wave round never copies it: only `WAVE_GATE_FILES` are
    copied, so the archive never contained the ignored path, the success branch was
    always taken, and the test stayed green with the verification deleted. That is
    a fixture that cannot exhibit the defect it names, written in the same batch as
    a rule against exactly that, and it was caught by review rather than by me.

    This ignores `*.log`, which for a wave round means `wave.log` — a file the
    archiver DOES copy and the manifest DOES name. `git add <dir>` then returns 0
    having skipped it, which is the state the per-path check exists to refuse.
    """
    import subprocess

    repo = _repo_with_index(tmp_path)
    (repo / ".gitignore").write_text("*.log\n")
    subprocess.run(["git", "-C", str(repo), "add", "-A"], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-qm", "ignore"], check=True,
                   capture_output=True)

    run = _staging_run(tmp_path)
    result, root = _archive(tmp_path, run, kind="wave-gate",
                            extra=("--wave-sha", "a" * 40, "--status", "completed"))

    # The manifest must actually name the ignored file, or this proves nothing.
    # Asserted on the FAILURE path via the message, since a refused run rolls the
    # manifest back — the copy list is what makes `wave.log` reach it.
    assert result.returncode == 1, (
        "an ignored path the manifest names was staged silently:\n" + result.stdout
    )
    assert "the index does not carry" in result.stderr, result.stderr

    # AND THE TRANSACTION UNWOUND. The round directory is gone and the manifest no
    # longer names it, so the identical command can be retried — the contract a
    # post-transaction staging step had broken.
    assert not (root / "wave-gate" / "run").exists(), "the archive was left published"
    sums = root / "SHA256SUMS"
    assert not sums.is_file() or "wave-gate/run" not in sums.read_text(), (
        "the manifest still names the rolled-back round"
    )
    # ...and no staged ghost of the deleted files remains in the index.
    staged = subprocess.run(
        ["git", "-C", str(repo), "diff", "--cached", "--name-only"],
        capture_output=True, text=True, check=True).stdout
    assert "wave-gate/run" not in staged, f"rollback left staged ghosts: {staged!r}"


def test_a_probe_failure_is_not_read_as_a_scratch_directory(tmp_path):
    """An unreadable repository is unknown, not "no repository".

    THE NON-VACUITY WITNESS for failing closed on the probe. The first version took
    any non-zero probe as "not a checkout" and returned SUCCESS with the archive
    unstaged — so a real worktree whose metadata git refuses would have published a
    manifest naming files no index carried, and said it was fine.
    """
    import shutil

    repo = _repo_with_index(tmp_path)
    # A `.git` that EXISTS and cannot be understood — an invalid gitfile. The probe
    # then fails for a reason that is not "there is no repository here", which is
    # precisely the case the first version read as a scratch directory.
    shutil.rmtree(repo / ".git")
    (repo / ".git").write_text("gitdir: \n")

    result, _root = _archive(tmp_path, _staging_run(tmp_path), kind="wave-gate",
                             extra=("--wave-sha", "a" * 40, "--status", "completed"))
    assert result.returncode == 1, result.stdout
    assert "could not be probed" in result.stderr or "rolled back" in result.stderr, (
        result.stderr
    )


def test_a_rollback_preserves_index_entries_it_did_not_make(tmp_path):
    """The rollback restores the pre-run index, not the subtree at HEAD.

    THE NON-VACUITY WITNESS for exact index restoration — and the second attempt.
    The first staged a loose file into the evidence root and never reached the
    rollback at all: preflight refuses an archive holding files no manifest names,
    so the run died before staging and the branch under test never executed. It
    passed with the fix mutated out, which is what a fixture that cannot exhibit
    the defect always does.

    This builds the real state instead: a SUCCESSFUL archive stages its own files
    and is deliberately left uncommitted — the ordinary condition of this repository
    mid-session — and a second archive then fails during staging. Resetting the
    subtree to HEAD would discard the first archive's entries while reporting "the
    archive is as it was"; restoring the exact pre-run index keeps them.
    """
    import subprocess

    repo = _repo_with_index(tmp_path)
    (repo / ".gitignore").write_text("*.log\n")
    subprocess.run(["git", "-C", str(repo), "add", "-A"], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-qm", "ignore"], check=True,
                   capture_output=True)

    # A first archive that SUCCEEDS and stages itself. A commit-review round carries
    # no `.log`, so nothing is ignored and staging completes.
    first = _commit_review_run(tmp_path, name="cdx-review.FIRST01")
    ok, _root = _archive(tmp_path, first, kind="commit-review")
    assert ok.returncode == 0, ok.stderr
    staged_before = subprocess.run(
        ["git", "-C", str(repo), "diff", "--cached", "--name-only"],
        capture_output=True, text=True, check=True).stdout
    assert staged_before.strip(), "the first archive must leave entries staged"

    # ...then a wave round whose `wave.log` is ignored, so staging fails after the
    # add and the rollback runs with a non-empty pre-run index to preserve.
    result, _root = _archive(tmp_path, _staging_run(tmp_path), kind="wave-gate",
                             extra=("--wave-sha", "a" * 40, "--status", "completed"))
    assert result.returncode == 1, result.stdout

    staged_after = subprocess.run(
        ["git", "-C", str(repo), "diff", "--cached", "--name-only"],
        capture_output=True, text=True, check=True).stdout
    assert staged_after == staged_before, (
        "the rollback changed index entries it did not make:\n"
        f"  before: {staged_before!r}\n  after:  {staged_after!r}"
    )


def test_a_rollback_does_not_destroy_an_archive_file_named_like_its_temp(tmp_path):
    """The rollback's temporary must not collide with an accounted archive file.

    THE NON-VACUITY WITNESS for the collision-proof temporary — and I had recorded
    this fix as UNGRADED, claiming a fault had to be injected. It does not: the
    archive's own filename rules permit `SHA256SUMS.rollback`, so the collision is
    reachable by placing that file and triggering an ordinary staging failure. The
    reviewer pointed that out, and being wrong about what could be tested is how a
    fix ships unmeasured.
    """
    import subprocess

    repo = _repo_with_index(tmp_path)
    (repo / ".gitignore").write_text("*.log\n")
    subprocess.run(["git", "-C", str(repo), "add", "-A"], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-qm", "ignore"], check=True,
                   capture_output=True)

    # An archive that succeeds, so a manifest exists to be restored later...
    ok, root = _archive(tmp_path, _commit_review_run(tmp_path, name="cdx-review.FIRST01"),
                        kind="commit-review")
    assert ok.returncode == 0, ok.stderr

    # ...and a file occupying the name a fixed temporary would have used.
    decoy = root / "SHA256SUMS.rollback"
    decoy.write_text("evidence that must survive\n")
    ok2, _ = _archive(tmp_path, _commit_review_run(tmp_path, name="cdx-review.SECOND1"),
                      kind="commit-review", extra=("--accept-new",))
    assert ok2.returncode == 0, ok2.stderr
    assert "SHA256SUMS.rollback" in (root / "SHA256SUMS").read_text(), (
        "the decoy must be accounted for, or this proves nothing"
    )

    # A staging failure now drives the manifest restore.
    result, _ = _archive(tmp_path, _staging_run(tmp_path), kind="wave-gate",
                         extra=("--wave-sha", "a" * 40, "--status", "completed"))
    assert result.returncode == 1, result.stdout

    assert decoy.is_file(), "the rollback destroyed an accounted archive file"
    assert decoy.read_text() == "evidence that must survive\n", (
        f"the rollback overwrote an accounted archive file: {decoy.read_text()!r}"
    )


def test_a_rollback_restores_index_state_no_listing_can_express(tmp_path):
    """Intent-to-add and skip-worktree survive a rollback, because it copies the file.

    THE NON-VACUITY WITNESS for the structural fix, and the third form this test has
    taken — each form matching the fix it was written against, and each fix a fuller
    MODEL of the git index than the last. Recreating entries from `ls-files -s` lost
    the extended flags; adding `-v` still rendered an `add -N` entry as an ordinary
    `H` row; refusing what those listings could not express rejected a staged
    DELETION, whose zero index mode looks identical to the intent bit in porcelain v2,
    and still missed fsmonitor-valid, which neither listing carries.

    So the fix stopped modelling: `.git/index` is copied byte for byte and put back.
    This asserts the property that ends the class — state the listings cannot express
    SURVIVES — using the two such states that can be created portably. Hand-run
    against the mutant (the copy replaced by the previous `ls-files -s -v` snapshot
    and `--index-info` restore): the intent-to-add entry comes back as a staged empty
    blob and the skip-worktree bit is cleared, both under an exact-restore claim.
    """
    import subprocess

    repo = _repo_with_index(tmp_path)
    (repo / ".gitignore").write_text("*.log\n")
    subprocess.run(["git", "-C", str(repo), "add", "-A"], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-qm", "ignore"], check=True,
                   capture_output=True)

    # Build the state through a SUCCESSFUL archive: preflight refuses an archive
    # holding files no manifest accounts for, so a loose file dies before staging and
    # the branch under test never runs. Two earlier drafts of this fixture did exactly
    # that and passed with the fix mutated out.
    first, root = _archive(tmp_path, _commit_review_run(tmp_path, name="cdx-review.KEEP001"),
                           kind="commit-review")
    assert first.returncode == 0, first.stderr
    accounted = root / "commit-reviews" / "cdx-review.KEEP001" / "review.json"
    assert accounted.is_file()
    other = next(f for f in sorted(accounted.parent.iterdir())
                 if f.is_file() and f != accounted)

    # `git add --patch` leaves an intent-to-add entry behind; skip-worktree is what a
    # sparse checkout sets. Neither is expressible in the listing the old code used.
    subprocess.run(["git", "-C", str(repo), "rm", "--cached", "-q", "--", str(accounted)],
                   check=True, capture_output=True)
    subprocess.run(["git", "-C", str(repo), "add", "-N", "--", str(accounted)],
                   check=True, capture_output=True)
    subprocess.run(["git", "-C", str(repo), "update-index", "--skip-worktree", "--",
                    str(other)], check=True, capture_output=True)

    def index_state():
        return subprocess.run(
            ["git", "-C", str(repo), "ls-files", "-s", "-v", "--", str(root)],
            capture_output=True, text=True, check=True).stdout, subprocess.run(
            ["git", "-C", str(repo), "status", "--porcelain=v2", "--", str(root)],
            capture_output=True, text=True, check=True).stdout

    before = index_state()
    assert " 000000 " in before[1], "the fixture must hold an intent-to-add entry"
    assert any(l[:1] == "S" for l in before[0].splitlines()), (
        "the fixture must hold a skip-worktree entry")

    # A wave round whose `wave.log` is ignored: staging fails, the rollback runs.
    result, _root = _archive(tmp_path, _staging_run(tmp_path), kind="wave-gate",
                             extra=("--wave-sha", "c" * 40, "--status", "completed"))
    assert result.returncode == 1, result.stdout

    after = index_state()
    assert after == before, (
        "the rollback did not put back index state the listings cannot express:\n"
        f"  before: {before!r}\n  after:  {after!r}")


def test_a_rollback_restores_the_manifest_mode_it_found(tmp_path):
    """The mode is snapshotted beside the bytes, not read back after the replace.

    THE NON-VACUITY WITNESS for the mode capture. `write_sums` writes atomically,
    so by the time a rollback runs the manifest on disk is a NEW file carrying the
    default mode — reading `SHA256SUMS.stat()` there reads the replacement, and a
    0640 manifest comes back at whatever the umask gave while the rollback reports
    the archive unchanged. Hand-run against the mutant (the mode read moved back
    after `write_sums`): the restored manifest is 0644, not 0640, and the run still
    prints that the archive is as it was.
    """
    import subprocess

    repo = _repo_with_index(tmp_path)
    (repo / ".gitignore").write_text("*.log\n")
    subprocess.run(["git", "-C", str(repo), "add", "-A"], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-qm", "ignore"], check=True,
                   capture_output=True)

    first = _commit_review_run(tmp_path, name="cdx-review.MODE001")
    ok, root = _archive(tmp_path, first, kind="commit-review")
    assert ok.returncode == 0, ok.stderr

    sums = root / "SHA256SUMS"
    os.chmod(sums, 0o640)
    assert sums.stat().st_mode & 0o7777 == 0o640

    # A wave round whose `wave.log` is ignored: staging fails, the rollback runs.
    result, _root = _archive(tmp_path, _staging_run(tmp_path), kind="wave-gate",
                             extra=("--wave-sha", "b" * 40, "--status", "completed"))
    assert result.returncode == 1, result.stdout
    assert sums.stat().st_mode & 0o7777 == 0o640, (
        "the rollback replaced the manifest's mode with the temporary's: "
        f"{oct(sums.stat().st_mode & 0o7777)}")
