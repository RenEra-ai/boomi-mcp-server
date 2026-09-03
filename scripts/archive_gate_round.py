#!/usr/bin/env python3
"""Archive one gate round into an issue's durable evidence directory (D28).

Every review round must be archived AS IT IS COLLECTED, not reconstructed later:
#152 fabricated six attestations by reading findings out of `wait` instead of the
collector, and orphaned seven daemons doing it. The archive is what makes a
completion claim checkable, so this script copies only what the collector itself
wrote, derives the index row from those bytes, and refuses anything it cannot
place.

Three round kinds, because the slice's roster has three gates that produce
durable state:

  commit-review    a Stage-2 Codex commit review (`commit-review-collect`)
  architect-review a §6 architect implementation review (`gate-attest`)
  wave-gate        a composite wave-gate run

Usage:

    archive_gate_round.py --issue 155 --kind commit-review \\
        --run-dir /tmp/cdx-review.XXXXXX --logical-loop "L2 (Stage-2 ...)"

    archive_gate_round.py --issue 155 --kind wave-gate \\
        --run-dir /tmp/wave.XXXX --logical-loop "L4 (composite wave gate)" \\
        --wave-sha <sha> --status completed

Then `git add` the archive BEFORE running the scanners: they compare SHA256SUMS
against the GIT INDEX, so a file that exists only in the worktree reads as
listed-but-untracked and fails the gate.
"""

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

#: Copied for a commit-review round. `cache-dir` and `missing-probes` are
#: deliberately absent: the first is a machine-local path that says nothing
#: about the review, and the second is poll bookkeeping. Archiving either makes
#: the record non-reproducible on another machine.
COMMIT_REVIEW_FILES = (
    "baseline",
    "cwd",
    "dirty",
    "scope",
    "start-head",
    "last-reviewed-sha",
    "t0",
    "teardown",
    "start.json",
    "review.json",
    "phase",
)

#: Copied for a §6 architect round. `refusal.json` is included on purpose: a
#: refused round is evidence too, and a round that left one behind must not look
#: like a round that never happened.
ARCHITECT_FILES = (
    "start.json",
    "attestation.json",
    "review.md",
    "refusal.json",
)

WAVE_GATE_FILES = (
    "wave.log",
    "summary.json",
    "manifests.log",
    "suite.log",
)

#: subdir, files, collector, indexed. `indexed` is the load-bearing column.
#:
#: A wave-gate round is archived but NOT indexed, and that is a contract with
#: the repository's archive scanner rather than an omission. That scanner reads
#: `start.json` for every indexed row and takes its `threadId` as the round's
#: identity, accepting only the two collectors that produce one. A wave-gate run
#: has no daemon and therefore no thread: indexing it wrote a row the scanner
#: refuses, so the mode advertised here would have produced an archive that
#: fails the very gate it exists to evidence. Its files are still archived and
#: still covered by the checksum manifest — what it does not get is a row
#: claiming a collector attested it, because none did.
KINDS = {
    "commit-review": ("commit-reviews", COMMIT_REVIEW_FILES, "commit-review-collect", True),
    "architect-review": ("architect-reviews", ARCHITECT_FILES, "gate-attest", True),
    "wave-gate": ("wave-gate", WAVE_GATE_FILES, "wave_gate", False),
}


def _confirmed_removed(path: Path) -> bool:
    """True only when the path is provably GONE — nothing weaker counts.

    `Path.exists()` is not a verification under the faults this is used to check
    for. Measured on the interpreters this repository runs: it re-raises for
    EACCES and EIO — the very errors a failed cleanup reports — so using it
    inside a failure handler escapes that handler without its diagnostic. And it
    follows symlinks, so a DANGLING symlink answers False while the directory
    entry is still there for the next run's scan to trip over.

    `lstat` raising ENOENT is the only answer that means removed. A successful
    lstat means something is still there; any other error means nobody knows.
    """
    try:
        os.lstat(path)
    except FileNotFoundError:
        return True
    except OSError:
        return False
    return False


def _remove_confirmed(path: Path) -> bool:
    """Remove a path and report whether it is PROVABLY gone.

    Every cleanup in this file exists so an operator can fix what the error names
    and run the same command again. A removal that silently failed defeats that:
    the leftover is refused as unaccounted by the next run, and an operator told
    the archive is clean looks anywhere except at the thing blocking them. So no
    site here claims a cleanliness it has not verified — this returns the fact and
    each caller reports it honestly.
    """
    # BOTH shapes. `rmtree` silently does nothing to a regular file, so a helper
    # that only called it left every file-shaped target in place while reporting
    # through the confirmation below — which is how the checksum staging file
    # came to be probed but never deleted.
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path, ignore_errors=True)
    else:
        try:
            path.unlink()
        except (FileNotFoundError, IsADirectoryError, PermissionError, OSError):
            pass
    return _confirmed_removed(path)


def _discard(durable: Path, parent_existed: bool = True) -> None:
    """Remove a refused round, and the parent only if THIS run created it.

    An empty `architect-reviews/` left behind by a refusal reads as "a round was
    archived here", which is the impression a refusal exists to avoid — but
    removing one that was already there is a different act entirely: it deletes
    something the run did not create, on the strength of it happening to be
    empty. `parent_existed` says which case this is; the default is the safe one.
    """
    shutil.rmtree(durable, ignore_errors=True)
    parent = durable.parent
    if not parent_existed and parent.is_dir() and not any(parent.iterdir()):
        parent.rmdir()


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_text(directory: Path, name: str):
    """One sidecar's stripped content, or None when it was never written."""
    path = directory / name
    if not path.is_file():
        return None
    text = path.read_text().strip()
    return text or None


class Unreadable:
    """A sidecar that EXISTS but cannot be parsed. Not the same as absent.

    Collapsing the two is how a malformed attestation was archived at exit 0: the
    presence guard saw None, concluded the round was simply unattested, and let it
    through with a null gate and status. The downstream scanner then rejects the
    result — so the producer had already reported success for output its consumer
    refuses, and may have stranded the source run on the strength of it.
    """

    def __init__(self, path, reason):
        self.path, self.reason = path, reason

    def __repr__(self):
        return f"<unreadable {self.path}: {self.reason}>"

    def __getattr__(self, name):
        # Deliberately LOUD. This sentinel is truthy on purpose — making it
        # falsey would let `x or {}` swap it for an empty mapping and archive a
        # corrupt round as an empty one — so a leak must announce itself rather
        # than surface as a bare attribute error three frames away.
        raise AssertionError(
            f"unreadable sidecar {self.path} reached {name!r}: {self.reason}. "
            "This should have been refused in the pre-flight; the round must not "
            "have been published.")


def read_json(directory: Path, name: str):
    """Parsed content, None when the file is ABSENT, or `Unreadable` when it is not.

    Callers that only care whether a fact is available may treat `Unreadable` as
    falsey-ish by testing `isinstance`; callers that must not proceed on a
    corrupt record check for it explicitly. The distinction is the point.
    """
    path = directory / name
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text())
    except (ValueError, OSError) as exc:
        return Unreadable(path, f"{type(exc).__name__}: {exc}")


#: Where each architect-round index field lives INSIDE a `gate-attest`
#: attestation. It is a mapping rather than four inline lookups because the
#: inline version was written from what the collector prints to STDOUT, not from
#: what it writes to the FILE, and read three keys the attestation has never
#: carried — so the one architect round archived before this went in recorded a
#: null prompt digest with the real one sitting two keys away, and a null
#: reviewed sha, and nothing noticed either.
#:
#: These paths are not invented here either, which is the point. They are the
#: paths the CONSUMER already reads: `tests/test_wave_gate.py`, the scanner that
#: validates these archives, resolves `turn.status`, `parsedVerdict` and
#: `prompt.actualSha256` out of the same attestation and always has. Keeping a
#: second copy of that schema is what produced every defect in this file.
ARCHITECT_ROW_PATHS = {
    "gate": ("gate",),
    "prompt_sha256": ("prompt", "actualSha256"),
    "status": ("turn", "status"),
    "verdict": ("parsedVerdict",),
}

#: THE COLLECTOR STOPPED SOURCING A VERDICT FOR ARCHITECT ROUNDS. It parses one
#: only for a `review` gate and writes an explicit null for an `architect` one,
#: so a round collected by the installed runtime carries the key with no value
#: while seven older archives here carry a populated one. Requiring a value the
#: producer no longer emits would refuse every future architect round.
#:
#: The verdict is DERIVED from the artifact instead, and only from its own final
#: line — the same one shared rule the collector applies, over bytes the
#: attestation already binds by sha256. Nothing here is typed by hand: an
#: artifact whose last non-empty line is not a verdict yields none, and the round
#: is archived without one rather than with a guess.
#: The two verdicts the architect gate prompt permits. A closed set, because the
#: prompt that binds the session states exactly these and the row records what the
#: gate concluded — a value outside them is a turn that did not conclude.
ARCHITECT_VERDICTS = ("NO ISSUES", "ISSUES FOUND")


def architect_verdict_from_artifact(run_dir: Path, attestation: dict):
    """The verdict a collected architect round carries, or None."""
    # STRIPPED, because `not "\n"` is False and a whitespace-only verdict would
    # otherwise satisfy this the way it once satisfied the check this replaced.
    attested = attestation.get("parsedVerdict")
    if isinstance(attested, str) and attested.strip() in ARCHITECT_VERDICTS:
        # The collector's own parser produced this one, under the shared rule.
        return attested.strip()
    # THE ENUM IS THE PROTOCOL, so it binds BOTH sources. The first version
    # exempted this branch on the reasoning that the collector had already parsed
    # it — but the collector's shared rule can also yield `UNCLEAR`, which is not
    # one of the two verdicts the gate prompt defines, and any other string it
    # returned would have been archived verbatim. A constraint applied to one of
    # two paths that produce the same field is a constraint on neither. Falling
    # through rather than refusing outright: the artifact may still carry a
    # well-formed verdict when the collector could not parse one.
    artifact = run_dir / "review.md"
    if not artifact.is_file():
        return None
    try:
        text = artifact.read_text(encoding="utf-8")
    except (UnicodeDecodeError, OSError):
        # UNREADABLE IS NO VERDICT, and it must not be an exception here. This
        # runs AFTER the staging directory has been renamed to its durable
        # destination, and the raise was outside the rollback that guards the
        # rename — so undecodable bytes left a published directory nothing
        # accounted for, which the next run refuses as unaccounted and which
        # therefore blocks the corrected retry. Returning None routes it into the
        # caller's existing unresolved-verdict path, which removes the directory
        # it just published. The digest preflight does not catch this because it
        # hashes raw bytes and never decodes them.
        return None
    for line in reversed([l.strip() for l in text.splitlines()]):
        if not line:
            continue
        if not line.startswith("VERDICT:"):
            return None
        verdict = line[len("VERDICT:"):].strip()
        # THE PROTOCOL'S OWN ENUM. The gate prompt permits exactly two verdicts,
        # and anything else on that line — a truncated turn, a paraphrase, a
        # model that answered in prose — is not one of them. Returned as-is it
        # was merely truthy, so the round archived as completed carrying a
        # verdict no reader can act on and no gate defines.
        return verdict if verdict in ARCHITECT_VERDICTS else None
    return None

#: The attestation `gate-attest` writes carries no reviewed SHA under any
#: spelling — checked against a real one, whose top-level keys are schema,
#: gateProtocol, gate, collectedAt, start, prompt, turn, artifact, inputPlan,
#: parsedVerdict and teardown. So THIS script cannot source one, and the rows it
#: creates carry null deliberately rather than probing invented keys.
#:
#: That is a statement about this derivation, NOT about the field. Twenty-four
#: architect rows across seven other archives in this repository DO carry a
#: populated reviewed sha, put there by a different producer. A re-derivation
#: must therefore never overwrite one with this null — which is exactly what
#: `rederive_index` refuses to do, and why it refuses rather than merges when
#: it meets a row shape it does not model.
#: The commit-review branch has a real sidecar for this and uses it.
#:
#: No mutation can tell this constant apart from the key-probing it replaced —
#: both yield null on every attestation that exists, which is exactly why the
#: probing version looked fine for as long as it did. The testable claim is the
#: PREMISE, that no reviewed-sha spelling is present at all, and that is what
#: the test asserts. Recorded so nobody later mistakes this for covered code.
ARCHITECT_ROUND_RECORDS_NO_REVIEWED_SHA = None


def _dig(mapping, path):
    """Resolve a path, returning None at the first level that is not a dict.

    The type check is load-bearing: `attestation.get("prompt") or {}` treated a
    truthy non-dict as a mapping and raised AFTER the destination directory had
    been created, so the half-made archive then blocked the operator's corrected
    retry. Here a malformed attestation resolves to None and is refused below,
    before anything is written.
    """
    node = mapping
    for key in path:
        if not isinstance(node, dict):
            return None
        node = node.get(key)
    return node


def derive_row(kind: str, run_dir: Path, durable: Path, rel_root: Path,
               logical_loop: str, status_override, wave_sha):
    """The index row, derived from the COPIED bytes rather than restated.

    A row that repeated what the caller claimed would attest the caller, not the
    round. Everything here is read back out of the archive directory.
    """
    subdir, _files, collector, _indexed = KINDS[kind]
    files = {}
    for path in sorted(durable.rglob("*")):
        if path.is_file():
            files[str(path.relative_to(rel_root))] = sha256_of(path)

    row = {
        "collector": collector,
        "durable_dir": str(durable.relative_to(rel_root)),
        "files": files,
        "logical_loop": logical_loop,
        "source_run_dir": str(run_dir),
    }

    if kind == "commit-review":
        start = read_json(durable, "start.json") or {}
        reviewed = read_text(durable, "last-reviewed-sha")
        start_head = read_text(durable, "start-head")
        phase = read_text(durable, "phase")
        # A round counts as completed ONLY when the recorded reviewed SHA equals
        # the SHA the daemon actually started on. A missing or mismatched
        # last-reviewed-sha is exactly the failed round that must not shrink the
        # next review's scope, so it is never laundered into a completion.
        completed = bool(reviewed) and reviewed == start_head
        row.update({
            "baseline": read_text(durable, "baseline"),
            "dirty": read_text(durable, "dirty"),
            "reviewed_sha": reviewed if completed else None,
            "scope": read_text(durable, "scope"),
            "started_at": start.get("startedAt"),
            "status": status_override or ("completed" if completed else (phase or "failed")),
            "teardown": read_text(durable, "teardown"),
            "verdict": None,
        })
    elif kind == "architect-review":
        attestation = read_json(durable, "attestation.json")
        if isinstance(attestation, Unreadable):
            attestation = None      # the archive path refuses these before we get here
        refusal = read_json(durable, "refusal.json")
        row.update({name: _dig(attestation, path)
                    for name, path in ARCHITECT_ROW_PATHS.items()})
        # THE ROW GETS THE VALIDATED VERDICT, ALWAYS — not only when the
        # attestation carried none. Taking the raw value whenever it was truthy
        # meant the enum decided whether the round could be archived and then
        # played no part in what was archived: an attestation carrying an
        # unclear verdict beside an artifact carrying a valid one passed
        # preflight through the fallback and still wrote the unclear value into
        # the index, which the downstream scanner then trusts. A constraint that
        # gates admission but not the record is a constraint on the gate only.
        row["verdict"] = architect_verdict_from_artifact(durable, attestation or {})
        row["reviewed_sha"] = ARCHITECT_ROUND_RECORDS_NO_REVIEWED_SHA
        if status_override:
            row["status"] = status_override
        elif refusal:
            row["status"] = "refused"
        # Otherwise the status is whatever `turn.status` resolved to — including
        # None for an attestation that carries none. That is deliberate: the
        # archive scanner accepts only "completed" or a status named in its
        # reasoned allowlist, so an unreadable attestation fails there instead of
        # being defaulted into a completion here.
    else:
        summary = read_json(durable, "summary.json") or {}
        row.update({
            "status": status_override or summary.get("status") or "completed",
            "verdict": summary.get("verdict"),
            "wave_sha": wave_sha or summary.get("wave_sha"),
        })
    return row


def read_sums(root: Path):
    """The manifest as a name -> digest mapping, or empty when there is none."""
    listed = {}
    if (root / "SHA256SUMS").is_file():
        for line in (root / "SHA256SUMS").read_text().splitlines():
            if line:
                digest, name = line.split("  ", 1)
                listed[name] = digest
    return listed


#: The checksum manifest is a line-oriented `<digest>  <name>` text format that
#: this repository's archive scanner reads, and it is NOT this script's to
#: change. That means some names simply cannot be recorded in it, and three
#: separate findings were all that one fact wearing different clothes: a newline
#: in a name produced a manifest nobody could parse again; a decomposed-unicode
#: name produced a manifest that disagreed with the git index; a symlinked
#: directory made the scan silently erase lines.
#:
#: Encoding around each of those in turn is sanitising the output of an
#: unbounded space, which this repository has already learned does not converge.
#: The input is bounded instead: a name that cannot be recorded faithfully is
#: REFUSED, and the limit is stated rather than worked around.
def refuse_unrecordable_names(root: Path) -> None:
    """Refuse any archived path the manifest cannot represent faithfully."""
    import unicodedata

    # The test below asks `str.splitlines()` itself rather than comparing against
    # a list of separators. Hand-listing them is how this check shipped seeing
    # only carriage return and newline while `splitlines()` also breaks on the
    # vertical tab, the form feed, three file/group/record separators, NEL and
    # both Unicode line and paragraph separators — eight it did not know about.
    # The manifest is read back with `splitlines()`, so `splitlines()` is the
    # authority on what breaks it, and asking the authority cannot drift.
    offenders = {}
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            # `rglob` follows these when hashing but records the link's own
            # path, so the manifest ends up describing something other than what
            # was hashed — and a symlinked directory drops its real entries.
            offenders.setdefault("a symlink, which the manifest cannot describe",
                                 []).append(str(path.relative_to(root)))
            continue
        if not path.is_file():
            continue
        rel = str(path.relative_to(root))
        if len(rel.splitlines()) > 1 or rel.splitlines() != [rel]:
            offenders.setdefault("a line separator, which ends the manifest "
                                 "record mid-name", []).append(repr(rel))
        elif rel != unicodedata.normalize("NFC", rel):
            offenders.setdefault("a decomposed form that git stores differently, "
                                 "so the manifest and the git index disagree",
                                 []).append(repr(rel))
    if offenders:
        raise SystemExit(
            "refusing: these archived paths cannot be recorded faithfully in a "
            "line-oriented checksum manifest, and a manifest that cannot be read "
            "back is worse than none:\n" + "\n".join(
                f"  {why}: {what}" for why, what in sorted(offenders.items())
            ) + "\nRename them before archiving."
        )


def scan_archive(root: Path):
    """Every archived file's current digest. Any file NAMED SHA256SUMS is excluded.

    Not just the top-level one: a nested checksum file copied in from a capture
    would otherwise be listed by the parent while the archive test's own scan
    skips it, and the two disagree.
    """
    return {
        str(path.relative_to(root)): sha256_of(path)
        for path in sorted(root.rglob("*"))
        if path.is_file() and path.name != "SHA256SUMS"
    }


def refuse_unless_archive_is_accounted_for(root: Path, accept_new: bool = False) -> None:
    """Check BEFORE anything is written. Pure — this function never mutates.

    Two separate lessons are encoded here, both learned from findings against
    earlier versions of this file.

    The first is WHAT to compare. Comparing only the set of NAMES let a file
    whose bytes changed under an unchanged path pass straight through, and the
    rewrite then recorded its new digest — turning a hash mismatch the archive
    scanner would have caught into evidence that looks valid. Names, digests and
    absences are all compared now.

    The second is WHEN to compare. Every earlier version raised only after the
    mutation it was guarding: the index was already rewritten, or the round
    directory and its row already created. That left the archive in a state the
    operator could not simply retry out of — the destination now exists, so the
    overwrite refusal blocks the corrected run, and an index already
    canonicalised makes the next run think there is nothing to do. So the check
    is a separate function, and it runs first.

    There is no per-caller variation, and removing it is the third lesson. The
    check once exempted "appeared" names while archiving, on the reasoning that
    a new round brings new files — but it runs BEFORE the round's directory is
    created, so nothing it can see was ever created by this invocation. The
    exemption could not admit what it was written for and admitted only files
    that predated the run: precisely the on-disk-but-unlisted state this exists
    to catch, which the rewrite then recorded as valid evidence.

    `accept_new` is the deliberate way to list a file that arrived outside this
    script — a live capture, most often. It exists because that flow is real:
    most files in this archive are captures, listed in the manifest and
    referenced by no index row, and until now they were absorbed silently by
    whichever round happened to be archived next. Accepting them is now an act
    the operator asks for and the output records.
    """
    refuse_unrecordable_names(root)
    listed, on_disk = read_sums(root), scan_archive(root)
    if not listed:
        # A brand-new archive has no manifest, but that is not licence to accept
        # whatever is already lying in it: a bootstrap has nothing on disk yet,
        # so anything present arrived from somewhere this script cannot vouch
        # for. Only an empty archive bootstraps silently.
        # `index.jsonl` is the archive's OWN skeleton, created when the archive
        # is opened and re-derivable from the rounds themselves — it is not
        # evidence that arrived from elsewhere, so it does not make a bootstrap
        # suspicious. Anything else present before the first round did.
        strangers = sorted(set(on_disk) - {"index.jsonl"})
        if strangers and not accept_new:
            raise SystemExit(
                "refusing: this archive has no manifest yet, but already holds "
                f"files this run did not create: {strangers[:5]}. Pass "
                "--accept-new to record them deliberately.")
        return

    unaccounted = {}
    vanished = sorted(set(listed) - set(on_disk))
    appeared = sorted(set(on_disk) - set(listed))
    rewritten = sorted(
        name for name in set(listed) & set(on_disk)
        if listed[name] != on_disk[name]
    )
    if vanished:
        unaccounted["listed but no longer on disk"] = vanished
    if appeared and not accept_new:
        unaccounted["on disk but never listed (pass --accept-new to record them "
                    "deliberately)"] = appeared
    if rewritten:
        unaccounted["listed but its bytes have changed"] = rewritten
    if not unaccounted:
        return

    raise SystemExit(
        "refusing to touch this archive: it already differs from what its own "
        "manifest records, and rewriting would absorb a change this run did not "
        "make. For most files here the manifest is the only record there is. "
        "The difference is usually external, but not always — a previous run of "
        "this script that failed before it was made rollback-safe could have "
        "left it, so check the round directories named below before assuming "
        "someone else touched the archive:\n"
        + "\n".join(f"  {why}: {what}" for why, what in sorted(unaccounted.items()))
        + "\nRestore the archive, or record the change deliberately."
    )


def write_sums(root: Path) -> int:
    """Rewrite SHA256SUMS over every archived file. Call the check FIRST.

    ATOMICALLY. A plain write truncates first and then fills, so a failure part
    way — a full disk, an I/O error — left a truncated manifest behind while the
    caller's rollback restored only the index and reported the archive intact.
    A truncated manifest is worse than a wrong one: an empty one reads as a
    brand-new archive to the check above, which then has nothing to compare and
    lets everything through. Writing beside it and moving it into place means
    the manifest is either wholly the old one or wholly the new one.
    """
    refuse_unrecordable_names(root)
    on_disk = scan_archive(root)
    body = "\n".join(f"{digest}  {name}" for name, digest in sorted(on_disk.items())) + "\n"
    staging = root / f".SHA256SUMS.partial-{os.getpid()}"
    try:
        staging.write_text(body)
        os.replace(staging, root / "SHA256SUMS")
    finally:
        # CONFIRMED, like every other removal here. This one was missed by a
        # sweep that enumerated two call SHAPES — `rmtree(staging…)` and
        # `_discard(durable…)` — instead of the property "every removal". A
        # surviving partial manifest is refused as unaccounted by the next run,
        # while the rollback above reports the archive is as it was.
        # REMOVE, then confirm. The previous line called the confirmation helper
        # alone, which only probes — so replacing an unchecked delete with it
        # removed the deletion entirely and left the partial manifest behind on
        # every failure. A verification is not a substitute for the action it
        # verifies, and swapping one for the other made this strictly worse than
        # the unchecked delete it replaced.
        if not _remove_confirmed(staging):
            print(f"  warning: {staging} could not be confirmed removed and will "
                  "be refused as unaccounted by the next run.", file=sys.stderr)
    return len(on_disk)


def rederive_index(root: Path) -> int:
    """Recompute every index row from the archived bytes it describes.

    The index is DERIVED data, and derived data goes stale the moment its
    derivation changes. That is not hypothetical here: correcting the
    attestation paths above turned the one archived architect row's null prompt
    digest into the real one, and without this the archive would keep serving
    the null while the script that produced it had already been fixed — a
    disagreement nothing would report, since the scanner does not check these
    fields against the attestation.

    Two facts are NOT in the archived bytes and are carried over from the
    existing row rather than guessed: the logical loop the round belongs to, and
    the run directory it came from. Everything else is re-read.
    """
    index = root / "index.jsonl"
    if not index.is_file():
        raise SystemExit(f"no index to re-derive at {index} — check --issue and --repo")
    lines = [l for l in index.read_text().splitlines() if l.strip()]
    if not lines:
        raise SystemExit(f"{index} is empty — there is nothing to re-derive")

    # The first line is the archive HEADER, not a round. Verify that rather than
    # assuming it: an index whose first line is a ROW would otherwise have that
    # row silently demoted into the header slot and dropped from the rewrite —
    # a byte of evidence lost at exit 0, which is the one outcome this whole
    # guard exists to prevent.
    header, rest = lines[0], lines[1:]
    probe = json.loads(header)
    if "durable_dir" in probe or "collector" in probe:
        raise SystemExit(
            f"{index} has no header line — its first line is a round row "
            f"({probe.get('durable_dir')}). Refusing, because rewriting would "
            "drop that row."
        )
    rows = [json.loads(l) for l in rest]

    by_collector = {c: k for k, (_s, _f, c, indexed) in KINDS.items() if indexed}

    # FAIL CLOSED on an archive this script did not write. Measured across the
    # thirteen archives in this repository: issues 152, 153, 171 and 175 carry
    # row fields no derivation here produces — `ordinal`, `ledger_cited`,
    # `scope_provenance`, `reconciled_disposition`, `verdict_source`,
    # `collected_at`, `thread_id`, `turn_token` and more. Re-deriving those rows
    # would replace each with this script's narrower shape and destroy the rest
    # IRREVERSIBLY, and `--issue` accepts any number, so the hazard is one
    # typo away. An unrecognised field means the row has a producer this script
    # does not model, and the only safe answer is to decline the whole run.
    foreign = {}
    for row in rows:
        kind = by_collector.get(row.get("collector"))
        durable = root / row["durable_dir"]
        if kind is None or not durable.is_dir():
            continue
        known = set(derive_row(kind, Path(row["source_run_dir"]), durable, root,
                               row["logical_loop"], None, row.get("wave_sha")))
        extra = sorted(set(row) - known - {"wave_sha"})
        if extra:
            foreign[row["durable_dir"]] = extra
    if foreign:
        raise SystemExit(
            "refusing to re-derive an index carrying fields this script does not "
            "produce — another producer wrote these rows and re-deriving them "
            "would discard what it recorded:\n" + "\n".join(
                f"  {d}: {fields}" for d, fields in sorted(foreign.items())
            )
        )

    changed, out = 0, []
    for row in rows:
        kind = by_collector.get(row.get("collector"))
        durable = root / row["durable_dir"]
        if kind is None or not durable.is_dir():
            out.append(row)
            continue
        fresh = derive_row(kind, Path(row["source_run_dir"]), durable, root,
                           row["logical_loop"], None, row.get("wave_sha"))
        # A status the CALLER overrode (a failed or timed-out round) is not
        # recoverable from the bytes, so it is preserved rather than recomputed
        # into a false completion.
        if row.get("status") != fresh.get("status") and row.get("status") in _NON_DERIVED_STATUSES:
            fresh["status"] = row["status"]

        # START FROM THE EXISTING ROW and update only what this derivation both
        # owns and can source. The previous shape — build a fresh row, then copy
        # back anything it left null — was wrong in two measured ways. It
        # resurrected a reviewed sha onto a commit-review round the bytes now
        # derive as FAILED, because "null" there is a deliberate verdict rather
        # than an absence. And it silently narrowed a richer value written under
        # a key this derivation also produces, which no copy-back can catch
        # because a narrowed value is not null.
        merged, narrowed, disagreed = dict(row), [], {}
        for name, value in fresh.items():
            if name in UNSOURCEABLE_BY_KIND.get(kind, ()):
                continue  # keep whatever the original producer recorded
            existing = row.get(name)
            if existing is None:
                merged[name] = value
                continue
            if value is None:
                # A null this derivation SOURCED is a verdict, not a gap — the
                # failed-round case above is exactly it — so it wins. The fields
                # where null means "nothing to say" are listed as unsourceable
                # and were skipped before reaching here.
                merged[name] = None
                continue
            if type(existing) is not type(value):
                narrowed.append(name)
                continue
            if isinstance(value, dict):
                # Two different disagreements, and calling both "narrowing" sent
                # an operator looking for the wrong thing: a KEY SET difference
                # means the archive on disk is not the archive the row records —
                # a file appeared or vanished — while a VALUE TYPE difference
                # means another producer recorded something richer here.
                if set(existing) != set(value):
                    disagreed[name] = {
                        "on disk but not in the row": sorted(set(value) - set(existing)),
                        "in the row but not on disk": sorted(set(existing) - set(value)),
                    }
                    continue
                if not all(type(existing.get(k)) is type(v) for k, v in value.items()):
                    narrowed.append(name)
                    continue
            merged[name] = value
        if disagreed:
            raise SystemExit(
                f"refusing to re-derive {row['durable_dir']}: what is on disk is "
                "not what this row records, and a re-derivation neither adds nor "
                "removes files — so the difference came from outside:\n" + "\n".join(
                    f"  {field}: {detail}" for field, detail in sorted(disagreed.items())
                )
            )
        if narrowed:
            raise SystemExit(
                f"refusing to re-derive {row['durable_dir']}: this script would "
                f"narrow {sorted(narrowed)}, whose recorded shape it does not "
                "model. Another producer wrote that value; re-deriving would "
                "replace it with something smaller."
            )
        if merged != row:
            changed += 1
        out.append(merged)

    # Write only if the BYTES move, and report that rather than the row count.
    # The two are not the same question, and answering the wrong one produced an
    # archive that exited 0 internally inconsistent: an index that is
    # semantically identical but bytewise noncanonical — unsorted keys from a
    # hand edit, a blank line, a missing trailing newline — is still rewritten
    # here in canonical form, while a row count of zero skipped the checksum
    # regeneration and left the manifest describing the bytes that used to be
    # there. Reproduced before fixing: the manifest and the index disagreed and
    # the archive scanner rejects exactly that.
    rendered = header + "\n" + "".join(
        json.dumps(row, sort_keys=True) + "\n" for row in out)
    # BYTES, not decoded text. `read_text()` applies universal-newline
    # translation, so a CRLF index compared equal to its LF rendering and was
    # left un-canonicalised while this comment claimed to be comparing bytes —
    # the claim outrunning the code, again.
    if rendered.encode() == index.read_bytes():
        return 0
    index.write_bytes(rendered.encode())
    # At least one byte moved, so the manifest must be refreshed even when no
    # ROW changed. Reported as one so the caller regenerates; the printed row
    # count is separate and stays honest about what was re-derived.
    return changed or _INDEX_BYTES_ONLY


#: The JSON sidecars each kind reads. A PRESENT one must parse to an object, or
#: the round is refused before anything is created.
#:
#: Checked in one place rather than at each call site, because the per-site
#: version is what failed: the unreadable sentinel introduced for the attestation
#: is truthy, so `start.get(...)` and `summary.get(...)` in the row builder kept
#: it through their `or {}` fallbacks and raised — and the row builder runs AFTER
#: the round has been published, so the failure landed exactly where a retry is
#: hardest. A syntactically valid `null` slipped the same guard for the opposite
#: reason: it parses fine, to nothing usable.
READ_JSON_BY_KIND = {
    "commit-review": ("start.json", "review.json"),
    "architect-review": ("start.json", "attestation.json", "refusal.json"),
    "wave-gate": ("summary.json",),
}


#: What the CONSUMER requires of a completed round, per kind — the archive
#: scanner in `tests/test_wave_gate.py`, which is the only thing that decides
#: whether an archived round is usable. Validating merely that a sidecar parses
#: to an object is a weaker contract than the one it must satisfy, and the gap
#: is not academic: an attestation missing its teardown, artifact or thread
#: binding parses perfectly and is then rejected downstream, so the producer
#: reports success for evidence its consumer refuses.
CONSUMER_REQUIRES = {
    "architect-review": {
        "attestation.json": (
            # `parsedVerdict` is NOT required here any more: the installed
            # collector writes it null for an architect gate, and the verdict is
            # derived from the artifact the attestation binds instead.
            ("teardown",), ("turn", "status"),
            ("artifact", "path"), ("artifact", "sha256"),
            ("start", "threadId"), ("prompt", "actualSha256"),
        ),
    },
    "commit-review": {"start.json": (("threadId",),)},
}


def refuse_unusable_json(source: Path, kind: str) -> None:
    """Every present sidecar must be readable AND satisfy the consumer's contract."""
    unusable = {}
    for name in READ_JSON_BY_KIND.get(kind, ()):
        path = source / name
        if not path.exists():
            continue                       # absent is a fact, not a corruption
        if not path.is_file():
            # A DIRECTORY at a sidecar's path is not an absent sidecar. The
            # presence test used to ask `is_file()`, which answers no for a
            # directory and sent it down the absent branch — archiving the round
            # at exit 0 with every derived field null.
            unusable[name] = f"is a {'directory' if path.is_dir() else 'special file'}"
            continue
        parsed = read_json(source, name)
        if isinstance(parsed, Unreadable):
            unusable[name] = parsed.reason
        elif not isinstance(parsed, dict):
            unusable[name] = f"parsed to {type(parsed).__name__}, not an object"
        else:
            missing = [
                "/".join(fp) for fp in CONSUMER_REQUIRES.get(kind, {}).get(name, ())
                if _dig(parsed, fp) in (None, "")
            ]
            if missing:
                unusable[name] = (
                    f"parses, but the archive scanner requires {missing} and this "
                    "does not carry them")

    # ...and the CROSS-FILE constraints, which presence checks cannot express.
    # The scanner does not merely require these facts to exist; it requires them
    # to AGREE — the attestation's thread must be the archived session's thread,
    # its artifact digest must be the archived review's digest, and its teardown
    # and turn status must read exactly the words it checks for. An attestation
    # can carry every required path and still be rejected, which is precisely
    # what a presence-only check publishes.
    if kind == "architect-review" and "attestation.json" not in unusable:
        att = read_json(source, "attestation.json")
        if isinstance(att, dict):
            # MANDATORY, not conditional. The previous form compared two values
            # only when BOTH were truthy, so a missing side skipped the check
            # entirely — and a missing side is precisely the case the consumer
            # rejects: its scanner reads the session record unconditionally and
            # requires the identifiers to be present AND equal. A check that
            # excuses absence is not enforcing a binding, it is enforcing a
            # coincidence.
            start = read_json(source, "start.json")
            start_thread = _dig(start, ("threadId",)) if isinstance(start, dict) else None
            att_thread = _dig(att, ("start", "threadId"))
            review = source / "review.md"
            claimed = _dig(att, ("artifact", "sha256"))
            claimed_path = _dig(att, ("artifact", "path"))

            problems = []
            if not start_thread:
                problems.append("the session record carries no thread identifier")
            elif not att_thread:
                problems.append("the attestation names no thread")
            elif start_thread != att_thread:
                problems.append(
                    f"its thread {att_thread!r} is not the archived session's "
                    f"thread {start_thread!r}")

            if _dig(att, ("teardown",)) != "confirmed":
                problems.append(
                    f"teardown reads {_dig(att, ('teardown',))!r}, and only "
                    "'confirmed' is accepted")
            if _dig(att, ("turn", "status")) != "completed":
                problems.append(
                    f"turn status reads {_dig(att, ('turn', 'status'))!r}, and only "
                    "'completed' is accepted for an archived round")

            if not review.is_file():
                problems.append("the review it attests is absent or not a regular file")
            elif not claimed:
                problems.append("it records no artifact digest for that review")
            else:
                try:
                    actual = sha256_of(review)
                except OSError as exc:
                    problems.append(f"the review cannot be read to verify its digest: {exc}")
                else:
                    if actual != claimed:
                        problems.append(
                            "its artifact digest does not match the review it archives")
            if not claimed_path:
                problems.append("it records no artifact path")
            elif os.path.normpath(str(claimed_path)) != os.path.normpath(
                    str(review)):
                problems.append(
                    f"its artifact path {claimed_path!r} names something other than "
                    "this round's review")

            if problems:
                unusable["attestation.json"] = "; ".join(problems)

    if unusable:
        raise SystemExit(
            "refusing to archive a round whose sidecars cannot be used:\n"
            + "\n".join(f"  {n}: {why}" for n, why in sorted(unusable.items()))
            + "\nA file that exists but holds nothing usable is not an absent file. "
              "Repair or remove it, then archive again.")


#: Statuses a caller supplies for a round that did not complete. They cannot be
#: re-derived from the archived bytes, so a re-derivation never overwrites one.
_NON_DERIVED_STATUSES = ("failed", "timeout", "refused")

#: Fields a derivation genuinely CANNOT source, per kind — the only ones a
#: re-derivation leaves entirely alone. Everything else this script produces is
#: authoritative for a row it owns, including a deliberate null.
#:
#: The distinction is load-bearing and was got wrong once. A blanket "never
#: replace a value with null" also protected `reviewed_sha` on a COMMIT-REVIEW
#: row, where null is not an absence but a verdict: the collector withholds the
#: marker for a round that failed, and resurrecting the sha there would restate
#: a completion the collector declined to record. An architect row is the
#: opposite case — the attestation carries no such field at all, so this script
#: has nothing to say and must not speak.
#: A field whose derived value is a HARDCODED None belongs here by definition:
#: writing a constant null is not sourcing a value, it is having nothing to say.
#: `verdict` on a commit-review row is exactly that — the commit-review collector
#: records no verdict, so this script writes None and always has. Found the
#: expensive way: sweeping the thirteen real archives to check the refusal, the
#: run modified two of them, and issue 180's six commit-review rows each lost a
#: recorded verdict of "clean" or "findings". The key-set guard could not see it
#: (the key exists in both shapes) and the sourced-null rule actively preferred
#: the null. `test_every_field_derived_as_a_CONSTANT_null_is_listed_unsourceable`
#: derives this table's contents from the derivation instead of trusting it.
#: Returned when the index bytes moved but no ROW did — a canonicalisation. It
#: is truthy so the checksum manifest is refreshed, and distinguishable so the
#: message does not claim a row was re-derived when none was.
_INDEX_BYTES_ONLY = -1

UNSOURCEABLE_BY_KIND = {
    "architect-review": ("reviewed_sha",),
    "commit-review": ("verdict",),
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--issue", required=True)
    # Not required under --rederive-index: it names no single round. Checked
    # below rather than by argparse, so one flag does not have to fake three.
    parser.add_argument("--kind", choices=sorted(KINDS))
    parser.add_argument("--run-dir", type=Path)
    parser.add_argument("--logical-loop")
    parser.add_argument("--status", default=None,
                        help="override the derived status (e.g. failed, timeout)")
    parser.add_argument("--wave-sha", default=None)
    parser.add_argument("--name", default=None,
                        help="archive directory name (defaults to the run dir's name)")
    parser.add_argument("--prompts", type=Path, default=None,
                        help="directory holding the gate's prompt files, when it is "
                             "not the run directory (the dispatcher-owned seam keeps "
                             "them apart on purpose)")
    parser.add_argument("--repo", type=Path, default=None)
    parser.add_argument("--accept-new", action="store_true",
                        help="record files that arrived in the archive outside this "
                             "script (a live capture, typically). Without it an "
                             "unlisted file refuses, because absorbing one silently "
                             "is how unaccounted evidence becomes accounted evidence")
    parser.add_argument("--rederive-index", action="store_true",
                        help="recompute every index row from the archived bytes and "
                             "exit; use after changing how a row is derived, or the "
                             "archive keeps serving the old derivation")
    args = parser.parse_args()

    # The twin of the name check. This script's own comment already warned that
    # the issue number is accepted as given and the hazard is one typo away; it
    # is joined into the archive path by the same operator, so `--issue
    # '999/../../../..'` wrote eleven paths outside the repository at exit 0.
    if not str(args.issue).isdigit():
        print(f"--issue must be a number, not a path: {args.issue!r}", file=sys.stderr)
        return 2

    repo = args.repo or Path(
        subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True).strip()
    )
    root = repo / "docs" / "architecture" / "evidence" / f"issue-{args.issue}"
    if not root.is_dir():
        print(f"no evidence archive for issue {args.issue} at {root}", file=sys.stderr)
        return 1

    if args.rederive_index and args.accept_new:
        print("--accept-new has no meaning with --rederive-index: a re-derivation "
              "adds no file, so there is nothing to accept. Archive the round that "
              "brings the file, or remove it.", file=sys.stderr)
        return 2

    if args.rederive_index:
        # FIRST, before the index is touched. The previous order rewrote the
        # index and only then discovered external damage, which left the
        # operator unable to retry: having fixed the damage, the next run saw an
        # already-canonical index, decided there was nothing to do, and exited 0
        # with the manifest still holding the index's old digest.
        refuse_unless_archive_is_accounted_for(root, accept_new=args.accept_new)
        changed = rederive_index(root)
        # ONLY when a row actually moved. The checksum manifest covers archived
        # FILES, and re-deriving changes none of them — the sole reason to touch
        # it is that `index.jsonl` is itself listed and its digest moved. A
        # regeneration on an unchanged archive rewrites the manifest anyway (the
        # ordering is not stable), so pointing this at an archive it has nothing
        # to fix would move real bytes for no reason. That is the one path here
        # that writes outside the index, so it gets the same restraint.
        listed = write_sums(root) if changed else None
        rows = 0 if changed == _INDEX_BYTES_ONLY else changed
        print(f"re-derived {rows} index row(s) from the archived bytes"
              + ("; the index was rewritten in canonical form" if changed == _INDEX_BYTES_ONLY else "")
              + ("; " + f"SHA256SUMS lists {listed} files" if changed
                 else "; SHA256SUMS left untouched — nothing changed"))
        print("\nNEXT: git add docs/architecture/evidence — the scanners compare "
              "SHA256SUMS against the GIT INDEX.")
        return 0

    if not (args.kind and args.run_dir and args.logical_loop):
        print("--kind, --run-dir and --logical-loop are required unless "
              "--rederive-index is given", file=sys.stderr)
        return 2
    if not args.run_dir.is_dir():
        print(f"run directory not found: {args.run_dir}", file=sys.stderr)
        return 1

    subdir, wanted, _collector, indexed = KINDS[args.kind]

    # A SINGLE path component. Joined unchecked, `--name ../ESCAPED` wrote the
    # round outside its kind directory and `--name ../../ESCAPED2` outside the
    # issue archive altogether — both at exit 0, the second covered by no
    # manifest line at all while an index row still claimed it. A completion
    # claim citing that round would never be captured by a `git add` of the
    # issue's evidence directory, which is the one thing the archive exists to
    # make checkable. An absolute name wrote nine files outside the repository
    # before failing on an unrelated path computation.
    if args.name is not None and (
            args.name in ("", ".", "..")
            or os.sep in args.name
            or (os.altsep and os.altsep in args.name)
            or Path(args.name).is_absolute()):
        print(f"--name must be a single directory name, not a path: {args.name!r}",
              file=sys.stderr)
        return 2

    # BEFORE the destination exists. Raising after it does is what makes a
    # failure non-retryable: the partial round is then in the way, and the
    # overwrite refusal blocks the corrected run even once the operator has
    # repaired what the check complained about.
    accepted = []
    if args.accept_new:
        _listed, _disk = read_sums(root), scan_archive(root)
        accepted = sorted(set(_disk) - set(_listed))
        # A file that appeared INSIDE an already-archived round is not a capture
        # and must not be absorbed. The manifest is rewritten over every file
        # while the index only appends the new row, so absorbing one would list
        # it as evidence while the round's own row still denies it — and a later
        # re-derivation then refuses on that disagreement, with no path forward
        # that this flag can offer. Captures live outside round directories;
        # anything inside one belongs to its round.
        round_subdirs = {subdir for subdir, _f, _c, _i in KINDS.values()}
        inside_a_round = sorted(
            name for name in accepted
            if len(Path(name).parts) > 2 and Path(name).parts[0] in round_subdirs
        )
        if inside_a_round:
            print("refusing to accept files that appeared inside an already "
                  "archived round — these belong to a round, not to the archive, "
                  "and listing them would leave the round's own row denying "
                  f"them:\n  {inside_a_round}\nRemove them, or delete the round "
                  "directory and archive it again.", file=sys.stderr)
            return 1
    refuse_unless_archive_is_accounted_for(root, accept_new=args.accept_new)

    durable = root / subdir / (args.name or args.run_dir.name)
    if durable.exists():
        print(f"refusing to overwrite an archived round: {durable}", file=sys.stderr)
        return 1
    # BUILT BESIDE the destination and moved in as one step. Copying straight
    # into the destination meant any failure part way — an unreadable source, a
    # full disk — left a partial round sitting exactly where the next attempt
    # wants to go, and the overwrite refusal then blocked the retry forever.
    # Widening a rollback to cover more of the copy loop is the patch I already
    # tried; making the destination appear only when the round is COMPLETE is
    # the property, and it holds for failures nobody enumerated.
    # BEFORE anything is created. Appending a round row into a missing index
    # makes that ROW the archive header, where the repository's scanner requires
    # a schema version and a source tip and where this script's own
    # re-derivation explicitly refuses a row — so the command reported success
    # and produced an archive nothing could read. An archive skeleton is created
    # deliberately; inventing a source tip here would fabricate provenance.
    #
    # Placed here, not at the point of use: the first version of this check sat
    # beside the index read and therefore refused only after the round had
    # already been published, which is the same check-after-mutate shape two
    # earlier rounds were about. Writing it a third time is what moved it here.
    index_path = root / "index.jsonl"
    if indexed and not index_path.is_file():
        print(f"no index at {index_path} — an archive is opened with its skeleton "
              "(index header plus manifest), not bootstrapped by its first round. "
              "Create the skeleton, then archive into it.", file=sys.stderr)
        return 1

    refuse_unusable_json(args.run_dir, args.kind)

    kind_dir_existed = durable.parent.is_dir()
    durable.parent.mkdir(parents=True, exist_ok=True)
    staging = durable.parent / f".partial-{durable.name}-{os.getpid()}"

    copied = []
    try:
        # Inside the guard from the FIRST mutation, not from the first copy.
        # Creating the staging directory is itself a write and can itself fail —
        # a read-only kind directory is the case that proved it — and sitting one
        # line above the guard it escaped as a traceback. Every attempt to fence
        # this operation has drawn the fence one step too late; this draws it at
        # the first thing that touches the disk.
        if staging.exists():
            shutil.rmtree(staging)
        staging.mkdir()
        for name in wanted:
            source = args.run_dir / name
            if source.is_file():
                shutil.copy2(source, staging / name)
                copied.append(name)
        if args.kind == "architect-review":
            # The prompts are REQUIRED evidence for a gate round — the archive
            # scanner hashes every file under `prompts/` and refuses a round
            # without it. They may not live in the run directory: the
            # dispatcher-owned seam deliberately keeps them in a separate share
            # directory, because the run directory holds `start.json`, the
            # collector's root of trust, and the helper that drives the turn is
            # a process that can write files. So the location is a parameter,
            # defaulting to the run directory.
            source = args.prompts or (args.run_dir / "prompts")
            if source.is_dir():
                shutil.copytree(source, staging / "prompts")
                copied.append("prompts/")
            else:
                for name in ("prompt", "retry"):
                    candidate = (args.prompts or args.run_dir) / name
                    if candidate.is_file():
                        (staging / "prompts").mkdir(exist_ok=True)
                        shutil.copy2(candidate, staging / "prompts" / name)
                        copied.append("prompts/" + name)
    except BaseException as failure:
        # VERIFIED, not asserted — the sibling of the publication handler below,
        # and missed when that one was corrected. A copy failure whose cleanup
        # also fails leaves a staging directory that blocks the next preflight,
        # so claiming nothing was written sends the operator the wrong way.
        gone = _remove_confirmed(staging)
        print(f"archiving failed while copying ({type(failure).__name__}: {failure}).",
              file=sys.stderr)
        if gone:
            print("  Nothing was written to the archive; fix the cause and run the "
                  "same command again.", file=sys.stderr)
        else:
            print(f"  THE ARCHIVE IS IN AN UNKNOWN STATE — {staging} could not be "
                  "confirmed removed and will be refused as unaccounted by the next "
                  "run. Remove it before running anything else against this archive.",
                  file=sys.stderr)
        return 1

    if not copied:
        gone = _remove_confirmed(staging)
        print(f"nothing to archive in {args.run_dir} — is it the right run directory?",
              file=sys.stderr)
        if not gone:
            print(f"  and {staging} could not be confirmed removed — remove it "
                  "before the next run, which will refuse it as unaccounted.",
                  file=sys.stderr)
        return 1

    # The destination comes into existence here, complete, in one step — and the
    # publication itself is guarded, because a rename can fail too (an I/O error,
    # or the destination appearing between the check and the move). Left
    # unguarded it stranded a populated staging directory inside the archive,
    # which the next run then refuses as unaccounted: exactly the retry-blocked
    # state building beside the destination was meant to remove.
    try:
        os.rename(staging, durable)
    except BaseException as failure:
        # VERIFY the cleanup rather than assert it. Suppressing the removal's own
        # errors and then reporting that nothing was left is the same shape as
        # the rollback that once claimed the archive was restored when it could
        # not restore it — and its sibling below already knows better. A
        # surviving staging directory blocks the next pre-flight as unaccounted,
        # so an operator told "nothing was left" would be looking anywhere but
        # at the thing in their way.
        shutil.rmtree(staging, ignore_errors=True)
        print(f"archiving failed while publishing the round ({type(failure).__name__}: "
              f"{failure}).", file=sys.stderr)
        if _confirmed_removed(staging):
            print("  Nothing was left in the archive; fix the cause and run the "
                  "same command again.", file=sys.stderr)
        else:
            print(f"  THE ARCHIVE IS IN AN UNKNOWN STATE — {staging} could not be "
                  "confirmed removed and will be refused as unaccounted by the "
                  "next run. Remove it before running anything else against this "
                  "archive.", file=sys.stderr)
        return 1

    # A gate round's PROMPT is required evidence, and it must be the prompt the
    # collector attested — not merely some file in a directory named prompts.
    #
    # Without this the command exited 0 for a missing, mistyped or simply wrong
    # `--prompts`: the collector sidecars alone make the copy non-empty, so an
    # unusable archive was recorded as a success. The repository's scanner does
    # catch it, but only later, by which time the source run directory may be
    # gone and the prompt unrecoverable. Refusing here, before anything is
    # recorded, is the difference between a fixable mistake and a lost round.
    if args.kind == "architect-review":
        # The digest lives at `prompt.actualSha256` in the attestation FILE. The
        # flat `promptSha256` this first read is the one `collect` prints to
        # STDOUT — a different surface — so reading it here made the guard inert
        # on every real round while a fixture that invented the flat field kept
        # the tests green. A fixture is not evidence; the shape is now asserted
        # against a real archived attestation by a test, so it cannot drift back.
        attestation = read_json(durable, "attestation.json")
        # An attestation that exists but cannot be PARSED is not an unattested
        # round — it is an attested round nobody can read. Treating the two
        # alike archived a malformed one at exit 0 with a null gate and status,
        # which the downstream scanner then refuses: success reported for output
        # its consumer rejects, with the source run possibly already discarded.
        if isinstance(attestation, Unreadable):
            if not _remove_confirmed(durable):
                print(f"  warning: {durable} could not be confirmed removed.",
                      file=sys.stderr)
            print("refusing to archive a gate round whose attestation cannot be "
                  f"read ({attestation.reason}). An unparseable attestation is "
                  "not an absent one; repair or remove it, then archive again.",
                  file=sys.stderr)
            return 1
        # An attestation that is PRESENT must be fully readable, or this round is
        # not archived. The previous version treated an unreadable digest exactly
        # like an absent attestation, so renaming the prompt block, dropping
        # `actualSha256`, or setting it null each archived the WRONG prompt at
        # exit 0 — the same fail-open the flat-key read produced, one layer in.
        # A round whose attestation cannot be resolved is not an unattested
        # round; it is an attested round nobody can place, which is worse.
        if attestation is not None and not read_json(durable, "refusal.json") \
                and args.status in (None, "completed"):
            # A non-empty STRING, not merely "resolved". Every one of these
            # facts is textual, and a corrupted or drifted attestation carrying
            # a list or a number would otherwise pass this check and then raise
            # deeper in. The two differ, and the first version of this comment
            # got it wrong by lumping them: a list or dict is unhashable, so it
            # raises IN the membership test, BEFORE `_discard` — leaving debris
            # that blocks the operator's corrected retry. An integer is hashable,
            # so the membership test succeeds and `_discard` does run; it then
            # raises in the refusal message, where it is not subscriptable. Only
            # the first leaves debris. Both are refused here instead.
            # `verdict` is EXEMPT from this resolution check, and only it. The
            # installed collector writes `parsedVerdict` null for an architect
            # gate — it parses one only for a `review` gate — so demanding a
            # string here would refuse every round the current runtime produces.
            # It is derived from the artifact instead, over bytes the attestation
            # binds by sha256; if that derivation also finds none, the round is
            # archived without a verdict rather than with an invented one.
            unresolved = sorted(
                name for name, path in ARCHITECT_ROW_PATHS.items()
                if name != "verdict"
                and (not isinstance(_dig(attestation, path), str)
                     or not _dig(attestation, path).strip())
            )
            # THE VERDICT MUST COME FROM SOMEWHERE — the attestation when the
            # collector sourced one, otherwise the artifact it binds. Only the
            # SOURCE is relaxed, never the requirement: exempting the field
            # outright would let a round with no verdict anywhere archive
            # silently, which is a weaker contract than the one it replaced.
            if not architect_verdict_from_artifact(durable, attestation or {}):
                unresolved = sorted(unresolved + ["verdict"])
            if unresolved:
                if not _remove_confirmed(durable):
                    print(f"  warning: {durable} could not be confirmed removed; the next run will refuse it.", file=sys.stderr)
                print("refusing to archive a gate round whose attestation does not "
                      f"resolve {unresolved} — the collector's schema and this "
                      "script disagree, so the archive would record nulls for facts "
                      "the attestation actually carries", file=sys.stderr)
                return 1
        attested = _dig(attestation, ARCHITECT_ROW_PATHS["prompt_sha256"])
        # REGULAR FILES only. A `--prompts` path holding just subdirectories —
        # the parent supplied one level too high — otherwise made the collection
        # non-empty while hashing nothing, and git does not carry empty
        # directories, so the durable evidence would not even survive a clone.
        archived = sorted(
            f for f in (durable / "prompts").glob("*")
            if (durable / "prompts").is_dir() and f.is_file()
        ) if (durable / "prompts").is_dir() else []
        digests = {sha256_of(f) for f in archived}
        if not archived:
            if not _remove_confirmed(durable):
                print(f"  warning: {durable} could not be confirmed removed; the next run will refuse it.", file=sys.stderr)
            print("refusing to archive a gate round with no prompt: pass --prompts "
                  "<dir> naming the directory that holds it", file=sys.stderr)
            return 1
        if attested and attested not in digests:
            if not _remove_confirmed(durable):
                print(f"  warning: {durable} could not be confirmed removed; the next run will refuse it.", file=sys.stderr)
            print("refusing to archive a gate round whose prompt is not the attested "
                  f"one (attestation's prompt.actualSha256 is {attested[:16]}…, "
                  f"archived prompts hash to "
                  f"{sorted(d[:16] for d in digests)}) — usually the wrong "
                  "--prompts directory, but check the digest's own spelling too: "
                  "this compares raw text, so uppercase hex or surrounding "
                  "whitespace fails here with the RIGHT directory",
                  file=sys.stderr)
            return 1

    row = derive_row(args.kind, args.run_dir, durable, root,
                     args.logical_loop, args.status, args.wave_sha)
    # From here every write is ROLLED BACK on failure. Without this the sequence
    # — create the directory, copy into it, append the row, rewrite the manifest
    # — left an archive that was both inconsistent AND unretryable when any step
    # failed: a read-only manifest, a full disk, an unreadable source file each
    # produced a half-archived round, and because the destination then existed,
    # the overwrite refusal blocked the identical command after the operator had
    # fixed exactly what the error named. Restoring the previous state is what
    # makes "fix what it says and run it again" true rather than aspirational.
    # DEFENSIVE, and inside the protected region. A brand-new issue archive has
    # no index yet, and reading it unconditionally — outside the try, after the
    # round had already been moved into place — turned bootstrapping a new
    # archive from a working operation into a crash that left the round orphaned
    # and the retry blocked. Measured against the previous commit: it exited 0
    # there and 1 here. A fix that breaks the first use of the thing it protects
    # is not a fix, and this one was mine.
    try:
        index_before = index_path.read_bytes() if index_path.is_file() else None
        if indexed:
            with (root / "index.jsonl").open("a") as handle:
                handle.write(json.dumps(row, sort_keys=True) + "\n")
        else:
            # Recorded BESIDE the round instead of in the shared index, so the
            # evidence exists and is checksummed without claiming an attestation
            # the scanner would then look for and not find.
            (durable / "round.json").write_text(
                json.dumps(row, sort_keys=True, indent=1) + "\n")

        listed = write_sums(root)
    except BaseException as failure:
        # The rollback is itself a mutation and can itself fail — a read-only
        # index is the case that proved it, where restoring needed exactly the
        # permission that was missing and the restore raised out of the handler.
        # So restore only what actually changed, and if even that cannot be done,
        # say the archive is in an UNKNOWN state rather than claiming it is as it
        # was. A rollback that lies is worse than no rollback.
        undone, unresolved = [], []
        try:
            if index_before is None:
                if index_path.is_file():
                    index_path.unlink()   # it did not exist before this run
                    undone.append("the index file this run created")
            elif index_path.read_bytes() != index_before:
                index_path.write_bytes(index_before)
                undone.append("the index row")
        except BaseException as restore_failure:
            unresolved.append(f"index.jsonl ({restore_failure})")
        try:
            if not _confirmed_removed(durable):
                _discard(durable, parent_existed=kind_dir_existed)
                # ...and CONFIRM it, rather than assuming the removal worked.
                # Sibling of the publication path above, swept for the same
                # reason: an existence test is not a verification under the
                # faults a failure handler exists to report.
                if not _confirmed_removed(durable):
                    raise OSError(f"{durable} could not be confirmed removed")
                undone.append("the round directory")
        except BaseException as restore_failure:
            unresolved.append(f"{durable} ({restore_failure})")

        print(f"archiving failed ({type(failure).__name__}: {failure}).",
              file=sys.stderr)
        if unresolved:
            print("  THE ARCHIVE IS IN AN UNKNOWN STATE — rollback could not "
                  f"complete: {unresolved}. Inspect it before running anything "
                  "else against it.", file=sys.stderr)
            return 1
        print(f"  rolled back {undone or ['nothing — no write had happened yet']}. "
              "The archive is as it was; fix the cause and run the same command "
              "again.", file=sys.stderr)
        return 1
    print(f"archived {len(copied)} file(s) to {durable.relative_to(repo)}")
    for name in accepted:
        print(f"  ACCEPTED into the manifest, not produced by this round: {name}")
    print(f"  status={row['status']} reviewed_sha={row.get('reviewed_sha')}")
    print("  {0}; SHA256SUMS lists {1} files".format(
        "index row appended" if indexed else "recorded in round.json, NOT indexed "
        "(a wave-gate run has no collector attestation to index)", listed))
    print(f"\nNEXT: git add {(root).relative_to(repo)} — the scanners compare "
          f"SHA256SUMS against the GIT INDEX, so an unstaged file fails as "
          f"listed-but-untracked.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
