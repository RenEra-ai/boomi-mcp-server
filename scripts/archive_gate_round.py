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


def _discard(durable: Path) -> None:
    """Remove a refused round, and its parent if that leaves it empty.

    An empty `architect-reviews/` left behind reads as "a round was archived
    here" to anyone looking, which is the impression a refusal exists to avoid.
    """
    shutil.rmtree(durable, ignore_errors=True)
    parent = durable.parent
    if parent.is_dir() and not any(parent.iterdir()):
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


def read_json(directory: Path, name: str):
    path = directory / name
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text())
    except (ValueError, OSError):
        return None


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

#: `gate-attest` records no reviewed SHA under any spelling — checked against a
#: real attestation, whose top-level keys are schema, gateProtocol, gate,
#: collectedAt, start, prompt, turn, artifact, inputPlan, parsedVerdict and
#: teardown. The row therefore carries null DELIBERATELY and says so, rather
#: than probing invented keys and letting the miss read as an absent value.
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
        refusal = read_json(durable, "refusal.json")
        row.update({name: _dig(attestation, path)
                    for name, path in ARCHITECT_ROW_PATHS.items()})
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


def regenerate_sums(root: Path) -> int:
    """Rewrite SHA256SUMS over every archived file.

    Any file NAMED SHA256SUMS is excluded, not just the top-level one: a nested
    checksum file copied in from a capture would otherwise be listed by the
    parent while the archive test's own scan skips it, and the two disagree.
    """
    lines = []
    for path in sorted(root.rglob("*")):
        if path.is_file() and path.name != "SHA256SUMS":
            lines.append(f"{sha256_of(path)}  {path.relative_to(root)}")
    (root / "SHA256SUMS").write_text("\n".join(lines) + "\n")
    return len(lines)


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
    lines = index.read_text().splitlines()
    header, rows = lines[0], [json.loads(l) for l in lines[1:] if l.strip()]

    by_collector = {c: k for k, (_s, _f, c, indexed) in KINDS.items() if indexed}
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
        if fresh != row:
            changed += 1
        out.append(fresh)

    with index.open("w") as handle:
        handle.write(header + "\n")
        for row in out:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
    return changed


#: Statuses a caller supplies for a round that did not complete. They cannot be
#: re-derived from the archived bytes, so a re-derivation never overwrites one.
_NON_DERIVED_STATUSES = ("failed", "timeout", "refused")


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
    parser.add_argument("--rederive-index", action="store_true",
                        help="recompute every index row from the archived bytes and "
                             "exit; use after changing how a row is derived, or the "
                             "archive keeps serving the old derivation")
    args = parser.parse_args()

    repo = args.repo or Path(
        subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True).strip()
    )
    root = repo / "docs" / "architecture" / "evidence" / f"issue-{args.issue}"
    if not root.is_dir():
        print(f"no evidence archive for issue {args.issue} at {root}", file=sys.stderr)
        return 1

    if args.rederive_index:
        changed = rederive_index(root)
        listed = regenerate_sums(root)
        print(f"re-derived {changed} index row(s) from the archived bytes; "
              f"SHA256SUMS lists {listed} files")
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

    durable = root / subdir / (args.name or args.run_dir.name)
    if durable.exists():
        print(f"refusing to overwrite an archived round: {durable}", file=sys.stderr)
        return 1
    durable.mkdir(parents=True)

    copied = []
    for name in wanted:
        source = args.run_dir / name
        if source.is_file():
            shutil.copy2(source, durable / name)
            copied.append(name)
    if args.kind == "architect-review":
        # The prompts are REQUIRED evidence for a gate round — the archive
        # scanner hashes every file under `prompts/` and refuses a round without
        # it. They may not live in the run directory: the dispatcher-owned seam
        # deliberately keeps them in a separate share directory, because the run
        # directory holds `start.json`, the collector's root of trust, and the
        # helper that drives the turn is a process that can write files. So the
        # location is a parameter, defaulting to the run directory.
        source = args.prompts or (args.run_dir / "prompts")
        if source.is_dir():
            shutil.copytree(source, durable / "prompts")
            copied.append("prompts/")
        else:
            for name in ("prompt", "retry"):
                candidate = (args.prompts or args.run_dir) / name
                if candidate.is_file():
                    (durable / "prompts").mkdir(exist_ok=True)
                    shutil.copy2(candidate, durable / "prompts" / name)
                    copied.append("prompts/" + name)

    if not copied:
        shutil.rmtree(durable)
        print(f"nothing to archive in {args.run_dir} — is it the right run directory?",
              file=sys.stderr)
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
        # An attestation that is PRESENT must be fully readable, or this round is
        # not archived. The previous version treated an unreadable digest exactly
        # like an absent attestation, so renaming the prompt block, dropping
        # `actualSha256`, or setting it null each archived the WRONG prompt at
        # exit 0 — the same fail-open the flat-key read produced, one layer in.
        # A round whose attestation cannot be resolved is not an unattested
        # round; it is an attested round nobody can place, which is worse.
        if attestation is not None and not read_json(durable, "refusal.json") \
                and args.status in (None, "completed"):
            unresolved = sorted(
                name for name, path in ARCHITECT_ROW_PATHS.items()
                if _dig(attestation, path) in (None, "")
            )
            if unresolved:
                _discard(durable)
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
            _discard(durable)
            print("refusing to archive a gate round with no prompt: pass --prompts "
                  "<dir> naming the directory that holds it", file=sys.stderr)
            return 1
        if attested and attested not in digests:
            _discard(durable)
            print("refusing to archive a gate round whose prompt is not the attested "
                  f"one (attestation's prompt.actualSha256 is {attested[:16]}…, "
                  f"archived prompts hash to "
                  f"{sorted(d[:16] for d in digests)}) — this is the wrong prompt "
                  "directory", file=sys.stderr)
            return 1

    row = derive_row(args.kind, args.run_dir, durable, root,
                     args.logical_loop, args.status, args.wave_sha)
    if indexed:
        with (root / "index.jsonl").open("a") as handle:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
    else:
        # Recorded BESIDE the round instead of in the shared index, so the
        # evidence exists and is checksummed without claiming an attestation
        # the scanner would then look for and not find.
        (durable / "round.json").write_text(json.dumps(row, sort_keys=True, indent=1) + "\n")

    listed = regenerate_sums(root)
    print(f"archived {len(copied)} file(s) to {durable.relative_to(repo)}")
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
