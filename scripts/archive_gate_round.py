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
        attestation = read_json(durable, "attestation.json") or {}
        refusal = read_json(durable, "refusal.json")
        row.update({
            "gate": attestation.get("gate"),
            "prompt_sha256": attestation.get("promptSha256"),
            "reviewed_sha": attestation.get("reviewedSha") or attestation.get("head"),
            "status": status_override or ("refused" if refusal else
                                          (attestation.get("status") or "completed")),
            "verdict": attestation.get("parsedVerdict") or attestation.get("verdict"),
        })
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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--issue", required=True)
    parser.add_argument("--kind", required=True, choices=sorted(KINDS))
    parser.add_argument("--run-dir", required=True, type=Path)
    parser.add_argument("--logical-loop", required=True)
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
    args = parser.parse_args()

    repo = args.repo or Path(
        subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True).strip()
    )
    if not args.run_dir.is_dir():
        print(f"run directory not found: {args.run_dir}", file=sys.stderr)
        return 1

    subdir, wanted, _collector, indexed = KINDS[args.kind]
    root = repo / "docs" / "architecture" / "evidence" / f"issue-{args.issue}"
    if not root.is_dir():
        print(f"no evidence archive for issue {args.issue} at {root}", file=sys.stderr)
        return 1

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
        attested = (read_json(durable, "attestation.json") or {}).get("promptSha256")
        archived = sorted((durable / "prompts").glob("*")) if (durable / "prompts").is_dir() else []
        digests = {sha256_of(f) for f in archived if f.is_file()}
        if not archived:
            _discard(durable)
            print("refusing to archive a gate round with no prompt: pass --prompts "
                  "<dir> naming the directory that holds it", file=sys.stderr)
            return 1
        if attested and attested not in digests:
            _discard(durable)
            print("refusing to archive a gate round whose prompt is not the attested "
                  f"one (attestation says {attested[:16]}…, archived prompts hash to "
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
