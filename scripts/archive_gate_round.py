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

KINDS = {
    "commit-review": ("commit-reviews", COMMIT_REVIEW_FILES, "commit-review-collect"),
    "architect-review": ("architect-reviews", ARCHITECT_FILES, "gate-attest"),
    "wave-gate": ("wave-gate", WAVE_GATE_FILES, "wave_gate"),
}


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
    subdir, _files, collector = KINDS[kind]
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
    parser.add_argument("--repo", type=Path, default=None)
    args = parser.parse_args()

    repo = args.repo or Path(
        subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True).strip()
    )
    if not args.run_dir.is_dir():
        print(f"run directory not found: {args.run_dir}", file=sys.stderr)
        return 1

    subdir, wanted, _collector = KINDS[args.kind]
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
    if args.kind == "architect-review" and (args.run_dir / "prompts").is_dir():
        shutil.copytree(args.run_dir / "prompts", durable / "prompts")
        copied.append("prompts/")

    if not copied:
        shutil.rmtree(durable)
        print(f"nothing to archive in {args.run_dir} — is it the right run directory?",
              file=sys.stderr)
        return 1

    row = derive_row(args.kind, args.run_dir, durable, root,
                     args.logical_loop, args.status, args.wave_sha)
    index = root / "index.jsonl"
    with index.open("a") as handle:
        handle.write(json.dumps(row, sort_keys=True) + "\n")

    listed = regenerate_sums(root)
    print(f"archived {len(copied)} file(s) to {durable.relative_to(repo)}")
    print(f"  status={row['status']} reviewed_sha={row.get('reviewed_sha')}")
    print(f"  index row appended; SHA256SUMS lists {listed} files")
    print(f"\nNEXT: git add {(root).relative_to(repo)} — the scanners compare "
          f"SHA256SUMS against the GIT INDEX, so an unstaged file fails as "
          f"listed-but-untracked.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
