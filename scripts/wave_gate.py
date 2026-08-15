#!/usr/bin/env python3
"""The M12 endgame verification gate — one fail-closed command.

WHY THIS EXISTS
---------------
This repository's test suite only runs when a human remembers the exact spell
(``PYTHONPATH=src`` with a Python that can import FastMCP).  A CI job that
"runs pytest" with the wrong interpreter or the wrong path collects a fraction
of the suite — or zero tests — and goes green.  Every check below therefore
**asserts a floor** against a committed expectation.  Nothing here aggregates
whatever it happened to find and calls that the answer: aggregate-a-set fails
open, assert-a-floor fails closed.

WHAT IT CHECKS
--------------
``ci``   — the required status check for every push to ``dev`` and every PR
           targeting ``dev``.  Baseline + manifest transition, then collection
           (floor, required nodes, reconciliation), then the full non-KB suite.
``wave`` — everything ``ci`` does, plus the per-wave obligations: every ACTIVE
           golden-manifest entry rendered TWICE in isolated child processes and
           compared byte-for-byte (determinism), then compared against its
           committed bytes, plus the relocatable plan-fingerprint seam (#153).

``manifests`` runs ONLY the baseline/manifest/transition checks.  It is a fast
local pre-check and is **not a gate**; it prints a banner saying so, and the
workflow invokes ``ci``.

DESIGN RULES THAT MUST NOT BE RELAXED
-------------------------------------
* Standard library only.  The gate must run before project imports are proven.
* The gate NEVER mutates the repository.  Every temporary file goes under
  ``tempfile.mkdtemp()``; child processes run with ``PYTHONDONTWRITEBYTECODE``;
  the worktree's ``git status`` is captured before and after and must match.
* The gate NEVER updates a manifest.  There is no ``--update``, no ``--skip``,
  no ``--minimum`` override, no alternate manifest path.  Manifests are
  committed artifacts edited deliberately by the slice that changes the corpus.
* A missing, malformed, all-zero or unavailable baseline FAILS CLOSED.  It is
  never silently replaced by a merge-base, ``HEAD^``, or a bootstrap.

EXIT CODES
----------
0  every requested check passed
1  an executed validation failed (pytest, floor, required node, golden bytes,
   nondeterminism, fingerprint, worktree hygiene)
2  a contract failure that prevented validation from being meaningful (usage,
   event/baseline resolution, bootstrap eligibility, manifest format, manifest
   transition)

Every failure prints one stable diagnostic code as the first stderr token so
CI logs and the negative-test matrix can key on it rather than on prose.
"""

from __future__ import annotations

import argparse
import errno
import base64
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
from pathlib import PurePosixPath

# --------------------------------------------------------------------------
# Constants — the committed contract
# --------------------------------------------------------------------------

NODES_MANIFEST = "tests/fixtures/wave_gate/test_nodes.jsonl"
GOLDENS_MANIFEST = "tests/fixtures/wave_gate/goldens.jsonl"
GOLDEN_DIR = "tests/fixtures/golden_xml"
GOLDEN_CORPUS = "tests/_wave_gate_golden_corpus.py"

#: The pytest invocation the whole programme is pinned to.  It appears here
#: once; CI and the local recipe both reach it through this script so the two
#: cannot drift.
PYTEST_TARGET = "tests"
PYTEST_IGNORE = "tests/kb"

RENDERERS = ("process-component-v1", "process-xml-v1", "component-xml-v1")
DISPOSITIONS = ("survivor", "transitional_oracle", "deletion_only")
STATES = ("active", "tombstone")

_SHA_RE = re.compile(r"\A[0-9a-f]{40}\Z")
_ZERO_SHA = "0" * 40
_OWNER_RE = re.compile(r"\A(repository|#[1-9][0-9]*)\Z")
#: A collected node id: a whitespace-free path under ``tests/`` ending ``.py``,
#: then ``::``, then the test name.
#:
#: The NAME half may contain spaces — a parametrized id is whatever ``repr`` of
#: the parameter produced, and this suite really has 177 of them
#: (``...[validation_query-validationQuery-SELECT 1-SELECT 1]``, and one that is
#: literally ``[ ]``). Requiring the whole line to be whitespace-free dropped
#: every one of them, which the summary-reconciliation check then caught as
#: 9,485 parsed vs 9,662 reported.
#:
#: Still anchored on ``.py::`` rather than a bare ``startswith("tests/")``:
#: pytest's warning summary also emits lines beginning ``tests/``, but in the
#: ``path.py:LINENO:`` form, which has no ``::`` after ``.py``.
_NODE_LINE_RE = re.compile(r"\Atests/[^\s]+\.py::\S.*\Z")
_COLLECTED_RE = re.compile(r"\A(\d+) tests? collected\b")
_ID_RE = re.compile(r"\A(pytest|golden)-(\d{6})\Z")

#: Manifest schemas.  ``header``/``row`` are the EXACT key tuples in the EXACT
#: order every line must use.  Field order is part of the format so a diff of
#: the file is legible and a reordering is caught rather than normalised away.
_SCHEMAS = {
    "pytest-nodes": {
        "path": NODES_MANIFEST,
        "id_prefix": "pytest",
        "row_kind": "test",
        "header": ("kind", "schema_version", "manifest", "minimum_active",
                   "minimum_collected", "maximum_skipped", "bootstrap_base"),
        "row": ("kind", "id", "node_id", "state"),
        #: Fields that describe the row's subject.  Immutable forever: a row is
        #: never repointed, only appended-after or tombstoned.
        "payload": ("node_id",),
        #: The subset of `payload` that must additionally be UNIQUE across rows.
        #: Not the same set: `renderer`, `owner` and `disposition` are immutable
        #: per row and shared by many rows.
        "unique": ("node_id",),
    },
    "goldens": {
        "path": GOLDENS_MANIFEST,
        "id_prefix": "golden",
        "row_kind": "golden",
        "header": ("kind", "schema_version", "manifest", "minimum_active",
                   "bootstrap_base"),
        "row": ("kind", "id", "input_case", "renderer", "expected_file",
                "owner", "disposition", "state"),
        "payload": ("input_case", "renderer", "expected_file", "owner",
                    "disposition"),
        "unique": ("input_case", "expected_file"),
    },
}

SCHEMA_VERSION = 1

#: Codes the gate EMITS without raising them as failures. The stderr contract is
#: "first token is a documented code", and the last-resort diagnostic fallback
#: must honour it too — a failure nobody can classify is a failure nobody acts
#: on. Declared here so the code/doc agreement test can account for it.
# Codes the doc-coverage check cannot find by scanning for a literal at a
# `_contract(...)`/`_invalid(...)` call site: either the gate emits them without
# raising, or it raises them through a variable. They are part of the same stderr
# contract, so they are declared here rather than left to a regex that would
# quietly stop covering them.
EMIT_ONLY_CODES = (
    "GATE_DIAGNOSTIC_UNRENDERABLE",     # emitted when the diagnostic cannot render
    "SCRATCH_FOREIGN_ENTRIES",          # raised via `_ScratchDir.refusal_code`
)


# --------------------------------------------------------------------------
# Failure plumbing
# --------------------------------------------------------------------------

class GateFailure(Exception):
    """A gate failure carrying its stable diagnostic code and exit status."""

    def __init__(self, code, message, status):
        super().__init__(message)
        self.code = code
        self.message = message
        # A failure can NEVER carry a success status. Guarding each raise site
        # individually was tried and leaked: provider-controlled code can
        # construct `GateFailure(..., 0)` — via a `__len__` on a tuple subclass,
        # say — and it reached `main()`, which returned 0. Reproduced end to end
        # with `main_status=0`. Enforcing the invariant in the constructor closes
        # every present and future path at once.
        # `type(status) is int`, and then a LITERAL is stored. An int subclass
        # whose `__eq__` reports equality with 1 while its underlying value is 0
        # passes an `in (1, 2)` test and is then read as 0 by `sys.exit()` — the
        # failure would terminate successfully. Copying to a literal makes the
        # stored value independent of whatever was passed in.
        self.status = 2 if (type(status) is int and status == 2) else 1


def _contract(code, message):
    """A failure that prevented validation from being meaningful (exit 2)."""
    return GateFailure(code, message, 2)


def _invalid(code, message):
    """A failure of an executed validation (exit 1)."""
    return GateFailure(code, message, 1)


# --------------------------------------------------------------------------
# git helpers
# --------------------------------------------------------------------------

def _c_locale_env():
    """Environment for any git call whose STDERR the gate INTERPRETS.

    `_refuse_unreadable` matches English phrases like `could not open` and
    `Permission denied`; under a non-English `LC_MESSAGES` git and libc localise
    both, the command still exits 0 with the subtree omitted, and the match would
    silently miss it. Any place that reads git's prose must pin the language it
    is reading.
    """
    env = dict(os.environ)
    env["LC_ALL"] = "C"
    env["LANG"] = "C"
    env["LC_MESSAGES"] = "C"
    return env


def _git(repo, *args, check=True):
    proc = subprocess.run(
        ["git", *args], cwd=str(repo), capture_output=True, text=True,
    )
    if check and proc.returncode != 0:
        raise _contract(
            "BASELINE_UNAVAILABLE",
            "git {0} failed in {1}: {2}".format(
                " ".join(args), repo, proc.stderr.strip()
            ),
        )
    return proc


def _repo_root(explicit=None):
    start = explicit or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    proc = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        cwd=str(start), capture_output=True, text=True,
    )
    if proc.returncode != 0:
        raise _contract(
            "BASELINE_UNAVAILABLE",
            "not inside a git worktree: {0}".format(start),
        )
    return os.path.realpath(proc.stdout.strip())


def _resolve_commit(repo, rev, code="BASELINE_UNAVAILABLE"):
    proc = subprocess.run(
        ["git", "rev-parse", "--verify", "--quiet", "{0}^{{commit}}".format(rev)],
        cwd=str(repo), capture_output=True, text=True,
    )
    sha = proc.stdout.strip()
    if proc.returncode != 0 or not _SHA_RE.match(sha):
        raise _contract(code, "cannot resolve commit {0!r} in {1}".format(rev, repo))
    _refuse_ambiguous(repo, rev, code)
    return sha


def _refuse_ambiguous(repo, rev, code):
    """A name that resolves two ways is a refusal, not a coin flip.

    ``git rev-parse --verify`` silently applies ref precedence, so an ambiguous
    name resolves to whichever wins and the gate validates a range the operator
    did not mean.

    ASK GIT rather than enumerate namespaces. An earlier revision checked
    ``refs/heads/<x>``, ``refs/tags/<x>`` and ``refs/remotes/<x>`` — measured on a
    probe repo, that misses ``refs/<x>`` entirely (git resolved to it, the probe
    saw only the branch, and the mismatch went unreported), and it cannot see
    ambiguity inside a revision EXPRESSION like ``<x>~0`` at all. Git already
    knows: it emits ``warning: refname '<x>' is ambiguous.`` on stderr, for
    expressions too. Using its answer removes the enumeration that has now
    drifted out of step with its own contract three times.
    """
    if _SHA_RE.match(str(rev)):
        return  # a full sha cannot be ambiguous
    # NO `--quiet` here: it suppresses the very warning being read (measured).
    # The resolvability check elsewhere keeps its `--quiet`; this call exists
    # solely to hear what git says on stderr.
    #
    # `-c core.warnAmbiguousRefs=true` because a repository may have turned the
    # warning off, and `LC_ALL=C` because a localized git need not produce the
    # English phrase. Both make the probe independent of how the machine happens
    # to be configured rather than trusting a default.
    proc = subprocess.run(
        ["git", "-c", "core.warnAmbiguousRefs=true", "rev-parse", "--verify",
         "{0}^{{commit}}".format(rev)],
        cwd=str(repo), capture_output=True, text=True, env=_c_locale_env(),
    )
    if "is ambiguous" in proc.stderr:
        raise _contract(
            code,
            "{0!r} is ambiguous — git reports: {1}. Pass the 40-character sha you "
            "mean.".format(rev, proc.stderr.strip().splitlines()[0]),
        )


def _blob_at(repo, sha, path):
    """Return the file's bytes at ``sha``, or ``None`` if it did not exist."""
    proc = subprocess.run(
        ["git", "show", "{0}:{1}".format(sha, path)],
        cwd=str(repo), capture_output=True,
    )
    if proc.returncode != 0:
        return None
    return proc.stdout


def _path_touched_in_ancestry(repo, sha, path):
    # `--full-history` is load-bearing, not tidiness. Path-limited `git log`
    # SIMPLIFIES history by default, so an addition on a side branch that an
    # `ours` merge discarded is pruned from the walk — the path then looks
    # untouched in ancestry and the one-time bootstrap exception is granted for a
    # manifest that already existed.
    proc = _git(repo, "log", "--full-history", "--max-count=1", "--format=%H",
                sha, "--", path)
    return bool(proc.stdout.strip())



def _status(repo):
    """A change fingerprint for the worktree.

    ``git status --porcelain`` alone records only path + status LETTER, so
    editing a file that was already modified leaves it byte-identical. Pairing it
    with ``git diff --numstat`` (line counts per tracked path) and HEAD makes an
    in-place edit, a commit, and a checkout all visible. It is still not a full
    content hash — a same-size, same-line-count edit to an already-dirty file
    would slip through — but it closes the cases a gate realistically causes.
    """
    # `--untracked-files=all`, NUL-delimited, captured as BYTES.
    #
    # `normal` collapses an untracked DIRECTORY to a single `?? dir/` entry, so
    # rewriting a file inside it left both snapshots identical. And the `-z`
    # stream must not go through text mode: universal-newline translation
    # rewrites a `\r` inside a legal POSIX filename, and strict UTF-8 decoding
    # raises on a filename that is merely bytes — either way the gate would look
    # at the wrong path, or crash, on a file the user is entitled to have.
    raw_status = subprocess.run(
        ["git", "-c", "core.quotePath=false", "status", "--porcelain",
         "--untracked-files=all", "-z"],
        cwd=str(repo), capture_output=True, env=_c_locale_env(),
    )
    _refuse_unreadable(raw_status, "git status")
    status = raw_status.stdout
    parts = [
        _git(repo, "rev-parse", "HEAD").stdout.strip(),
        # surrogateescape keeps undecodable bytes distinguishable and round-trippable
        status.decode("utf-8", "surrogateescape"),
        # DIGESTS of the patches, never the patches themselves. The fingerprint
        # is printed verbatim in a WORKTREE_DIRTY diagnostic, and an uncommitted
        # `.env`-style edit would put a live credential straight into the log —
        # reproduced with `TOKEN=SUPER_SECRET_VALUE`. A digest compares exactly as
        # well and discloses nothing.
        #
        # `--binary --full-index`: the default diff ABBREVIATES binary blob ids,
        # so two different binary contents sharing a 7-character prefix produced
        # identical snapshots.
        "diff:" + _patch_digest(repo),
        "diff-cached:" + _patch_digest(repo, "--cached"),
    ]
    # Untracked files have no diff at all, so hash their contents directly —
    # otherwise rewriting an existing untracked file is invisible.
    for chunk in status.split(b"\0"):
        if not chunk.startswith(b"?? "):
            continue
        rel_bytes = chunk[3:]
        # `rel` is for the FINGERPRINT TEXT only; every filesystem call below
        # takes the BYTES. Decoding the joined path back to str would re-encode
        # it through the filesystem encoding — which is not necessarily UTF-8 —
        # and address a different file, or raise. os.path and open accept bytes
        # paths directly, so the bytes never need to round-trip at all.
        rel = os.fsdecode(rel_bytes)
        target = os.path.join(os.fsencode(repo), rel_bytes)
        try:
            if os.path.islink(target):
                parts.append("{0}:symlink:{1}".format(
                    rel, hashlib.sha256(os.readlink(target)).hexdigest()))
            elif os.path.isfile(target):
                with open(target, "rb") as handle:
                    parts.append("{0}:{1}".format(
                        rel, hashlib.sha256(handle.read()).hexdigest()))
            else:
                parts.append("{0}:<not-a-regular-file>".format(rel))
        except OSError as exc:
            # FAIL CLOSED. Recording a stable "<unreadable errno>" token made an
            # unreadable file's CONTENT invisible: chmod / write / chmod produced
            # identical snapshots either side of a real mutation. A file the gate
            # cannot account for means the gate cannot make its claim.
            raise _invalid(
                "WORKTREE_DIRTY",
                "cannot fingerprint {0} ({1}); the read-only guarantee cannot be "
                "asserted over a file that cannot be read".format(rel, exc.strerror),
            )
    return "\n".join(parts)


def _patch_digest(repo, *extra):
    """SHA-256 of the full binary patch — content-exact, disclosure-free."""
    proc = subprocess.run(
        ["git", "diff", "--binary", "--full-index", *extra],
        cwd=str(repo), capture_output=True, env=_c_locale_env(),
    )
    _refuse_unreadable(proc, "git diff")
    return hashlib.sha256(proc.stdout).hexdigest()


#: Substrings by which git reports that it could not READ something and has
#: therefore omitted it. Deliberately an enumeration, and deliberately narrow —
#: see `_refuse_unreadable`.
_ACCESS_FAILURE_SIGNALS = (
    "permission denied",
    "could not open",
    "cannot open",
    "unable to read",
    "cannot read",
    "operation not permitted",
    "no such file or directory",
)


def _refuse_unreadable(proc, what):
    """Refuse when git reports it could not read part of the tree.

    Exit code alone is not enough: with an unreadable DIRECTORY, `git status`
    and `git diff` both exit 0, WARN on stderr (`could not open directory …
    Permission denied`) and silently omit every file underneath — so a mutation
    in there produced an identical fingerprint and the per-file `open()` guard
    was never reached.

    An earlier revision refused on ANY stderr, on the reasoning that these
    commands emit nothing on a clean checkout so anything at all means an
    incomplete snapshot. That was wrong, and the asymmetry is why: git also emits
    genuinely benign process diagnostics in some environments (a sandboxed macOS
    can produce `warning: confstr() failed … using /tmp instead` alongside
    complete output and exit 0). Over-matching makes the REQUIRED gate refuse
    every invocation in such an environment — the whole check becomes unusable —
    while under-matching leaves only the narrow residual that existed before.
    A gate that cannot run protects nothing, so the narrow match wins.

    KNOWN INCOMPLETE, and it cannot be otherwise by this route. Git prints
    `lstat()` failures as arbitrary errno text (a symlink loop yields
    `<path>: Too many levels of symbolic links`, which no fixed phrase list
    anticipates), and its directory iterator can treat a `readdir()` error as
    end-of-directory with NO diagnostic at all — so there are omissions with
    nothing on stderr to match. This function therefore raises the cost of an
    undetected mutation; it does not reduce it to zero.

    That is acceptable because the read-only guarantee does NOT rest on it. The
    gate writes only under `tempfile.mkdtemp()`, runs its children with
    `PYTHONDONTWRITEBYTECODE=1` and `-p no:cacheprovider`, and never invokes a
    mutating git command — the property is structural. This check is a runtime
    cross-check of that structure, and it is described as exactly that wherever
    it is documented.
    """
    noise = proc.stderr.decode("utf-8", "replace").strip()
    lowered = noise.lower()
    unreadable = any(signal in lowered for signal in _ACCESS_FAILURE_SIGNALS)
    if proc.returncode != 0 or unreadable:
        raise _invalid(
            "WORKTREE_DIRTY",
            "{0} could not account for the whole worktree (exit {1}): {2}".format(
                what, proc.returncode, noise or "<no detail>"
            ),
        )


def check_checkout_matches_event(repo, context):
    """The tree under test must be the tree the event describes.

    Without this the gate reads its baseline from one place and its evidence from
    another: a PR carrying an illegal manifest rewrite can be described by the
    event while the checkout being validated is some other, valid state. GitHub's
    own actions keep them in step, so this never fires in a healthy run — which
    is exactly why it has to be asserted rather than assumed.

    A ``pull_request`` run legitimately checks out ``refs/pull/N/merge``, a merge
    commit whose parents are the head and the base, so both that merge and a bare
    head checkout are accepted.
    """
    if context["kind"] in ("push", "pull_request"):
        # Comparing only the HEAD COMMIT would accept a runner whose worktree had
        # been edited: `_status` snapshots that dirty state and then merely checks
        # it does not change, so the gate would validate bytes that are not in the
        # event's tree at all. CI checkouts are clean; local `--base` runs keep
        # their dirty-tree support because the operator chose the baseline.
        dirty = _git(repo, "status", "--porcelain", "--untracked-files=normal").stdout
        if dirty.strip():
            raise _contract(
                "CHECKOUT_EVENT_MISMATCH",
                "the worktree is not clean, so the tree under test is not the tree "
                "the {0} event describes:\n{1}".format(context["kind"], dirty),
            )

    head = _git(repo, "rev-parse", "HEAD").stdout.strip()
    if context["kind"] == "push":
        expected = context.get("after")
        if expected and head != expected:
            raise _contract(
                "CHECKOUT_EVENT_MISMATCH",
                "the checkout is at {0} but the push event describes {1}".format(
                    head[:12], expected[:12]
                ),
            )
        return
    if context["kind"] != "pull_request":
        return

    event_head = context.get("event_head")
    target = context.get("target")
    if head == event_head:
        return
    # GitHub names the test-merge commit it built; when it is present, require
    # HEAD to BE that commit. Parentage alone proves only the shape: a commit with
    # exactly {head, target} as parents but the TARGET's tree — omitting every
    # change the PR makes — satisfied it, so the gate would validate a tree the
    # PR does not produce.
    # HEAD is not the PR head, so this must be the merge checkout — and only an
    # AUTHORITATIVE merge sha can establish that. Falling back to a parentage
    # check when the event carries none reopens exactly what this closes: a
    # commit with parents {head, target} and an arbitrary tree satisfies the
    # shape while containing none of the PR's changes.
    # `GITHUB_SHA` FIRST. For a `pull_request` event Actions sets it to the merge
    # commit it checked out, and it is always present — whereas
    # `pull_request.merge_commit_sha` is NULLABLE (GitHub computes mergeability
    # asynchronously and sends null until it settles), so requiring the payload
    # field would reject legitimate PR runs before a single test executed.
    env_sha = os.environ.get("GITHUB_SHA", "")
    merge_sha = env_sha if _SHA_RE.match(env_sha) else context.get("merge_sha")
    if not merge_sha:
        raise _contract(
            "CHECKOUT_EVENT_MISMATCH",
            "the checkout at {0} is not the PR head {1}, and neither GITHUB_SHA "
            "nor merge_commit_sha identifies the merge that was built; the tree "
            "under test cannot be tied to the event".format(
                head[:12], (event_head or "?")[:12]
            ),
        )
    if head == merge_sha:
        return
    raise _contract(
        "CHECKOUT_EVENT_MISMATCH",
        "the checkout at {0} is neither the PR head {1} nor the merge commit {2} "
        "the event names".format(
            head[:12], (event_head or "?")[:12], merge_sha[:12]
        ),
    )


# --------------------------------------------------------------------------
# Strict manifest parsing
# --------------------------------------------------------------------------

def _no_duplicate_keys(pairs):
    seen = set()
    for key, _value in pairs:
        if key in seen:
            raise ValueError("duplicate JSON key {0!r}".format(key))
        seen.add(key)
    return dict(pairs)


class Manifest(object):
    """A parsed, structurally-validated manifest.

    Structural validity is everything checkable from the file alone: encoding,
    line discipline, field presence/order/type, enum membership, id sequence,
    uniqueness, and the header floors.  Whether the rows agree with the tree
    (golden files present/absent) and whether the file legally evolved from its
    baseline are separate, later checks — a file can be well-formed and still
    an illegal transition.
    """

    def __init__(self, name, header, rows):
        self.name = name
        self.header = header
        self.rows = rows

    @property
    def active(self):
        return [r for r in self.rows if r["state"] == "active"]

    @property
    def tombstoned(self):
        return [r for r in self.rows if r["state"] == "tombstone"]

    def by_id(self):
        return {r["id"]: r for r in self.rows}


def parse_manifest(raw, name):
    """Parse ``raw`` bytes as the ``name`` manifest, or raise ``GateFailure``."""
    spec = _SCHEMAS[name]

    def bad(detail):
        return _contract(
            "MANIFEST_FORMAT_INVALID", "{0}: {1}".format(spec["path"], detail)
        )

    if not isinstance(raw, bytes):
        raise bad("internal: expected bytes")
    if raw.startswith(b"\xef\xbb\xbf"):
        raise bad("byte-order mark is not permitted")
    if b"\r" in raw:
        raise bad("CRLF/CR line endings are not permitted")
    if raw == b"":
        raise bad("file is empty")
    if not raw.endswith(b"\n"):
        raise bad("missing final newline")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise bad("not valid UTF-8: {0}".format(exc))

    lines = text.split("\n")
    # A trailing newline yields one empty trailing element; anything else empty
    # is a blank line, which is not permitted.
    if lines[-1] != "":
        raise bad("internal: final newline accounting")
    lines = lines[:-1]
    for index, line in enumerate(lines, start=1):
        if line.strip() == "":
            raise bad("line {0}: blank lines are not permitted".format(index))
        if line != line.strip():
            raise bad("line {0}: leading/trailing whitespace".format(index))

    objects = []
    for index, line in enumerate(lines, start=1):
        try:
            obj = json.loads(line, object_pairs_hook=_no_duplicate_keys)
        except ValueError as exc:
            raise bad("line {0}: {1}".format(index, exc))
        if not isinstance(obj, dict):
            raise bad("line {0}: expected a JSON object".format(index))
        objects.append(obj)

    header_obj = objects[0]
    _check_fields(header_obj, spec["header"], bad, 1)
    if header_obj["kind"] != "manifest":
        raise bad("line 1: kind must be 'manifest'")
    if header_obj["manifest"] != name:
        raise bad("line 1: manifest must be {0!r}".format(name))
    # `type(...) is int`, not `==` alone: `True == 1` and `1.0 == 1` in Python,
    # so a header carrying JSON `true` or `1.0` would be accepted as version 1 and
    # land in an IMMUTABLE row.
    if (type(header_obj["schema_version"]) is not int
            or header_obj["schema_version"] != SCHEMA_VERSION):
        raise bad("line 1: schema_version must be the integer {0}".format(SCHEMA_VERSION))
    for floor_field in ("minimum_active", "minimum_collected", "maximum_skipped"):
        if floor_field not in spec["header"]:
            continue
        value = header_obj[floor_field]
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise bad("line 1: {0} must be a non-negative integer".format(floor_field))
    # `str(value)` first would let a JSON integer of 40 decimal digits match the
    # sha pattern, and two identically malformed manifests would then agree with
    # each other through transition validation.
    if (type(header_obj["bootstrap_base"]) is not str
            or not _SHA_RE.match(header_obj["bootstrap_base"])):
        raise bad("line 1: bootstrap_base must be a 40-char lowercase hex sha string")

    rows = []
    for index, obj in enumerate(objects[1:], start=2):
        _check_fields(obj, spec["row"], bad, index)
        if obj["kind"] != spec["row_kind"]:
            raise bad("line {0}: kind must be {1!r}".format(index, spec["row_kind"]))
        _check_row_values(obj, name, bad, index)
        rows.append(obj)

    _check_id_sequence(rows, spec, bad)
    _check_uniqueness(rows, spec, bad)

    active = [r for r in rows if r["state"] == "active"]
    if len(active) != header_obj["minimum_active"]:
        # EQUALITY, not ">=". The transition arithmetic already keeps the two in
        # step, but bootstrap skips that arithmetic entirely — so a ">=" check
        # would let the introducing change commit a floor BELOW its own row count
        # and weaken the ledger permanently, with every later transition faithfully
        # preserving the weakened number.
        raise _contract(
            "MANIFEST_FLOOR_INVALID",
            "{0}: {1} active rows but minimum_active is {2}; the floor must equal "
            "the active row count".format(
                spec["path"], len(active), header_obj["minimum_active"]
            ),
        )
    return Manifest(name, header_obj, rows)


def _check_fields(obj, expected, bad, index):
    keys = tuple(obj.keys())
    if keys != expected:
        missing = [k for k in expected if k not in keys]
        extra = [k for k in keys if k not in expected]
        if missing or extra:
            raise bad(
                "line {0}: field set mismatch (missing={1}, unexpected={2})".format(
                    index, missing, extra
                )
            )
        raise bad(
            "line {0}: field ORDER must be {1}, got {2}".format(index, list(expected), list(keys))
        )


def _check_row_values(obj, name, bad, index):
    if not isinstance(obj["id"], str) or not _ID_RE.match(obj["id"]):
        raise bad("line {0}: id must match <prefix>-NNNNNN".format(index))
    if obj["state"] not in STATES:
        raise bad("line {0}: state must be one of {1}".format(index, list(STATES)))
    if name == "pytest-nodes":
        node = obj["node_id"]
        if not isinstance(node, str) or not _NODE_LINE_RE.match(node):
            raise bad("line {0}: node_id is not a tests/... pytest node id".format(index))
        if node.startswith(PYTEST_IGNORE + "/"):
            raise bad(
                "line {0}: node_id is under {1}, which this suite excludes".format(
                    index, PYTEST_IGNORE
                )
            )
    else:
        if obj["renderer"] not in RENDERERS:
            raise bad("line {0}: renderer must be one of {1}".format(index, list(RENDERERS)))
        if obj["disposition"] not in DISPOSITIONS:
            raise bad(
                "line {0}: disposition must be one of {1}".format(index, list(DISPOSITIONS))
            )
        if not isinstance(obj["owner"], str) or not _OWNER_RE.match(obj["owner"]):
            raise bad("line {0}: owner must be 'repository' or '#<issue>'".format(index))
        if not isinstance(obj["input_case"], str) or not obj["input_case"]:
            raise bad("line {0}: input_case must be a non-empty string".format(index))
        _check_expected_file(obj["expected_file"], bad, index)


def _check_expected_file(value, bad, index):
    if not isinstance(value, str) or not value:
        raise bad("line {0}: expected_file must be a non-empty string".format(index))
    if value != value.strip() or "\\" in value:
        raise bad("line {0}: expected_file must be a clean POSIX path".format(index))
    pure = PurePosixPath(value)
    if pure.is_absolute() or ".." in pure.parts:
        raise bad("line {0}: expected_file must be relative with no '..'".format(index))
    if pure.parent != PurePosixPath(GOLDEN_DIR):
        raise bad(
            "line {0}: expected_file must live directly under {1}".format(index, GOLDEN_DIR)
        )
    if pure.suffix != ".xml":
        raise bad("line {0}: expected_file must be a .xml file".format(index))


def _check_id_sequence(rows, spec, bad):
    for position, row in enumerate(rows, start=1):
        expected = "{0}-{1:06d}".format(spec["id_prefix"], position)
        if row["id"] != expected:
            raise bad(
                "row {0}: id must be {1!r} (ids are positional, contiguous and "
                "never reused); got {2!r}".format(position, expected, row["id"])
            )


def _check_uniqueness(rows, spec, bad):
    for field in spec["unique"]:
        seen = {}
        for row in rows:
            value = row[field]
            if value in seen:
                raise bad(
                    "duplicate {0} {1!r} in rows {2} and {3}".format(
                        field, value, seen[value], row["id"]
                    )
                )
            seen[value] = row["id"]


# --------------------------------------------------------------------------
# Transition validation (base -> current)
# --------------------------------------------------------------------------

_LEGAL_STATE_TRANSITIONS = {
    ("active", "active"),
    ("active", "tombstone"),
    ("tombstone", "tombstone"),
}


def validate_transition(base, current, name):
    """Validate that ``current`` is a legal successor of ``base``.

    Returns ``(appended_active, newly_tombstoned, born_tombstoned)`` so the
    caller can check the floor arithmetic AND report every retirement.  Every
    failure is ``MANIFEST_TRANSITION_ILLEGAL``.
    """
    spec = _SCHEMAS[name]

    def bad(detail):
        return _contract(
            "MANIFEST_TRANSITION_ILLEGAL", "{0}: {1}".format(spec["path"], detail)
        )

    for field in ("schema_version", "manifest", "bootstrap_base"):
        if base.header[field] != current.header[field]:
            raise bad("header field {0!r} is immutable".format(field))

    if len(current.rows) < len(base.rows):
        raise bad(
            "rows are append-only: {0} rows at base, {1} now (a row was deleted)".format(
                len(base.rows), len(current.rows)
            )
        )

    newly_tombstoned = 0
    for position, (was, now) in enumerate(zip(base.rows, current.rows), start=1):
        if was["id"] != now["id"]:
            raise bad(
                "row {0}: id changed {1!r} -> {2!r}; ids are positional and "
                "never reassigned".format(position, was["id"], now["id"])
            )
        for field in spec["payload"]:
            if was[field] != now[field]:
                raise bad(
                    "row {0} ({1}): {2} is immutable ({3!r} -> {4!r}); append a "
                    "new row instead of repointing this one".format(
                        position, was["id"], field, was[field], now[field]
                    )
                )
        if (was["state"], now["state"]) not in _LEGAL_STATE_TRANSITIONS:
            raise bad(
                "row {0} ({1}): illegal state transition {2} -> {3}".format(
                    position, was["id"], was["state"], now["state"]
                )
            )
        if was["state"] == "active" and now["state"] == "tombstone":
            newly_tombstoned += 1

    # An appended row may NOT arrive tombstoned. A tombstone is a RETIREMENT
    # RECORD, and there is nothing to retire for an identity that was never in
    # the manifest: the row would permanently reserve an id for something that
    # never collected and never existed here.
    #
    # The multi-commit push that adds a test in one commit and removes it in a
    # later one needs no row at all — from the range's endpoints that test simply
    # never existed. An earlier revision admitted born-tombstones to make such a
    # push legal; that solved a problem that does not exist, and it opened a way
    # to mint reserved identities with unchanged floors.
    appended = current.rows[len(base.rows):]
    born_tombstoned = [row for row in appended if row["state"] == "tombstone"]
    if born_tombstoned:
        raise _contract(
            "MANIFEST_TRANSITION_ILLEGAL",
            "{0}: rows {1} are appended already tombstoned; a tombstone records "
            "the retirement of a row that existed, so an identity that was never "
            "active cannot be introduced retired. A row added and removed within "
            "the same range needs no row at all.".format(
                name, [row["id"] for row in born_tombstoned]
            ),
        )
    appended_active = len(appended)

    _check_floor_arithmetic(base, current, appended_active, newly_tombstoned, name)
    return appended_active, newly_tombstoned, []


def _check_floor_arithmetic(base, current, appended, newly_tombstoned, name):
    spec = _SCHEMAS[name]
    expected = base.header["minimum_active"] + appended - newly_tombstoned
    if current.header["minimum_active"] != expected:
        raise _contract(
            "MANIFEST_FLOOR_INVALID",
            "{0}: minimum_active must be {1} (base {2} + {3} appended - {4} "
            "tombstoned), got {5}".format(
                spec["path"], expected, base.header["minimum_active"], appended,
                newly_tombstoned, current.header["minimum_active"],
            ),
        )
    if "minimum_collected" in spec["header"]:
        lowest = base.header["minimum_collected"] - newly_tombstoned
        if current.header["minimum_collected"] < lowest:
            raise _contract(
                "MANIFEST_FLOOR_INVALID",
                "{0}: minimum_collected may only drop by the number of rows "
                "tombstoned in this change (floor {1}), got {2}".format(
                    spec["path"], lowest, current.header["minimum_collected"]
                ),
            )
    if "maximum_skipped" in spec["header"]:
        # A cap that can be raised silently is not a cap. Lowering it (tightening)
        # is always allowed; raising it is the exact move someone makes to get a
        # mass-skip past the gate, so it is refused here and must instead be a
        # deliberate edit reviewed on its own — the same treatment a tombstone gets.
        if current.header["maximum_skipped"] > base.header["maximum_skipped"]:
            raise _contract(
                "MANIFEST_FLOOR_INVALID",
                "{0}: maximum_skipped may be lowered but not raised ({1} -> {2}); "
                "raising the skip cap is how a mass-skip is laundered past this "
                "gate. If more skips are genuinely warranted, say so explicitly in "
                "a change whose only subject is that cap.".format(
                    spec["path"], base.header["maximum_skipped"],
                    current.header["maximum_skipped"],
                ),
            )


# --------------------------------------------------------------------------
# Baseline resolution
# --------------------------------------------------------------------------

def resolve_baseline(repo, *, event_path=None, event_name=None, base=None):
    """Resolve the manifest baseline for this run context, or fail closed.

    There is exactly one rule per context and no fallback between them:

    PR run    the merge-base of the event's head and base SHAs — and it must be
              unique.  Zero or several merge bases mean the comparison is not
              well defined, which is a refusal, not a coin flip.
    push run  ``github.event.before``, verbatim.  NEVER a merge-base: on a push
              to ``dev`` a merge-base of HEAD against ``dev`` is HEAD itself, so
              it would compare the new tip with itself and validate nothing.
    local run an explicit ``--base``.  No ``HEAD^``, no branch, no remote fetch:
              an inferred local baseline is how a gate silently reviews the
              wrong range.
    """
    if base is not None:
        if event_path is not None:
            raise _contract(
                "BASELINE_EVENT_INVALID", "--base and --github-event are mutually exclusive"
            )
        return {
            "sha": _resolve_commit(repo, base, "BASELINE_UNAVAILABLE"),
            "kind": "local",
            "target": None,
            "event_head": None,
            "after": None,
        }

    if event_path is None:
        raise _contract(
            "BASELINE_EVENT_INVALID",
            "no baseline: pass --base <commit> locally, or --github-event <path> in CI",
        )

    name = event_name or os.environ.get("GITHUB_EVENT_NAME") or ""
    if name not in ("push", "pull_request"):
        raise _contract(
            "BASELINE_EVENT_INVALID",
            "GITHUB_EVENT_NAME must be 'push' or 'pull_request', got {0!r}".format(name),
        )
    try:
        with open(event_path, "rb") as handle:
            event = json.loads(handle.read().decode("utf-8"))
    except (OSError, ValueError) as exc:
        raise _contract(
            "BASELINE_EVENT_INVALID", "cannot read event payload {0}: {1}".format(event_path, exc)
        )
    if not isinstance(event, dict):
        raise _contract("BASELINE_EVENT_INVALID", "event payload is not a JSON object")

    if name == "push":
        return _baseline_from_push(repo, event)
    return _baseline_from_pull_request(repo, event)


def _baseline_from_push(repo, event):
    before = event.get("before")
    if not isinstance(before, str) or not _SHA_RE.match(before):
        raise _contract(
            "BASELINE_EVENT_INVALID",
            "push event has no usable 'before' sha: {0!r}".format(before),
        )
    if before == _ZERO_SHA:
        raise _contract(
            "BASELINE_ZERO_SHA",
            "push 'before' is the all-zero sha (branch creation / force-push): there "
            "is no baseline to validate the manifest transition against",
        )
    # `before` IS the branch tip the push builds on, so it doubles as the
    # target: if the manifests exist there, bootstrap is already impossible via
    # the ordinary all-present check.
    resolved = _resolve_commit(repo, before, "BASELINE_UNAVAILABLE")
    after = event.get("after")
    if not isinstance(after, str) or not _SHA_RE.match(after) or after == _ZERO_SHA:
        # Silently degrading this to None made `check_checkout_matches_event` skip
        # its comparison, so a synthetic event with a valid `before` could bless
        # any checkout — defeating the binding entirely.
        raise _contract(
            "BASELINE_EVENT_INVALID",
            "push event has no usable 'after' sha: {0!r}".format(after),
        )
    return {"sha": resolved, "kind": "push", "target": resolved, "after": after}


def _baseline_from_pull_request(repo, event):
    pull = event.get("pull_request")
    if not isinstance(pull, dict):
        raise _contract("BASELINE_EVENT_INVALID", "pull_request event has no 'pull_request'")
    try:
        head = pull["head"]["sha"]
        target = pull["base"]["sha"]
    except (KeyError, TypeError):
        raise _contract(
            "BASELINE_EVENT_INVALID", "pull_request event lacks head.sha / base.sha"
        )
    for label, value in (("head", head), ("base", target)):
        if not isinstance(value, str) or not _SHA_RE.match(value) or value == _ZERO_SHA:
            raise _contract(
                "BASELINE_EVENT_INVALID",
                "pull_request {0}.sha is not a usable sha: {1!r}".format(label, value),
            )
    head = _resolve_commit(repo, head, "BASELINE_UNAVAILABLE")
    target = _resolve_commit(repo, target, "BASELINE_UNAVAILABLE")
    proc = subprocess.run(
        ["git", "merge-base", "--all", head, target],
        cwd=str(repo), capture_output=True, text=True,
    )
    bases = [line.strip() for line in proc.stdout.splitlines() if line.strip()]
    if proc.returncode != 0 or not bases:
        raise _contract(
            "BASELINE_MERGE_BASE_MISSING",
            "no merge base between {0} and {1} (is the checkout shallow? this "
            "workflow requires fetch-depth: 0)".format(head, target),
        )
    if len(bases) > 1:
        raise _contract(
            "BASELINE_MERGE_BASE_AMBIGUOUS",
            "{0} merge bases between {1} and {2}: {3}".format(
                len(bases), head, target, bases
            ),
        )
    # The merge base is the right baseline for TRANSITIONS, but the wrong thing
    # to decide bootstrap on: a branch cut before the manifests landed keeps a
    # merge base that predates them forever, so it would look like a fresh
    # introduction even after they exist on the target branch. Carry the target
    # tip so `check_bootstrap` can ask the question that actually matters —
    # "do these already exist on the branch we are merging into?"
    merge_sha = pull.get("merge_commit_sha")
    return {
        "sha": bases[0], "kind": "pull_request", "target": target,
        "event_head": head,
        "merge_sha": merge_sha
        if isinstance(merge_sha, str) and _SHA_RE.match(merge_sha or "") else None,
    }


# --------------------------------------------------------------------------
# Bootstrap — the single, scoped exception
# --------------------------------------------------------------------------

def check_bootstrap(repo, baseline, manifests, *, require_flag, flag_given,
                    target=None):
    """Decide whether this change is the one legal manifest bootstrap.

    Returns True if bootstrap applies.  Raises if the change LOOKS like a
    bootstrap but does not qualify — a half-bootstrap is never waved through.

    Bootstrap is permitted exactly once, for the change that introduces BOTH
    manifests.  The ancestry proof (neither path was ever touched anywhere in
    the baseline's history) is what makes it non-reusable: a later
    delete-and-recreate has the paths in its ancestry and therefore can never
    be misclassified as another bootstrap.

    ``require_flag`` is True only for a LOCAL run.  A local baseline is whatever
    the operator typed, so an explicit ``--bootstrap`` is the operator saying
    "yes, I mean the one-time exception".  In CI there is nothing to confirm and
    nobody to confirm it: the event payload IS the evidence, and it is stronger
    than a flag — the push's ``before`` (or the PR's merge base) must itself
    equal the ``bootstrap_base`` both headers declare.  Requiring the flag there
    too would make the bootstrap unreachable in CI, so the run that lands the
    manifests could never go green.
    """
    # FIRST decide whether a bootstrap is even being claimed. Everything below
    # this point applies only to a change that presents itself as the
    # introduction; running any of it against an ordinary transition would
    # refuse perfectly normal work. (An earlier revision ran the target probe
    # ahead of this and rejected EVERY post-landing PR whose target had moved
    # on — the manifests are present at the target for all of them.)
    present = {
        name: _blob_at(repo, baseline, _SCHEMAS[name]["path"]) is not None
        for name in _SCHEMAS
    }
    if all(present.values()):
        return False
    if any(present.values()):
        missing = sorted(n for n, ok in present.items() if not ok)
        raise _contract(
            "BOOTSTRAP_NOT_ALLOWED",
            "manifests {0} are absent at baseline {1} while the others exist: a "
            "manifest cannot be introduced on its own".format(missing, baseline),
        )

    # A bootstrap IS being claimed. Now ask the target branch, when there is
    # one: a PR branched before the manifests landed keeps a merge base that
    # predates them forever, so the merge base alone will always say "absent"
    # and always look like a fresh introduction — even long after the ledger
    # exists on `dev`, and even though GitHub checks out `refs/pull/N/merge`,
    # whose tree HAS them. Only the target tip answers the question that
    # decides it: do these already exist on the branch we are merging into?
    if target is not None and target != baseline:
        landed = sorted(
            spec["path"] for spec in _SCHEMAS.values()
            if _blob_at(repo, target, spec["path"]) is not None
        )
        if landed:
            raise _contract(
                "BOOTSTRAP_NOT_ALLOWED",
                "{0} already exist(s) on the target branch ({1}) though not at "
                "the merge base; a stale merge base does not make this an "
                "introduction. Rebase so the baseline carries the manifests and "
                "the change is validated as a transition.".format(
                    ", ".join(landed), target[:12]
                ),
            )

    for name, spec in _SCHEMAS.items():
        if _path_touched_in_ancestry(repo, baseline, spec["path"]):
            raise _contract(
                "BOOTSTRAP_NOT_ALLOWED",
                "{0} exists somewhere in the ancestry of {1}: bootstrap is a "
                "one-time exception and cannot be reused after a deletion".format(
                    spec["path"], baseline
                ),
            )

    # NOTE on a rule that is deliberately NOT here: "at most one commit in
    # baseline..HEAD may touch a manifest". It looks like it would confine
    # bootstrap to the introduction, but it refuses ordinary multi-commit
    # development of the very change that introduces the ledger — the slice
    # would be unable to validate itself after its second commit. The correct
    # discriminator is not how many commits touched the file, it is whether the
    # ledger has LANDED anywhere that matters, which the baseline-ancestry probe
    # above and the target probe at the top of this function answer between them.

    declared = {m.header["bootstrap_base"] for m in manifests.values()}
    if len(declared) != 1:
        raise _contract(
            "BOOTSTRAP_NOT_ALLOWED",
            "manifests declare different bootstrap_base values: {0}".format(sorted(declared)),
        )
    declared_base = declared.pop()
    if target is not None and target != declared_base:
        # The architect contract: a PR bootstrap requires the TARGET to be the
        # declared base. Checking only the merge base lets a PR go green while
        # `dev` advances past `bootstrap_base` — and the resulting push, whose
        # `before` is that advanced tip, then fails. Green PR, red merge.
        raise _contract(
            "BOOTSTRAP_NOT_ALLOWED",
            "the target is {0} but the manifests declare bootstrap_base {1}; the "
            "branch they are introduced onto has moved, so this can no longer be "
            "the bootstrap. Rebase and regenerate the headers.".format(
                target[:12], declared_base[:12]
            ),
        )
    if declared_base != baseline:
        raise _contract(
            "BOOTSTRAP_NOT_ALLOWED",
            "bootstrap_base {0} does not equal the validated baseline {1}".format(
                declared_base, baseline
            ),
        )
    if require_flag:
        if not flag_given:
            raise _contract(
                "BOOTSTRAP_NOT_ALLOWED",
                "a local run must pass --bootstrap to exercise the one-time bootstrap "
                "exception (CI does not need it: the event's own baseline must equal "
                "the declared bootstrap_base, which it does here)",
            )
        # A LOCAL bootstrap is an operator assertion, and it is labelled as one.
        #
        # There is deliberately no local check that the exception is still
        # unspent. Eight successive formulations of "has this ledger landed?"
        # were each defeated — ancestry-only; a commit-count rule; exempting
        # `*/<branch>` mirrors; enumerating ref namespaces; `--abbrev-ref`
        # ambiguity; matching the introducing COMMIT rather than the path;
        # `--all --not <own_ref>` subtracting merged-in commits; and default
        # history simplification pruning a `merge -s ours` addition. They did not
        # fail through sloppiness: locally the OPERATOR chooses the baseline, so
        # no rule can separate "legitimately introducing the ledger" from
        # "asserting a stale baseline". The question is ill-posed here, and
        # answering it wrongly in either direction is worse than not claiming to
        # answer it — a false refusal blocks the introduction itself.
        #
        # The authority for bootstrap therefore lives where the baseline is
        # supplied by the platform rather than chosen by the person being
        # checked: the `ci` arms. Those are unaffected, and they are strict —
        # `push` compares against the branch tip it builds on, `pull_request`
        # additionally requires the target to carry no manifests.
        # Accurate about what was and was not proven. The eligibility itself IS
        # derived — both manifests absent at the baseline, neither path anywhere
        # in its ancestry, and the declared `bootstrap_base` matching — so the
        # flag confirms intent and never substitutes for the derivation. What a
        # local run cannot check is the BASELINE CHOICE: `--base` is whatever the
        # operator typed, and a baseline from before the manifests landed has no
        # prior manifest to transition from, so no transition is validated.
        _emit(
            "wave_gate: NOTE — bootstrap eligibility was DERIVED (manifests absent "
            "at the baseline and never touched in its ancestry); --bootstrap only "
            "confirms you meant it. The baseline itself is your choice, and a "
            "pre-manifest baseline has no transition to validate. Once the ledger "
            "has landed, run `--base <a commit that carries the manifests>` "
            "WITHOUT --bootstrap to exercise the transition rules."
        )
    return True


# --------------------------------------------------------------------------
# Current-tree self-consistency
# --------------------------------------------------------------------------

def check_golden_tree(repo, goldens):
    """Every active row has its file; every tombstoned row's file is gone; and
    no golden XML exists that no row declares.

    The set equality is the half that matters: without it a golden could be
    added with no manifest row and would never be executed by the wave gate.
    """
    golden_dir = os.path.join(repo, GOLDEN_DIR)
    if not os.path.isdir(golden_dir):
        raise _invalid("GOLDEN_FILE_MISSING", "{0} does not exist".format(GOLDEN_DIR))

    _refuse_symlinked_ancestor(
        repo, GOLDEN_DIR, "GOLDEN_FILE_UNDECLARED", "goldens", make=_invalid
    )

    on_disk = set()
    for entry in sorted(os.listdir(golden_dir)):
        full = os.path.join(golden_dir, entry)
        if os.path.islink(full):
            raise _invalid(
                "GOLDEN_FILE_UNDECLARED",
                "{0}/{1} is a symlink; goldens must be regular files".format(GOLDEN_DIR, entry),
            )
        if os.path.isdir(full):
            # Refused, not skipped. Manifest rows must name a file DIRECTLY under
            # GOLDEN_DIR, so anything inside a subdirectory can never be declared
            # — and skipping the directory would leave it out of `on_disk` too,
            # making the "set equality" claim below vacuously true while the
            # nested golden is never rendered by anything.
            raise _invalid(
                "GOLDEN_FILE_UNDECLARED",
                "{0}/{1} is a directory; the golden corpus is flat, and nothing "
                "inside a subdirectory can be declared in the manifest or "
                "rendered by the gate".format(GOLDEN_DIR, entry),
            )
        if not os.path.isfile(full):
            raise _invalid(
                "GOLDEN_FILE_UNDECLARED",
                "{0}/{1} is neither a regular file nor a directory".format(
                    GOLDEN_DIR, entry
                ),
            )
        if not entry.endswith(".xml"):
            raise _invalid(
                "GOLDEN_FILE_UNDECLARED",
                "{0}/{1} is not a .xml file".format(GOLDEN_DIR, entry),
            )
        on_disk.add("{0}/{1}".format(GOLDEN_DIR, entry))

    declared = {row["expected_file"] for row in goldens.active}
    missing = sorted(declared - on_disk)
    if missing:
        raise _invalid(
            "GOLDEN_FILE_MISSING",
            "active manifest rows name files that do not exist: {0}".format(missing),
        )
    undeclared = sorted(on_disk - declared)
    if undeclared:
        raise _invalid(
            "GOLDEN_FILE_UNDECLARED",
            "golden files with no active manifest row (add a row, or tombstone "
            "the file's row and delete the file): {0}".format(undeclared),
        )
    still_there = sorted(
        row["expected_file"] for row in goldens.tombstoned
        if os.path.exists(os.path.join(repo, row["expected_file"]))
    )
    if still_there:
        raise _invalid(
            "GOLDEN_FILE_UNDECLARED",
            "tombstoned rows whose files are still present: {0}".format(still_there),
        )


# --------------------------------------------------------------------------
# pytest collection and execution
# --------------------------------------------------------------------------

def _pytest_env():
    """EXACTLY the documented invocation environment — nothing wider.

    The committed node ids were captured under ``PYTHONPATH=src``, which is also
    what the README and the workflow specify. Collecting under a WIDER path can
    resolve imports differently, so the gate would be measuring a different node
    set than the floor and the required-node list were measured against. The two
    must be the same environment or neither number means anything.
    """
    env = dict(os.environ)
    env["PYTHONPATH"] = "src"
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env.pop("PYTHONHASHSEED", None)
    return env


def _render_env(repo, hashseed, tmpdir=None):
    """The golden-render child runs OUTSIDE pytest, so it must reproduce by hand
    the ``sys.path`` entries pytest would have inserted for it: the repo root
    (for the ``src.``-prefixed spelling one producer uses), ``tests/`` and
    ``tests/patterns/`` (for the producers' sibling imports).

    Deliberately a different environment from ``_pytest_env`` — this child is not
    collecting tests, and giving pytest this wider path would change the node set.
    """
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        [os.path.join(repo, "src"), repo, os.path.join(repo, "tests"),
         os.path.join(repo, "tests", "patterns")]
    )
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env["PYTHONHASHSEED"] = str(hashseed)
    if tmpdir is not None:
        # Per-pass temporary space, so the two children cannot meet through it.
        for key in ("TMPDIR", "TEMP", "TMP"):
            env[key] = tmpdir
    return env


def _pytest_argv(*extra):
    return [
        sys.executable, "-m", "pytest", PYTEST_TARGET,
        "--ignore={0}".format(PYTEST_IGNORE),
        "-p", "no:cacheprovider", *extra,
    ]


def collect_nodes(repo, tmpdir):
    """Run collection and return the set of collected node ids, fail-closed.

    Four independent things must hold, because any one of them alone can be
    satisfied by a broken run: the child must EXIT 0, it must print exactly one
    parseable collection summary, it must print at least one node, and the
    summary count must equal the number of unique node lines.  A collection
    that half-imports and reports a smaller number is caught by the last one.
    """
    proc = subprocess.run(
        _pytest_argv("--collect-only", "-q"),
        cwd=str(repo), capture_output=True, text=True, env=_pytest_env(),
    )
    if proc.returncode != 0:
        tail = "\n".join((proc.stdout + proc.stderr).splitlines()[-40:])
        raise _invalid(
            "PYTEST_COLLECTION_FAILED",
            "collection exited {0}:\n{1}".format(proc.returncode, tail),
        )
    unique = parse_collection_output(proc.stdout)
    with tmpdir.open_for_write("collected.txt") as handle:
        handle.write("\n".join(sorted(unique)) + "\n")
    return unique


def parse_collection_output(stdout):
    """Turn ``--collect-only -q`` output into a node-id set, fail-closed."""
    nodes = []
    summaries = []
    for line in stdout.splitlines():
        stripped = line.strip()
        if _NODE_LINE_RE.match(stripped):
            nodes.append(stripped)
            continue
        match = _COLLECTED_RE.match(stripped)
        if match:
            summaries.append(int(match.group(1)))

    if len(summaries) != 1:
        raise _invalid(
            "PYTEST_COLLECTION_FAILED",
            "expected exactly one collection summary line, found {0}".format(len(summaries)),
        )
    if not nodes:
        raise _invalid("PYTEST_COLLECTION_EMPTY", "collection produced no node ids")
    unique = set(nodes)
    if len(unique) != len(nodes):
        duplicates = sorted({n for n in nodes if nodes.count(n) > 1})[:10]
        raise _invalid(
            "PYTEST_COLLECTION_DUPLICATE",
            "duplicate node ids in collection: {0}".format(duplicates),
        )
    if summaries[0] != len(nodes):
        raise _invalid(
            "PYTEST_COLLECTION_FAILED",
            "collection summary says {0} tests but {1} node lines were parsed; "
            "the output does not reconcile".format(summaries[0], len(nodes)),
        )
    return unique


def check_collection(nodes_manifest, collected):
    """Assert the collection floor, then the required-node subset.

    Order is deliberate: the floor is checked first so it can be demonstrated
    to trip on its own, with every required node still present and every
    collected test passing.
    """
    floor = nodes_manifest.header["minimum_collected"]
    if len(collected) < floor:
        raise _invalid(
            "PYTEST_COLLECTION_FLOOR",
            "collected {0} tests, below the committed floor of {1}; a partial "
            "collection is not a green run".format(len(collected), floor),
        )
    required = [row["node_id"] for row in nodes_manifest.active]
    missing = sorted(n for n in required if n not in collected)
    if missing:
        raise _invalid(
            "PYTEST_NODE_MISSING",
            "{0} required node id(s) are not in the collection; removing a "
            "required test needs an explicit manifest tombstone in the same "
            "change. First 20: {1}".format(len(missing), missing[:20]),
        )

    # The mirror of the golden rule (`check_golden_tree` already refuses a
    # tombstoned row whose file survives). Without it the two halves of a
    # retirement can be split across changes: tombstone a test that is still
    # there — legally lowering both floors — and the deletion later needs no
    # manifest edit at all, because the floor reduction was already prepaid and
    # a tombstoned node is not required. A tombstone must mean the test is gone
    # NOW, not that someone intends to remove it.
    retired = [row["node_id"] for row in nodes_manifest.tombstoned]
    surviving = sorted(n for n in retired if n in collected)
    if surviving:
        raise _invalid(
            "PYTEST_NODE_TOMBSTONED_BUT_PRESENT",
            "{0} tombstoned node id(s) are still collected; a tombstone records a "
            "retirement that has happened, so the test must be removed in the same "
            "change. First 20: {1}".format(len(surviving), surviving[:20]),
        )


def run_suite(repo, nodes_manifest, collected):
    """Run the suite and account for what actually EXECUTED.

    The exit code alone is not evidence. A required node id proves a test still
    EXISTS and is collectable; it says nothing about whether it RAN. pytest
    collects a skipped test, reports it in the count, and exits 0 — so a single
    module-level ``pytestmark = pytest.mark.skip`` silently neutralises every
    test in that module while collection, the floor, the required-node check and
    the exit code all stay green. Measured on this suite: one such line disarmed
    119 required tests with the whole gate passing.

    That is the same aggregate-fails-open shape this gate exists to prevent, one
    level up, so the same answer applies: assert a committed bound. ``passed`` is
    floored (derived, not a second free number) and ``skipped`` is capped.

    The cap is not zero because environment-conditional skips are legitimate and
    genuinely differ between here and a runner — this suite has 22 runtime
    ``pytest.skip()`` sites plus a ``skipif`` on ``gcloud`` being on PATH, which
    is true locally and false on ubuntu-24.04. The cap is set well above that
    (measured 18 here, ~19 expected on a bare runner) and far below any
    mass-skip.
    """
    argv = _pytest_argv("-q", "-rs")
    proc = subprocess.Popen(
        argv, cwd=str(repo), env=_pytest_env(),
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1,
    )
    tail = []
    # Streamed, not captured: a 13-minute silent step is unreadable in a CI log,
    # and buffering the whole run to parse it at the end would produce exactly
    # that.
    for line in proc.stdout:
        sys.stderr.write(line)
        tail.append(line)
        if len(tail) > 400:
            del tail[0]
    sys.stderr.flush()
    status = proc.wait()
    summary = _parse_suite_summary("".join(tail))

    if status != 0:
        raise _invalid(
            "PYTEST_FAILED",
            "the non-KB suite exited {0} ({1})".format(status, summary),
        )
    if summary["failed"] or summary["errors"]:
        raise _invalid(
            "PYTEST_FAILED",
            "the non-KB suite exited 0 but reported {0} failed / {1} errors".format(
                summary["failed"], summary["errors"]
            ),
        )

    cap = nodes_manifest.header["maximum_skipped"]
    if summary["skipped"] > cap:
        raise _invalid(
            "PYTEST_SKIPPED_EXCEEDS_CAP",
            "{0} tests skipped, above the committed cap of {1}. A test that is "
            "collected but never runs is not evidence of anything; if this rise "
            "is deliberate, raise maximum_skipped in {2} in the same change."
            .format(summary["skipped"], cap, NODES_MANIFEST),
        )

    floor = len(collected) - cap
    if summary["passed"] < floor:
        raise _invalid(
            "PYTEST_PASSED_BELOW_FLOOR",
            "{0} tests passed, below the floor of {1} ({2} collected - {3} "
            "permitted skips)".format(summary["passed"], floor, len(collected), cap),
        )
    return summary


def _parse_suite_summary(text):
    """Read pytest's outcome counts out of its final summary line."""
    counts = {"passed": 0, "failed": 0, "skipped": 0, "errors": 0}
    seen = False
    for line in text.splitlines():
        stripped = line.strip().strip("=").strip()
        if not re.search(r"\b\d+ (passed|failed|skipped|error)", stripped):
            continue
        if " in " not in stripped:
            continue
        seen = True
        for key, pattern in (
            ("passed", r"(\d+) passed"), ("failed", r"(\d+) failed"),
            ("skipped", r"(\d+) skipped"), ("errors", r"(\d+) error"),
        ):
            match = re.search(pattern, stripped)
            counts[key] = int(match.group(1)) if match else 0
    if not seen:
        raise _invalid(
            "PYTEST_SUMMARY_UNPARSEABLE",
            "the suite produced no parseable outcome summary; its result cannot "
            "be accounted for, so it is not a pass",
        )
    return counts


# --------------------------------------------------------------------------
# Golden rendering — the deterministic double compile
# --------------------------------------------------------------------------

def _render_dir(tmpdir, index):
    """A fresh per-pass directory, created and OPENED relative to the scratch.

    Returns ``(path, fd)``. The descriptor is what writes go through: converting
    the pass directory to a plain string and calling `open()` on it would follow
    a retargeted name and write outside the held scratch. The pathname is derived
    only to hand to the child process, which must resolve a name because it is a
    separate process.
    """
    name = "render-{0}".format(index)
    held = getattr(tmpdir, "fd", None)
    try:
        if held is not None:
            fd = tmpdir.mkdir_owned(name)
        else:
            os.mkdir(os.path.join(tmpdir, name), 0o700)
            fd = None
    except OSError as exc:
        raise _invalid(
            "GOLDEN_RENDER_FAILED",
            "cannot create the pass-{0} scratch directory ({1})".format(index, exc),
        )
    return os.path.join(tmpdir, name), fd


def _render_pass(repo, goldens, tmpdir, hashseed):
    """Render every ACTIVE golden once, in a fresh child process.

    A child process, not an in-process call: two renders inside one interpreter
    share every module-level cache, so they would agree even if the emission
    depended on import order or on a hash-seeded iteration.  Two children with
    DIFFERENT ``PYTHONHASHSEED`` values is what makes the comparison meaningful.
    """
    request = [
        {"id": row["id"], "input_case": row["input_case"], "renderer": row["renderer"]}
        for row in goldens.active
    ]
    pass_dir, pass_fd = _render_dir(tmpdir, hashseed)
    request_name = "request-{0}.json".format(hashseed)
    try:
        if pass_fd is not None:
            handle = os.fdopen(
                os.open(request_name,
                        os.O_WRONLY | os.O_CREAT | os.O_EXCL | _O_NOFOLLOW, 0o600,
                        dir_fd=pass_fd),
                "w",
            )
            # Recorded only after the EXCLUSIVE create succeeded.
            tmpdir.own("render-{0}/{1}".format(hashseed, request_name))
        else:
            handle = open(os.path.join(pass_dir, request_name), "x")
        with handle:
            json.dump(request, handle)
    finally:
        if pass_fd is not None:
            _close_quietly(pass_fd)
    request_path = os.path.join(pass_dir, request_name)

    proc = subprocess.run(
        [sys.executable, os.path.join(repo, GOLDEN_CORPUS), "--render", request_path],
        # The two passes must not share a runtime filesystem, not just a request
        # file. Separate directories alone leave both children with the repo as
        # cwd and the SAME inherited TMPDIR, so a renderer that caches through
        # `tempfile.gettempdir()` serves pass 1's bytes to pass 2 and the
        # determinism check agrees with itself. `cwd` is the pass directory too,
        # so a relative write lands there; every path handed to the child is
        # absolute, so nothing depends on it.
        cwd=pass_dir, capture_output=True, text=True,
        env=_render_env(repo, hashseed, pass_dir),
    )
    if proc.returncode != 0:
        tail = "\n".join((proc.stdout + proc.stderr).splitlines()[-40:])
        raise _invalid(
            "GOLDEN_RENDER_FAILED",
            "golden render (PYTHONHASHSEED={0}) exited {1}:\n{2}".format(
                hashseed, proc.returncode, tail
            ),
        )

    results = {}
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            envelope = json.loads(line)
        except ValueError:
            raise _invalid(
                "GOLDEN_RENDER_FAILED",
                "unparseable render envelope: {0!r}".format(line[:200]),
            )
        rid = envelope["id"]
        if rid in results:
            raise _invalid(
                "GOLDEN_OUTPUT_SET_MISMATCH", "duplicate render result for {0}".format(rid)
            )
        results[rid] = base64.b64decode(envelope["b64"])
    return results


def check_goldens(repo, goldens, tmpdir):
    expected_ids = {row["id"] for row in goldens.active}
    # SEPARATE directories, not just separate processes. Sharing one directory
    # lets a renderer that caches beside its request file have pass 2 read what
    # pass 1 wrote — the two passes then agree because they shared state, which
    # is precisely the nondeterminism this check exists to expose.
    first = _render_pass(repo, goldens, tmpdir, 1)
    second = _render_pass(repo, goldens, tmpdir, 2)

    for label, results in (("pass 1", first), ("pass 2", second)):
        if set(results) != expected_ids:
            raise _invalid(
                "GOLDEN_OUTPUT_SET_MISMATCH",
                "{0} rendered {1} ids, manifest declares {2} active (missing={3}, "
                "extra={4})".format(
                    label, len(results), len(expected_ids),
                    sorted(expected_ids - set(results))[:10],
                    sorted(set(results) - expected_ids)[:10],
                ),
            )

    nondeterministic = sorted(rid for rid in expected_ids if first[rid] != second[rid])
    if nondeterministic:
        raise _invalid(
            "GOLDEN_NONDETERMINISTIC",
            "two isolated renders disagree for: {0}".format(nondeterministic),
        )

    mismatched = []
    for row in goldens.active:
        path = os.path.join(repo, row["expected_file"])
        with open(path, "rb") as handle:
            committed = handle.read()
        if first[row["id"]] != committed:
            mismatched.append(row["expected_file"])
    if mismatched:
        raise _invalid(
            "GOLDEN_MISMATCH",
            "rendered bytes differ from the committed golden for: {0}".format(mismatched),
        )
    return len(expected_ids)


# --------------------------------------------------------------------------
# #153 seam — the relocatable plan fingerprint
# --------------------------------------------------------------------------

#: #153 replaces this with a provider object exposing:
#:
#:     cases() -> list[str]
#:     fingerprint(case, *, account, environment, mutation=None) -> str
#:     mutations(case) -> list[str]
#:
#: The gate then asserts BOTH halves of the contract: the same plan under two
#: different account/environment identities must yield the SAME fingerprint
#: (relocatability), and every declared semantic mutation must yield a
#: DIFFERENT one (discrimination).  Checking only the first would be satisfied
#: by a constant.
PLAN_FINGERPRINT_PROVIDER = None

_RELOCATION_A = {"account": "wave-gate-account-a", "environment": "wave-gate-env-a"}
_RELOCATION_B = {"account": "wave-gate-account-b", "environment": "wave-gate-env-b"}

#: The mutation classes the plan enumerates. A provider must declare all four:
#: proving a fingerprint reacts to a "semantic" change says nothing about
#: whether it reacts to an envelope, policy, or revision change.
REQUIRED_MUTATION_KINDS = ("semantic", "envelope", "policy", "revision")


def run_plan_fingerprint_checks(require, provider=None):
    """The #153 seam.  Returns a short status string.

    While #153 is unimplemented this is INFORMATIONAL — #152 must be able to go
    green before the fingerprint type exists.  ``--require-plan-fingerprint``
    turns the pending state into a hard failure, which is how #153 proves the
    seam actually activates instead of silently staying pending forever.
    """
    provider = provider if provider is not None else PLAN_FINGERPRINT_PROVIDER
    if provider is None:
        if require:
            raise _invalid(
                "PLAN_FINGERPRINT_PENDING",
                "--require-plan-fingerprint was passed but no provider is "
                "registered; the relocatable plan fingerprint arrives with #153",
            )
        return "pending:#153"

    cases = _provider_strings(lambda: provider.cases(), "cases()")
    if not cases:
        raise _invalid(
            "PLAN_FINGERPRINT_MISMATCH", "the registered provider declares no cases"
        )
    for case in cases:
        first, material_a = _fingerprint(provider, case, _RELOCATION_A)
        second, material_b = _fingerprint(provider, case, _RELOCATION_B)

        # Compare the canonical MATERIAL, not only the digest. A digest that
        # matches proves nothing on its own — a provider returning a constant
        # matches too. The property being asserted is that the canonical bytes
        # carry no account or environment identity, which is checkable only by
        # looking at the bytes.
        if material_a != material_b:
            raise _invalid(
                "PLAN_FINGERPRINT_MISMATCH",
                "case {0!r}: canonical material differs between identities, so the "
                "plan is not relocatable even though the digests may agree".format(case),
            )
        if first != second:
            raise _invalid(
                "PLAN_FINGERPRINT_MISMATCH",
                "case {0!r}: fingerprint is not relocatable ({1} != {2})".format(
                    case, first, second
                ),
            )

        mutations = _provider_strings(lambda: provider.mutations(case), "mutations()")
        missing = [kind for kind in REQUIRED_MUTATION_KINDS if kind not in mutations]
        if missing:
            # The plan enumerates these four. A provider that declares only
            # "semantic" would pass a discrimination check while saying nothing
            # about envelope, policy or revision changes.
            raise _invalid(
                "PLAN_FINGERPRINT_MISMATCH",
                "case {0!r}: mutation kinds {1} are not declared; the contract "
                "requires all of {2}".format(
                    case, missing, list(REQUIRED_MUTATION_KINDS)
                ),
            )
        seen_mutations = {}
        for mutation in mutations:
            mutated, mutated_material = _fingerprint(
                provider, case, _RELOCATION_A, mutation
            )
            # Relocatability is a property of EVERY plan, not just the unmutated
            # one: a provider could be identity-independent for the base case and
            # account-dependent as soon as anything changes.
            relocated, relocated_material = _fingerprint(
                provider, case, _RELOCATION_B, mutation
            )
            if mutated != relocated or mutated_material != relocated_material:
                raise _invalid(
                    "PLAN_FINGERPRINT_MISMATCH",
                    "case {0!r} under mutation {1!r}: not relocatable".format(
                        case, mutation
                    ),
                )
            if mutated_material == material_a:
                raise _invalid(
                    "PLAN_FINGERPRINT_MISMATCH",
                    "case {0!r}: mutation {1!r} did not change the canonical "
                    "material, so it is not the mutation it claims to be".format(
                        case, mutation
                    ),
                )
            if mutated == first:
                raise _invalid(
                    "PLAN_FINGERPRINT_MISMATCH",
                    "case {0!r}: mutation {1!r} changed the canonical material but "
                    "not the fingerprint — a collision the gate must not accept"
                    .format(case, mutation),
                )
            seen_mutations[mutation] = mutated_material

        # Each KIND must move the plan differently. Without this a provider can
        # declare all four names, ignore which one was asked for, and return one
        # identical "changed" plan every time — every result differs from the
        # base, so the loop above passes while proving nothing about envelope,
        # policy or revision discrimination.
        collisions = {}
        for kind, material in seen_mutations.items():
            collisions.setdefault(material, []).append(kind)
        indistinct = sorted(v for v in collisions.values() if len(v) > 1)
        if indistinct:
            raise _invalid(
                "PLAN_FINGERPRINT_MISMATCH",
                "case {0!r}: mutation kinds {1} produced identical canonical "
                "material, so the provider is not distinguishing them".format(
                    case, indistinct
                ),
            )
    return "checked:{0} case(s)".format(len(cases))


def _fingerprint_line(status):
    """The #153 seam's contract line.

    `PLAN_FINGERPRINT_PENDING issue=#153` is what #153 activates against, so it
    is emitted verbatim rather than as human prose — a lowercase sentence would
    make the seam unaddressable by the contract the plan names.
    """
    if status.startswith("pending"):
        return "PLAN_FINGERPRINT_PENDING issue=#153"
    return "wave_gate: plan fingerprint {0}".format(status)


def run_fingerprint_phase(require, provider=None):
    """One fail-closed boundary around the whole provider phase.

    The provider's OUTPUTS are compared and formatted after the guarded calls
    return, so the boundary has to wrap the phase, not just the calls into it.

    A real ``GateFailure`` passes through UNCHANGED — that is what keeps the
    stable diagnostic contract (``PLAN_FINGERPRINT_PENDING`` must stay
    ``PLAN_FINGERPRINT_PENDING``, and a real mismatch must keep its detail), and
    it is safe because ``GateFailure`` now guarantees its own status is 1 or 2.

    Anything else becomes a fixed-message status-1 failure. The message does NOT
    inspect the caught object or its class: even ``type(exc).__name__`` can run
    provider code through a metaclass hook, and a hook that raised
    ``SystemExit(0)`` would escape the very handler meant to contain it.
    """
    try:
        return run_plan_fingerprint_checks(require, provider)
    except GateFailure:
        raise
    except BaseException:  # noqa: BLE001
        raise _invalid(
            "PLAN_FINGERPRINT_MISMATCH",
            "the plan-fingerprint phase raised a non-gate exception; the "
            "provider is not usable",
        )


def _provider_call(fn, *args):
    """Call into the #153 provider, keeping failures on the diagnostic path.

    A provider that raises must not surface as an unhandled traceback: the gate's
    contract is that every refusal carries a stable code.
    """
    try:
        return fn(*args)
    except BaseException as exc:  # noqa: BLE001
        # NOT `except GateFailure: raise`. A provider is third-party code from
        # this function's point of view, and `GateFailure` carries its own exit
        # status — `GateFailure(..., 0)` would have propagated straight out and
        # exited the wave green. Everything a provider raises becomes a status-1
        # PLAN_FINGERPRINT_MISMATCH.
        # BaseException, not Exception: a provider raising `SystemExit(0)` would
        # otherwise terminate the whole wave with status 0 and skip the final
        # hygiene check — a green run that validated nothing.
        raise _invalid(
            "PLAN_FINGERPRINT_MISMATCH",
            "the plan-fingerprint provider raised {0}: {1}".format(
                type(exc).__name__, exc
            ),
        )


def _provider_strings(fn, what):
    """A provider list, MATERIALIZED and type-checked inside the guard.

    ``list(_provider_call(...))`` iterates AFTER the guard has returned, so a
    generator that raises mid-iteration escapes uncoded — and a bare string would
    quietly become a list of characters.
    """
    def _materialize():
        value = fn()
        if isinstance(value, (str, bytes)) or value is None:
            raise TypeError(
                "{0} must be a list of strings, got {1}".format(
                    what, type(value).__name__
                )
            )
        items = list(value)
        for item in items:
            if not isinstance(item, str) or not item:
                raise TypeError(
                    "{0} must contain non-empty strings, got {1!r}".format(what, item)
                )
        return items

    return _provider_call(_materialize)


def _fingerprint(provider, case, identity, mutation=None):
    """Return ``(digest, material)`` from the provider, strictly typed."""
    result = _provider_call(
        lambda: provider.fingerprint(case, mutation=mutation, **identity)
    )
    # `type(...) is tuple`, not `isinstance`: a tuple SUBCLASS can override
    # `__len__`/`__iter__` and run provider code during the check itself.
    if type(result) is not tuple or len(result) != 2:
        raise _invalid(
            "PLAN_FINGERPRINT_MISMATCH",
            "case {0!r}: provider must return (digest, canonical_material), got "
            "{1}".format(case, type(result).__name__),
        )
    digest, material = result
    if type(digest) is not str:
        raise _invalid(
            "PLAN_FINGERPRINT_MISMATCH",
            "case {0!r}: digest must be an exact str".format(case),
        )
    if type(material) is not bytes or not material:
        raise _invalid(
            "PLAN_FINGERPRINT_MISMATCH",
            "case {0!r}: canonical material must be non-empty bytes — the gate "
            "compares the bytes the digest was derived from, not just the "
            "digest".format(case),
        )
    # RECOMPUTE it. Accepting any non-empty string lets the digest and the
    # material drift apart entirely: a provider could return a stable digest and
    # independently varying bytes and satisfy every other check, so "the exact
    # canonical byte material used to derive it" would be unverified.
    expected = "sha256:" + hashlib.sha256(material).hexdigest()
    if digest != expected:
        raise _invalid(
            "PLAN_FINGERPRINT_MISMATCH",
            "case {0!r}: digest {1!r} is not sha256 of the canonical material "
            "(expected {2!r}); the fingerprint must be derived from the bytes the "
            "provider returns".format(case, digest, expected),
        )
    return digest, material


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------

def _load_current(repo):
    manifests = {}
    for name, spec in _SCHEMAS.items():
        path = os.path.join(repo, spec["path"])
        try:
            with open(path, "rb") as handle:
                raw = handle.read()
        except OSError as exc:
            # Distinct from a FORMAT failure on purpose: "the ledger is not
            # there" and "the ledger is malformed" call for different fixes, and
            # the missing case is what a half-introduced or half-deleted pair
            # looks like from the tree side.
            raise _contract(
                "MANIFEST_MISSING", "cannot read {0}: {1}".format(spec["path"], exc)
            )
        if os.path.islink(path):
            raise _contract(
                "MANIFEST_FORMAT_INVALID", "{0} is a symlink".format(spec["path"])
            )
        _refuse_symlinked_ancestor(
            repo, spec["path"], "MANIFEST_FORMAT_INVALID", "manifests"
        )
        manifests[name] = parse_manifest(raw, name)
    return manifests


def run_manifest_phase(repo, baseline, *, is_local, bootstrap_flag, target=None):
    """Baseline + format + transition + current-tree self-consistency."""
    current = _load_current(repo)
    bootstrapping = check_bootstrap(
        repo, baseline, current, require_flag=is_local,
        flag_given=bootstrap_flag, target=target,
    )
    if not bootstrapping:
        for name, spec in _SCHEMAS.items():
            raw = _blob_at(repo, baseline, spec["path"])
            if raw is None:  # pragma: no cover - check_bootstrap already proved presence
                raise _contract(
                    "BASELINE_UNAVAILABLE",
                    "{0} is unreadable at baseline {1}".format(spec["path"], baseline),
                )
            base = parse_manifest(raw, name)
            _appended, tombstoned, born = validate_transition(base, current[name], name)
            for row in born:
                _emit(
                    "wave_gate: TOMBSTONE {0} {1} owner={2} disposition={3} "
                    "(appended already retired)".format(
                        name, row["id"], row.get("owner", "repository"),
                        row.get("disposition", "n/a"),
                    )
                )
            if tombstoned:
                # The plan's compensating control for the ownership git cannot
                # enforce: surface WHO claimed each retirement so review can check
                # the change really belongs to the issue the row names.
                was = {row["id"]: row["state"] for row in base.rows}
                for row in current[name].rows:
                    if row["state"] == "tombstone" and was.get(row["id"]) == "active":
                        _emit(
                            "wave_gate: TOMBSTONE {0} {1} owner={2} disposition={3}".format(
                                name, row["id"], row.get("owner", "repository"),
                                row.get("disposition", "n/a"),
                            )
                        )
    check_golden_tree(repo, current["goldens"])
    return current, bootstrapping


def _emit(message):
    sys.stderr.write(message + "\n")
    sys.stderr.flush()


def execute(args):
    repo = _repo_root(args.repo)
    status_before = _status(repo)

    explicit_base = getattr(args, "base", None)
    context = resolve_baseline(
        repo,
        event_path=getattr(args, "github_event", None),
        event_name=getattr(args, "event_name", None),
        base=explicit_base,
    )
    baseline = context["sha"]
    check_checkout_matches_event(repo, context)
    _emit("wave_gate: baseline {0} ({1})".format(baseline, context["kind"]))

    current, bootstrapping = run_manifest_phase(
        repo, baseline,
        is_local=explicit_base is not None,
        bootstrap_flag=getattr(args, "bootstrap", False),
        target=context["target"],
    )
    if bootstrapping:
        _emit("wave_gate: BOOTSTRAP — manifests are introduced by this change")
    _emit(
        "wave_gate: manifests ok ({0} required nodes, {1} active goldens)".format(
            len(current["pytest-nodes"].active), len(current["goldens"].active)
        )
    )
    if args.command == "manifests":
        _emit(
            "wave_gate: NOT A GATE — 'manifests' checks the manifests only. CI must "
            "run 'ci'; a wave must run 'wave'."
        )
        return 0

    failure = None
    unexpected = False
    tmpdir = None
    try:
        # Inside the boundary: a scratch that cannot be created safely is a gate
        # failure like any other, and still gets the closing fingerprint.
        tmpdir = make_scratch_dir(repo)
        collected = collect_nodes(repo, tmpdir)
        check_collection(current["pytest-nodes"], collected)
        _emit("wave_gate: collection ok ({0} tests)".format(len(collected)))

        summary = run_suite(repo, current["pytest-nodes"], collected)
        _emit(
            "wave_gate: non-KB suite green ({0} passed, {1} skipped, cap {2})".format(
                summary["passed"], summary["skipped"],
                current["pytest-nodes"].header["maximum_skipped"],
            )
        )

        if args.command == "wave":
            rendered = check_goldens(repo, current["goldens"], tmpdir)
            _emit(
                "wave_gate: {0} active goldens deterministic and byte-exact".format(rendered)
            )

        # BOTH `ci` and `wave` run the #153 seam and emit its contract line —
        # a machine-readable token, not prose, because #153 activates against
        # this exact string.
        status = run_fingerprint_phase(getattr(args, "require_plan_fingerprint", False))
        _emit(_fingerprint_line(status))
    except GateFailure as exc:
        failure = exc
    except BaseException:  # noqa: BLE001
        # NOT just GateFailure. An ordinary RuntimeError/OSError out of
        # collection, scratch output or golden processing would otherwise
        # propagate past BOTH the retargeting bookkeeping and the closing
        # worktree fingerprint — making "the fingerprint always runs" false
        # exactly when the tree is most likely to have been disturbed, and
        # replacing the coded diagnostic with a traceback. It is re-raised
        # after the closing sequence, so nothing is swallowed.
        unexpected = True
    finally:
        disposed = True if tmpdir is None else tmpdir.dispose()

    # A broken binding is a GATE FAILURE, not a cleanup nuisance — and it is
    # recorded as PENDING rather than raised here, because the closing
    # fingerprint below is exactly the evidence this path needs. Raising
    # immediately would skip it on the one route where a repository mutation is
    # most plausible, which is also what makes `dispose()` refuse to delete:
    # whatever the gate wrote through the changed name STAYS on disk, so the
    # fingerprint can see it and report `WORKTREE_DIRTY`. Deleting it here would
    # destroy the only evidence that anything happened.
    if not disposed and failure is None:
        failure = _contract(
            tmpdir.refusal_code,
            "the scratch directory was replaced while the gate was running; the "
            "gate's own writes cannot be accounted for, and nothing was removed "
            "through the changed name.",
        )
    elif not disposed:
        _emit(
            "{0} the scratch was not disposed of cleanly; nothing was removed "
            "through it.".format(tmpdir.refusal_code)
        )

    # UNCONDITIONAL, including after a failure — a failing test is exactly when
    # the tree is most likely to have been disturbed, and skipping the check on
    # that path would make the read-only guarantee true only when nothing went
    # wrong. The original failure still wins: hygiene is reported alongside it,
    # never in place of it.
    try:
        check_worktree_unchanged(status_before, _status(repo))
    except GateFailure as hygiene:
        if failure is None:
            failure = hygiene
        else:
            _report("{0} {1}".format(hygiene.code, hygiene.message))
    except BaseException:  # noqa: BLE001
        # The CLOSING call is inside the boundary as well. `_status()` shells out
        # to git and hashes files; an unexpected exception there escaped `main()`
        # with its own exit semantics, so a `SystemExit(0)` from the very last
        # step exited GREEN after everything else had passed.
        unexpected = True


    if unexpected:
        # NORMALIZED, not re-raised. `main()` catches only `GateFailure`, so
        # re-raising handed the process the exception's own exit semantics — and
        # `SystemExit(0)` from anywhere inside the gate then exits GREEN. The
        # message deliberately inspects nothing about the object: reading even
        # `type(exc).__name__` can run foreign code through a metaclass hook,
        # which is the exact route four earlier rounds closed on the diagnostic
        # path.
        raise _invalid(
            "GATE_UNEXPECTED_ERROR",
            "the gate raised a non-gate exception; the run is not valid",
        )
    if failure is not None:
        raise failure
    return 0


def make_scratch_dir(repo):
    """Scratch space, PROVEN to be outside the repository.

    `tempfile.mkdtemp()` honours `TMPDIR`, so with `TMPDIR` pointing at the
    worktree the gate's scratch is created INSIDE the repository and removed
    again before the closing fingerprint — leaving the before/after comparison
    identical while the gate really did write into the tree. Reproduced:
    `inside_repo=True`, `fingerprint_equal_after_cleanup=True`.

    "Writes only outside the repository" is the structural invariant that the
    best-effort worktree fingerprint leans on, so it has to be ENFORCED rather
    than asserted in a comment. Removed on every exit, including a failing one,
    so a red run leaves no trail.

    The path RETURNED is the resolved one that was checked, not the spelling
    `mkdtemp()` happened to produce. Verifying one name and then writing through
    another is its own defect: with `TMPDIR` a symlink, the checked path and the
    used path are different objects, and only the checked one was ever proven to
    be outside the tree.
    """
    candidate = tempfile.mkdtemp(prefix="wave-gate-")
    # The identity of what we JUST CREATED, captured before anything else touches
    # the name. Containment answers "is this directory outside the repo"; it does
    # NOT answer "is this the directory I made". Without that second question, an
    # unrelated directory swapped onto the candidate name is accepted, written
    # into, and then recursively deleted by `dispose()` — destroying real files
    # that were never ours.
    created = os.stat(candidate)
    resolved = os.path.realpath(candidate)
    # Open FIRST, validate the OPENED OBJECT second. Validating a path and then
    # opening it leaves a window in which the parent is retargeted between the
    # two, so the descriptor lands on the replacement — and every later stat/fstat
    # comparison then agrees, because both sides name the replacement. Whatever we
    # end up holding is what gets judged.
    fd = os.open(resolved, os.O_RDONLY | _O_DIRECTORY)
    try:
        _refuse_scratch_created_here(fd, created)
        _refuse_scratch_inside_repo(fd, repo)
        scratch = _ScratchDir(resolved, fd, repo, _probe_dotdot_at(fd))
    except BaseException:
        # NEVER `shutil.rmtree(candidate)` here. On this path the gate has just
        # decided the candidate is not usable, and the pathname is exactly what it
        # distrusts — a sibling that moves a tracked directory onto that name turns
        # cleanup into deletion of the worktree. Discard through the descriptor,
        # which can only name the directory actually opened, and never leak it.
        _discard_scratch_at(fd)
        raise
    return scratch


_O_DIRECTORY = getattr(os, "O_DIRECTORY", 0)
_O_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)
_DOTDOT_PROBE = ".wave-gate-dotdot-probe"
# A TAG, not a bool. `bool` is a subclass of `int` in Python, so a `True`
# calibration would satisfy an `isinstance(..., int)` errno comparison AND equal
# `errno.EPERM` (1) — an unrelated EPERM would then read as the calibrated unlink
# signal and prove a removal that never happened. The two outcomes are different
# kinds of answer, so they get different types.
_DOTDOT_SURVIVES = object()


class _ScratchDir(object):
    """The verified scratch directory — an OBJECT, re-proved on every use.

    A pathname is not a directory. `make_scratch_dir` proves that a path denotes
    a directory outside the worktree, but the binding between name and object is
    not stable: a concurrent process running as the same user can rename the
    resolved parent and leave a symlink to the repository in its place, after
    which the identical string denotes a directory INSIDE the tree. The write
    then lands in the worktree and cleanup removes it again, leaving the closing
    fingerprint unchanged — a green gate over a tree the gate itself wrote into.

    Holding an open descriptor keeps hold of the object that was actually
    verified. `__fspath__` is the single funnel through which this value becomes
    a string, so every existing `os.path.join(...)` and `shutil.rmtree(...)` call
    re-asserts the binding without any of them being rewritten — an invariant at
    one chokepoint rather than a list of hardened call sites. Writes the gate
    performs itself additionally go through `fd`, which cannot be redirected at
    all.
    """

    __slots__ = ("_path", "fd", "_repo", "_dotdot", "_owned", "refusal_code")

    def __init__(self, path, fd, repo, dotdot):
        self._path = path
        self.fd = fd
        self._repo = repo
        self._dotdot = dotdot
        # Every entry the gate creates, in creation order. Disposal removes THESE
        # and nothing else. A recursive delete of whatever happens to be present
        # destroys anything a concurrent process moved in — reproduced: an
        # unrelated subtree containing `precious.txt` moved into a valid scratch
        # was deleted while `dispose()` returned True. Owning what you delete is
        # the invariant; enumerating the filenames in the cleanup code would just
        # be the same list written twice.
        self._owned = []
        self.refusal_code = "SCRATCH_RETARGETED"

    def _binding_holds(self):
        """True when the name still denotes the held directory AND it is outside."""
        try:
            self.__fspath__()
            _refuse_scratch_inside_repo(self.fd, self._repo)
        except GateFailure:
            return False
        return True

    def __fspath__(self):
        try:
            named = os.stat(self._path)
        except OSError as exc:
            raise _contract(
                "SCRATCH_RETARGETED",
                "the scratch directory {0} is no longer reachable by name ({1}); "
                "it was replaced while the gate was running.".format(self._path, exc),
            )
        held = os.fstat(self.fd)
        if (named.st_dev, named.st_ino) != (held.st_dev, held.st_ino):
            raise _contract(
                "SCRATCH_RETARGETED",
                "the scratch directory {0} now denotes a different directory than "
                "the one that passed the containment check; it was replaced while "
                "the gate was running.".format(self._path),
            )
        return self._path

    def __str__(self):
        return self.__fspath__()

    def mkdir_owned(self, name):
        """Create a subdirectory the gate owns, relative to the held descriptor.

        `os.mkdir` already fails if the name exists, so creation IS the proof of
        ownership — and the record is made only after it succeeds.
        """
        os.mkdir(name, 0o700, dir_fd=self.fd)
        self._owned.append(name)
        return os.open(name, os.O_RDONLY | _O_DIRECTORY | _O_NOFOLLOW, dir_fd=self.fd)

    def own(self, relpath):
        """Record an entry the gate created below this scratch."""
        self._owned.append(relpath)

    def open_for_write(self, name):
        """Create a file the gate OWNS — exclusively, or not at all.

        `O_CREAT|O_TRUNC` without `O_EXCL` does not create, it *takes over*: a file
        a sibling already placed at this name is truncated, recorded as ours, and
        then deleted at disposal. Reproduced: a foreign `collected.txt` was
        overwritten, `dispose()` returned True and removed it, and the worktree
        fingerprint never saw a thing.

        `O_EXCL` makes creation the proof of ownership, and `O_NOFOLLOW` stops the
        name resolving through a symlink somebody else planted. The record is made
        only after the exclusive create succeeds.
        """
        if os.open in os.supports_dir_fd:
            handle = os.open(
                name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | _O_NOFOLLOW,
                0o600,
                dir_fd=self.fd,
            )
            self._owned.append(name)
            return os.fdopen(handle, "w")
        stream = open(os.path.join(self, name), "x")
        self._owned.append(name)
        return stream

    def dispose(self):
        """Remove the scratch, or remove nothing at all — and never lie about it.

        Returns True only when the directory was disposed of cleanly. False means
        the binding broke, and the caller turns that into a gate failure.

        The destructive step is BRACKETED by identity+containment checks rather
        than preceded by one. A single check before deleting is not enough: a
        sibling can rename the held directory into the worktree after the check,
        and fd-relative deletion then erases the generated files from their new
        in-repo location. Checking again afterwards cannot un-delete them, but it
        does ensure the gate goes RED instead of exiting 0 over it — and the
        check BEFORE means that in the ordinary version of that race nothing is
        deleted at all, because the directory is already inside the repo when we
        look.

        The final `rmdir` is itself descriptor-relative, against the parent
        derived from the held descriptor AT DISPOSAL TIME, and the entry it
        removes is the one that IS this directory — looked up by inode in the
        live parent rather than taken from the stored path. A remembered
        basename is just another stale name: move the scratch to a new name,
        leave a symlink at the old path and an empty directory of the old name
        beside it, and identity checks pass (`os.stat` follows the symlink) while
        the remembered name denotes something else entirely — `..` from the
        directory we hold is its parent now, so there is no cached handle to go
        stale. NO destructive operation in this class resolves a pathname. A failing `rmdir` is a signal, not noise: it means the directory
        is no longer where it was, or is no longer empty. Swallowing it and returning True would
        leave an empty directory in the worktree that git does not track, so the
        closing fingerprint would match and the gate would pass — exactly the
        outcome the retargeting rules exist to prevent.
        """
        try:
            try:
                return self._dispose()
            finally:
                # INSIDE the guard. A `finally` placed outside `except` runs
                # after the handler has completed, so anything it raises escapes
                # anyway — measured: `ESCAPED the 'total' boundary: RuntimeError`.
                # Closing a descriptor is exactly the kind of operation an
                # instrumented environment or a signal can make throw.
                self._close()
        except BaseException:
            # TOTAL BY CONSTRUCTION. `dispose()` runs in `execute()`'s `finally`,
            # so ANY exception escaping here replaces the pending GateFailure with
            # an uncoded traceback AND skips the closing worktree fingerprint —
            # the gate loses both its diagnostic and its evidence. This is the
            # third time an escape from the cleanup path has been found, so the
            # guarantee is placed at the boundary once instead of being chased
            # call by call: no path out of this method raises, and anything
            # unexpected reads as "not disposed", which is the fail-closed answer.
            return False

    def _dispose(self):
        if not self._binding_holds():
            return False
        try:
            _remove_owned(self.fd, self._owned)
        except _ForeignEntry:
            # Classified here, not left as retargeting: a foreign entry NESTED in
            # an owned directory never reaches the top-level listing below,
            # because its parent's `rmdir` fails first.
            self.refusal_code = "SCRATCH_FOREIGN_ENTRIES"
            return False
        except OSError:
            return False
        # Anything still here was not created by the gate. Refuse rather than
        # delete it, and say so with its own code — reporting it as retargeting
        # would name the wrong cause.
        try:
            leftover = os.listdir(self.fd)
        except OSError:
            return False
        if leftover:
            self.refusal_code = "SCRATCH_FOREIGN_ENTRIES"
            return False
        if not self._binding_holds():
            return False
        # The parent is derived HERE, from the descriptor we hold, not cached at
        # construction. A cached handle goes stale the moment anything renames the
        # original parent — `..` from the held directory is its parent NOW, so
        # there is no window in which the two can disagree and no stale state to
        # reason about.
        try:
            parent_fd = os.open("..", os.O_RDONLY | _O_DIRECTORY, dir_fd=self.fd)
        except OSError:
            return False
        try:
            try:
                held = os.fstat(self.fd)
                name = _entry_naming(parent_fd, held)
            except OSError:
                return False
            if name is None:
                return False
            try:
                os.rmdir(name, dir_fd=parent_fd)
            except OSError:
                return False
            return _removal_proved(parent_fd, self.fd, held, self._dotdot)
        finally:
            _close_quietly(parent_fd)

    def _close(self):
        try:
            os.close(self.fd)
        except OSError:
            pass


def _close_quietly(fd):
    try:
        os.close(fd)
    except OSError:
        pass


def _entry_naming(parent_fd, held):
    """The name in `parent_fd` that denotes `held`, or None.

    Compared without following symlinks — a symlink pointing AT the directory is
    not the directory, and removing it would leave the real one behind while
    reporting success. Re-proved immediately before use by the caller's `rmdir`,
    which can only ever remove an empty directory.
    """
    for entry in os.scandir(parent_fd):
        try:
            st = entry.stat(follow_symlinks=False)
        except OSError:
            continue
        if (st.st_dev, st.st_ino) == (held.st_dev, held.st_ino):
            return entry.name
    return None


def _probe_dotdot_at(fd):
    """Measure what `..` does for a REMOVED directory on THIS filesystem.

    `_removal_proved` reads `..` as evidence, and how it behaves after a removal
    is platform-specific: on macOS it still opens and names the old parent, while
    a system that invalidates the lookup would report `OSError` for a perfectly
    ordinary successful removal. Assuming either way is how a gate ends up
    permanently red on the deployment platform, or permanently credulous on the
    development one. So it is measured — inside the scratch directory that has
    just passed containment, using a directory the gate creates and removes
    itself.

    Returns `_DOTDOT_SURVIVES` when `..` still resolves after removal, or the SPECIFIC errno
    this filesystem reports when it does not, or None when the probe could not
    run — which `_removal_proved` treats as "cannot interpret" and fails closed
    on.
    """
    try:
        os.mkdir(_DOTDOT_PROBE, dir_fd=fd)
    except OSError:
        return None
    probe = None
    try:
        probe = os.open(_DOTDOT_PROBE, os.O_RDONLY | _O_DIRECTORY, dir_fd=fd)
        os.rmdir(_DOTDOT_PROBE, dir_fd=fd)
    except OSError:
        try:
            os.rmdir(_DOTDOT_PROBE, dir_fd=fd)
        except OSError:
            pass
        if probe is not None:
            _close_quietly(probe)
        return None
    try:
        _close_quietly(os.open("..", os.O_RDONLY | _O_DIRECTORY, dir_fd=probe))
        return _DOTDOT_SURVIVES
    except OSError as exc:
        # The SPECIFIC errno this filesystem reports for an unlinked directory.
        # Returning a bare False would later accept any `OSError` at all —
        # `EACCES`, `EMFILE` — as the unlink signal, which is a different event
        # entirely and would turn an unrelated failure into "cleanly disposed".
        return exc.errno
    finally:
        _close_quietly(probe)


def _removal_proved(parent_fd, fd, held, dotdot_survives):
    """After the fact, prove the directory we HELD is the one that went away.

    No pre-check can be atomic with `rmdir` — POSIX has no remove-by-descriptor —
    so every guard placed *before* the call leaves a window, and closing one
    window has only ever revealed the next. This asserts the OUTCOME instead:
    whatever interleaving occurred, the gate either proves the held directory is
    gone from the parent it removed from, or fails closed.

    Two observations, both measured rather than assumed. macOS does NOT zero
    `st_nlink` for an open descriptor on a removed directory (measured:
    `nlink after it was removed : 2`), so a link-count test would silently agree
    with every case. What does discriminate:

    * the parent no longer lists our inode — measured `None` after a correct
      removal, and the entry's name after removing something else;
    * `..` from the held descriptor still names that parent, so a directory
      moved elsewhere mid-race is caught. `dotdot_survives` records what this
      filesystem actually does after a removal (see `_probe_dotdot_at`) — either
      the `_DOTDOT_SURVIVES` tag, or the exact errno it reports. An unreadable `..` is NOT proof of
      unlinking: it counts only when the errno MATCHES the calibrated one, so an
      `EACCES` or `EMFILE` from an unrelated cause cannot masquerade as the
      unlink signal.
    """
    if _entry_naming(parent_fd, held) is not None:
        return False                      # still linked here: we removed something else
    try:
        up = os.open("..", os.O_RDONLY | _O_DIRECTORY, dir_fd=fd)
    except OSError as exc:
        # Proof ONLY when this is the exact error the calibration observed for an
        # unlinked directory. Any other errno is an unrelated lookup failure —
        # accepting it would report a clean disposal on the strength of a
        # permission or descriptor-exhaustion error.
        # `type(...) is int`, deliberately not `isinstance`: `isinstance(True,
        # int)` is True, so a survives-calibration would match `errno.EPERM`.
        return type(dotdot_survives) is int and exc.errno == dotdot_survives
    try:
        if dotdot_survives is not _DOTDOT_SURVIVES:
            return False                  # `..` should not have opened here: unproven
        return os.fstat(up).st_ino == os.fstat(parent_fd).st_ino
    finally:
        _close_quietly(up)


def _refuse_symlinked_ancestor(repo, relpath, code, what, make=None):
    """Refuse a declared path reached through a symlinked DIRECTORY.

    Checking only the final component is not enough: replacing `golden_xml`
    itself (or a manifest's directory) with a symlink to a tree of perfectly
    ordinary regular files passes a leaf-only check, and the gate then validates
    files that are not at their declared Git paths at all.
    """
    walked = repo
    for part in relpath.split("/"):
        walked = os.path.join(walked, part)
        if os.path.islink(walked):
            # The FAILURE CLASS is the caller's, not this helper's. The golden
            # tree reports its own diagnostics as executed-validation failures
            # (status 1) while manifest reading reports contract failures
            # (status 2); hard-coding one here would hand machine consumers the
            # wrong category for the same code they already know.
            raise (make or _contract)(
                code,
                "{0} is reached through a symlink at {1}; {2} must be at their "
                "declared paths".format(relpath, os.path.relpath(walked, repo), what),
            )


def _refuse_scratch_created_here(fd, created):
    """Refuse anything that is not the directory `mkdtemp()` just created.

    `mkdtemp()` returns a NAME. Between its return and the `os.open()` that
    follows, a process running as the same user can replace the directory at that
    name with one of its own — and containment cannot tell the difference,
    because an attacker's directory outside the repository passes that check
    perfectly well. The gate would then treat somebody else's files as its
    scratch space and `dispose()` would recursively delete them.

    Two independent refusals, because either alone is weak:

    * identity — the opened inode must be the one observed immediately after
      creation;
    * shape — `mkdtemp()` creates an EMPTY directory with mode 0700 owned by us,
      so anything holding entries, or with looser permissions, is not it. This is
      what stops the swapped-directory-full-of-real-files case even if the
      identity observation itself were raced.
    """
    try:
        here = os.fstat(fd)
    except OSError as exc:
        raise _contract(
            "SCRATCH_NOT_OURS",
            "cannot stat the opened scratch directory ({0}).".format(exc),
        )
    if (here.st_dev, here.st_ino) != (created.st_dev, created.st_ino):
        raise _contract(
            "SCRATCH_NOT_OURS",
            "the scratch directory was replaced between creation and use; the "
            "gate refuses to write into, or delete, a directory it did not make.",
        )
    if stat.S_IMODE(here.st_mode) != 0o700 or here.st_uid != os.geteuid():
        raise _contract(
            "SCRATCH_NOT_OURS",
            "the scratch directory does not have the mode and ownership "
            "`mkdtemp()` creates (mode={0:o}, uid={1}).".format(
                stat.S_IMODE(here.st_mode), here.st_uid
            ),
        )
    try:
        entries = os.listdir(fd)
    except OSError as exc:
        raise _contract(
            "SCRATCH_NOT_OURS",
            "cannot read the opened scratch directory ({0}).".format(exc),
        )
    if entries:
        raise _contract(
            "SCRATCH_NOT_OURS",
            "the scratch directory is not empty ({0} entries); `mkdtemp()` "
            "creates an empty one, so this is somebody else's "
            "directory.".format(len(entries)),
        )


def _discard_scratch_at(fd):
    """Remove a freshly created, still-empty scratch THROUGH its descriptor."""
    try:
        parent = os.open("..", os.O_RDONLY | _O_DIRECTORY, dir_fd=fd)
    except OSError:
        _close_quietly(fd)
        return
    try:
        name = _entry_naming(parent, os.fstat(fd))
        if name is not None:
            os.rmdir(name, dir_fd=parent)
    except OSError:
        pass
    finally:
        _close_quietly(parent)
        _close_quietly(fd)


def _refuse_scratch_inside_repo(fd, repo):
    """Decide containment for the OPENED DIRECTORY, walking real parents.

    Two things this deliberately does not do. It does not compare path strings:
    on a case-insensitive filesystem `realpath()` preserves the spelling it was
    given, so `TMPDIR=/users/…/repo` against `/Users/…/repo` passes a lexical
    prefix test while landing physically inside the worktree (measured:
    `lexical check would refuse: False`, `PHYSICALLY inside the repo: True`).
    And it does not walk `os.path.dirname()`: that walks the NAME, which a
    concurrent same-user process can retarget. `..` opened relative to a
    directory descriptor is the real parent in the real tree, so this climbs the
    filesystem itself.

    A directory that cannot be stat'd is not evidence of safety, so it fails
    closed as `SCRATCH_CONTAINMENT_UNPROVEN`.
    """
    try:
        root = os.stat(repo)
    except OSError as exc:
        raise _contract(
            "SCRATCH_CONTAINMENT_UNPROVEN",
            "cannot stat the repository root {0} ({1}), so the scratch directory "
            "cannot be proven to be outside it.".format(repo, exc),
        )
    current = os.dup(fd)
    try:
        while True:
            try:
                here = os.fstat(current)
            except OSError as exc:
                raise _contract(
                    "SCRATCH_CONTAINMENT_UNPROVEN",
                    "cannot stat a parent of the scratch directory ({0}) while "
                    "checking that it lies outside the repository.".format(exc),
                )
            if (here.st_dev, here.st_ino) == (root.st_dev, root.st_ino):
                raise _contract(
                    "SCRATCH_INSIDE_REPO",
                    "the scratch directory resolves inside the repository {0}; "
                    "the gate must not write into the tree it is validating. "
                    "Point TMPDIR somewhere outside the repository and "
                    "re-run.".format(repo),
                )
            try:
                parent = os.open("..", os.O_RDONLY | _O_DIRECTORY, dir_fd=current)
            except OSError as exc:
                raise _contract(
                    "SCRATCH_CONTAINMENT_UNPROVEN",
                    "cannot open the parent of the scratch directory ({0}) while "
                    "checking that it lies outside the repository.".format(exc),
                )
            above = os.fstat(parent)
            if (above.st_dev, above.st_ino) == (here.st_dev, here.st_ino):
                os.close(parent)          # `..` of the root is the root: done.
                return
            os.close(current)
            current = parent
    finally:
        try:
            os.close(current)
        except OSError:
            pass


class _ForeignEntry(Exception):
    """Something the gate did not create is in the way of its own cleanup."""


def _remove_owned(dirfd, owned):
    """Remove exactly the entries the gate created, deepest first.

    Never a recursive sweep of whatever is present, and never through a
    multi-component NAME: `unlink("render-1/request-1.json")` follows an
    intermediate `render-1` that a sibling replaced with a symlink, and deletes
    the target's file instead. Every component is opened with `O_NOFOLLOW` from
    its parent's descriptor, so a replaced component is refused rather than
    traversed.
    """
    for parts in sorted((r.split("/") for r in owned), key=len, reverse=True):
        _remove_owned_entry(dirfd, parts)


def _refuse_unbound_child(parent_fd, name, child_fd):
    """`name` in `parent_fd` must still BE the directory `child_fd` holds."""
    try:
        named = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except OSError:
        raise _ForeignEntry(name)
    held = os.fstat(child_fd)
    if (named.st_dev, named.st_ino) != (held.st_dev, held.st_ino):
        raise _ForeignEntry(name)


def _remove_owned_entry(dirfd, parts):
    if len(parts) > 1:
        try:
            child = os.open(
                parts[0], os.O_RDONLY | _O_DIRECTORY | _O_NOFOLLOW, dir_fd=dirfd
            )
        except FileNotFoundError:
            return
        except OSError:
            # ELOOP/ENOTDIR: the component is no longer the directory we made.
            raise _ForeignEntry(parts[0])
        try:
            # The SAME proof the scratch root already makes, applied uniformly to
            # every directory the gate deletes through — not a new mechanism, a
            # missing application of the existing one. A sibling can rename an
            # opened `render-*` into the worktree between `os.open` and the
            # recursion; deletion would then proceed through a descriptor that is
            # still perfectly valid, leaving an empty in-repo directory git does
            # not track while the root's own binding checks keep passing.
            _refuse_unbound_child(dirfd, parts[0], child)
            _remove_owned_entry(child, parts[1:])
            _refuse_unbound_child(dirfd, parts[0], child)
        finally:
            _close_quietly(child)
        return

    name = parts[0]
    try:
        os.unlink(name, dir_fd=dirfd)
        return
    except FileNotFoundError:
        return
    except IsADirectoryError:
        pass
    except OSError as exc:
        if exc.errno not in (errno.EPERM, errno.EISDIR):
            raise
    try:
        os.rmdir(name, dir_fd=dirfd)
    except FileNotFoundError:
        return
    except OSError as exc:
        if exc.errno in (errno.ENOTEMPTY, errno.EEXIST):
            # An owned directory holding something we did not create.
            raise _ForeignEntry(name)
        raise


def check_worktree_unchanged(before, after):
    """The gate is read-only; prove it rather than assert it in a comment.

    Compares ``git status`` BEFORE and AFTER rather than requiring a clean tree:
    a local ``wave --base`` run legitimately validates uncommitted work, so
    "clean" is the wrong predicate. "Unchanged by me" is the right one.
    """
    if after != before:
        raise _invalid(
            "WORKTREE_DIRTY",
            "the gate changed the worktree; it must be read-only.\n--- before ---\n"
            "{0}\n--- after ---\n{1}".format(before, after),
        )


def build_parser():
    parser = _GateArgumentParser(
        prog="wave_gate.py", description=__doc__.split("\n")[0],
    )
    parser.add_argument(
        "--repo", default=None,
        help="repository to check (default: the repo this script lives in)",
    )
    subparsers = parser.add_subparsers(parser_class=_GateArgumentParser, dest="command", required=True)

    ci = subparsers.add_parser(
        "ci", help="the required CI check: manifests + collection + full non-KB suite",
    )
    ci.add_argument("--github-event", required=True, metavar="PATH",
                    help="path to the GitHub event payload ($GITHUB_EVENT_PATH)")
    ci.add_argument("--event-name", default=None,
                    help="override GITHUB_EVENT_NAME (tests only)")

    wave = subparsers.add_parser(
        "wave", help="the per-wave gate: ci + every active golden, twice, plus the "
                     "#153 fingerprint seam",
    )
    # Not `required=True`: argparse would exit 2 with no diagnostic code, and the
    # coded, explained refusal from `resolve_baseline` is the contract.
    wave.add_argument("--base", default=None, metavar="COMMIT",
                      help="the baseline commit (REQUIRED; never inferred)")
    wave.add_argument("--bootstrap", action="store_true",
                      help="permit the one-time manifest bootstrap exception")
    wave.add_argument("--require-plan-fingerprint", action="store_true",
                      help="fail if the #153 plan-fingerprint provider is not registered")

    manifests = subparsers.add_parser(
        "manifests", help="NOT A GATE: manifest/baseline/transition checks only",
    )
    group = manifests.add_mutually_exclusive_group(required=True)
    group.add_argument("--base", metavar="COMMIT")
    group.add_argument("--github-event", metavar="PATH")
    manifests.add_argument("--event-name", default=None)
    manifests.add_argument("--bootstrap", action="store_true")
    return parser


def exit_status_for(failure):
    """The process exit status for a failure — RECOMPUTED, never trusted.

    The constructor already normalises `status`, but a constructor invariant only
    holds at construction: `GateFailure` is mutable and subclassable, so an
    attribute can be reassigned afterwards, or a subclass can supply its own.
    Enforcing the rule where the value is CONSUMED closes that regardless of what
    happened to the object in between — which is the right place for it, and also
    covers ordinary bugs, not just adversarial ones.

    `type(...) is int` because an int subclass can report equality with 2 while
    holding 0, and `sys.exit()` reads the underlying value. Anything not provably
    the literal 2 becomes 1: a failure never exits green.
    """
    try:
        status = failure.status
    except BaseException:  # noqa: BLE001 - a property could raise
        return 1
    return 2 if (type(status) is int and status == 2) else 1


class _HelpRequested(Exception):
    """`--help`/`--version` completed — a success, not a failure.

    Distinguished from `SystemExit(0)` on purpose: the outermost boundary treats
    every escaping exception as a failure, so help needs a signal of its own
    rather than relying on an exit code the boundary is designed to distrust.
    """


class _UsageError(Exception):
    """argparse rejected the command line — raised INSTEAD of printing."""

    def __init__(self, detail):
        Exception.__init__(self, detail)
        self.detail = detail


class _GateArgumentParser(argparse.ArgumentParser):
    """A parser that never writes to stderr and never exits on its own.

    `--help`/`--version` still exit 0 through `SystemExit`, which is correct and
    is left alone; only the ERROR path is diverted, so the gate's coded
    diagnostic is the first token on stderr rather than trailing argparse's
    usage block.
    """

    def error(self, message):
        raise _UsageError(message)

    def exit(self, status=0, message=None):
        if status:
            raise _UsageError(message.strip() if message else "invalid arguments")
        raise _HelpRequested()


def _report(text):
    """Emit a last-resort diagnostic that can NEVER decide the exit status.

    Every last-resort report runs INSIDE an exception handler, where a raise
    escapes the enclosing `try` entirely — so a sink that can throw hands the
    process an exit status chosen by the failure of REPORTING. That is the same
    class as the five exit-status escapes already closed, appearing in the
    handlers themselves; the `GATE_DIAGNOSTIC_UNRENDERABLE` fallback had it too.
    Sibling sweep: every `_emit` call inside an `except` suite now goes through
    here.
    """
    try:
        _emit(text)
    except BaseException:  # noqa: BLE001
        pass


def main(argv=None):
    try:
        # INSIDE the boundary. argparse exits 2 with usage text and no code, so
        # parsing outside it left a class of invocations — `ci --base ...` — that
        # failed without a stable diagnostic, contradicting the contract that
        # EVERY failure carries one.
        try:
            args = build_parser().parse_args(argv)
        except _HelpRequested:
            return 0
        except _UsageError as exc:
            # argparse's own `error()` writes `usage: ...` to stderr BEFORE it
            # raises, so merely catching SystemExit still leaves the stable code
            # as the second thing a machine consumer sees. The parser is
            # overridden to raise instead of printing, so the coded line is first.
            raise _contract(
                "GATE_USAGE_INVALID",
                "{0}; see --help".format(exc.detail),
            )
        return execute(args)
    except GateFailure as failure:
        # DECIDE FIRST, RENDER SECOND — and that ordering is the whole point.
        #
        # Four successive rounds each found a different dunder by which a
        # hostile provider could reach the exit path through the DIAGNOSTIC:
        # `__name__` via a metaclass, `__repr__` in a formatter, `__eq__` on an
        # int subclass, `__hash__`/`__str__` on a str subclass. Patching them one
        # at a time cannot terminate — there is always another special method.
        #
        # So the exit decision no longer depends on rendering at all. It is taken
        # before any message is built, from a guarded attribute read, a `type()`
        # check and a comparison against a literal — none of which can execute
        # foreign code. Rendering then happens inside its own guard and can fail
        # freely: the worst outcome is a less informative line, never a green
        # exit.
        status = exit_status_for(failure)
        # RENDERING can run foreign code (`__str__`/`__format__`), so it is
        # guarded; REPORTING can fail too, so it goes through a sink that cannot
        # throw. Neither may reach the exit status, which is already decided.
        try:
            rendered = "{0} {1}".format(failure.code, failure.message)
        except BaseException:  # noqa: BLE001
            rendered = (
                "GATE_DIAGNOSTIC_UNRENDERABLE the gate failed and its own "
                "diagnostic could not be rendered; the exit status is "
                "authoritative"
            )
        _report(rendered)
        return status
    except BaseException:  # noqa: BLE001
        # THE invariant, at the only place that can enforce it: the gate decides
        # its own exit status, and an exception NEVER decides it for the gate.
        #
        # Five instances of one defect class were found one at a time, each a
        # statement further out — `dispose()` totality, its `close()`,
        # `execute()`'s re-raise, the closing fingerprint, and finally the
        # OPENING fingerprint and everything else before `execute()`'s inner try.
        # Each fix was correct and none of them terminated the class, because
        # each guarded a REGION while the class is about the process boundary.
        # A sibling sweep of all 51 try blocks is what surfaced the fifth without
        # a sixth review round.
        #
        # Nothing about the object is read — not its type, not its args, not its
        # `__str__` — so no foreign code runs on this path.
        _report(
            "GATE_UNEXPECTED_ERROR the gate raised a non-gate exception; the "
            "run is not valid"
        )
        return 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
