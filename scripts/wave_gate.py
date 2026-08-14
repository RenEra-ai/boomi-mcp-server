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
import base64
import json
import os
import re
import shutil
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


# --------------------------------------------------------------------------
# Failure plumbing
# --------------------------------------------------------------------------

class GateFailure(Exception):
    """A gate failure carrying its stable diagnostic code and exit status."""

    def __init__(self, code, message, status):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status


def _contract(code, message):
    """A failure that prevented validation from being meaningful (exit 2)."""
    return GateFailure(code, message, 2)


def _invalid(code, message):
    """A failure of an executed validation (exit 1)."""
    return GateFailure(code, message, 1)


# --------------------------------------------------------------------------
# git helpers
# --------------------------------------------------------------------------

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
    return sha


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
    proc = _git(repo, "log", "--max-count=1", "--format=%H", sha, "--", path)
    return bool(proc.stdout.strip())



def _status(repo):
    return _git(repo, "status", "--porcelain", "--untracked-files=normal").stdout


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
    if header_obj["schema_version"] != SCHEMA_VERSION:
        raise bad("line 1: schema_version must be {0}".format(SCHEMA_VERSION))
    for floor_field in ("minimum_active", "minimum_collected", "maximum_skipped"):
        if floor_field not in spec["header"]:
            continue
        value = header_obj[floor_field]
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise bad("line 1: {0} must be a non-negative integer".format(floor_field))
    if not _SHA_RE.match(str(header_obj["bootstrap_base"])):
        raise bad("line 1: bootstrap_base must be a 40-char lowercase hex sha")

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
    if len(active) < header_obj["minimum_active"]:
        raise _contract(
            "MANIFEST_FLOOR_INVALID",
            "{0}: {1} active rows is below the committed floor {2}".format(
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

    Returns ``(appended_active, newly_tombstoned)`` so the caller can check the
    floor arithmetic.  Every failure is ``MANIFEST_TRANSITION_ILLEGAL``.
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

    appended = current.rows[len(base.rows):]
    for row in appended:
        if row["state"] != "active":
            raise bad(
                "appended row {0} must start 'active'; a row cannot be born "
                "tombstoned".format(row["id"])
            )

    _check_floor_arithmetic(base, current, len(appended), newly_tombstoned, name)
    return len(appended), newly_tombstoned


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
    return {"sha": resolved, "kind": "push", "target": resolved}


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
    return {"sha": bases[0], "kind": "pull_request", "target": target}


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
    # Ask the target branch first, when there is one. A PR branched before the
    # manifests landed keeps a merge base that predates them FOREVER, so the
    # merge base alone will always say "absent" and always look like a fresh
    # introduction — even long after the ledger exists on `dev`. The question
    # that actually decides bootstrap is "do these already exist on the branch
    # we are merging into?", and only the target tip can answer it.
    if target is not None and target != baseline:
        landed = sorted(
            spec["path"] for spec in _SCHEMAS.values()
            if _blob_at(repo, target, spec["path"]) is not None
        )
        if landed:
            raise _contract(
                "BOOTSTRAP_NOT_ALLOWED",
                "{0} already exist(s) on the target branch ({1}); a stale merge "
                "base does not make this an introduction. Rebase so the baseline "
                "carries the manifests and the change is validated as a "
                "transition.".format(", ".join(landed), target[:12]),
            )

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
    if declared_base != baseline:
        raise _contract(
            "BOOTSTRAP_NOT_ALLOWED",
            "bootstrap_base {0} does not equal the validated baseline {1}".format(
                declared_base, baseline
            ),
        )
    if require_flag and not flag_given:
        raise _contract(
            "BOOTSTRAP_NOT_ALLOWED",
            "a local run must pass --bootstrap to exercise the one-time bootstrap "
            "exception (CI does not need it: the event's own baseline must equal "
            "the declared bootstrap_base, which it does here)",
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


def _render_env(repo, hashseed):
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
    with open(os.path.join(tmpdir, "collected.txt"), "w") as handle:
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
    request_path = os.path.join(tmpdir, "request-{0}.json".format(hashseed))
    with open(request_path, "w") as handle:
        json.dump(request, handle)

    proc = subprocess.run(
        [sys.executable, os.path.join(repo, GOLDEN_CORPUS), "--render", request_path],
        cwd=str(repo), capture_output=True, text=True,
        env=_render_env(repo, hashseed),
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

    cases = list(provider.cases())
    if not cases:
        raise _invalid(
            "PLAN_FINGERPRINT_MISMATCH", "the registered provider declares no cases"
        )
    for case in cases:
        first = provider.fingerprint(case, **_RELOCATION_A)
        second = provider.fingerprint(case, **_RELOCATION_B)
        if first != second:
            raise _invalid(
                "PLAN_FINGERPRINT_MISMATCH",
                "case {0!r}: fingerprint is not relocatable ({1} != {2})".format(
                    case, first, second
                ),
            )
        mutations = list(provider.mutations(case))
        if not mutations:
            raise _invalid(
                "PLAN_FINGERPRINT_MISMATCH",
                "case {0!r}: no semantic mutations declared; a fingerprint proven "
                "only to be stable is indistinguishable from a constant".format(case),
            )
        for mutation in mutations:
            mutated = provider.fingerprint(case, mutation=mutation, **_RELOCATION_A)
            # Relocatability is a property of EVERY plan, not just the unmutated
            # one. Checking mutations under a single identity would accept a
            # provider that is identity-independent for the base case and
            # account-dependent as soon as anything changes — which is not a
            # relocatable fingerprint, it is one that happens to look relocatable
            # in the one place it was measured.
            relocated = provider.fingerprint(case, mutation=mutation, **_RELOCATION_B)
            if mutated != relocated:
                raise _invalid(
                    "PLAN_FINGERPRINT_MISMATCH",
                    "case {0!r} under mutation {1!r}: fingerprint is not "
                    "relocatable ({2} != {3})".format(case, mutation, mutated, relocated),
                )
            if mutated == first:
                raise _invalid(
                    "PLAN_FINGERPRINT_MISMATCH",
                    "case {0!r}: mutation {1!r} did not change the fingerprint".format(
                        case, mutation
                    ),
                )
    return "checked:{0} case(s)".format(len(cases))


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
            validate_transition(base, current[name], name)
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

    # Scratch space lives OUTSIDE the repository — see check_worktree_unchanged.
    # Removed on every exit, including a failing one, so a red run does not leave
    # a trail of directories behind on a CI runner or a developer's machine.
    tmpdir = tempfile.mkdtemp(prefix="wave-gate-")
    failure = None
    try:
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
            status = run_plan_fingerprint_checks(args.require_plan_fingerprint)
            _emit("wave_gate: plan fingerprint {0}".format(status))
    except GateFailure as exc:
        failure = exc
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

    # UNCONDITIONAL, including after a failure — a failing test is exactly when
    # the tree is most likely to have been disturbed, and skipping the check on
    # that path would make the read-only guarantee true only when nothing went
    # wrong. The original failure still wins: hygiene is reported alongside it,
    # never in place of it.
    try:
        check_worktree_unchanged(status_before, _status(repo))
    except GateFailure as hygiene:
        if failure is None:
            raise
        _emit("{0} {1}".format(hygiene.code, hygiene.message))

    if failure is not None:
        raise failure
    return 0


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
    parser = argparse.ArgumentParser(
        prog="wave_gate.py", description=__doc__.split("\n")[0],
    )
    parser.add_argument(
        "--repo", default=None,
        help="repository to check (default: the repo this script lives in)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

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
    wave.add_argument("--base", required=True, metavar="COMMIT",
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


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return execute(args)
    except GateFailure as failure:
        _emit("{0} {1}".format(failure.code, failure.message))
        return failure.status


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
