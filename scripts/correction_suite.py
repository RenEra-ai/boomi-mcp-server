#!/usr/bin/env python3
"""The full local non-KB suite for a correction loop — one command, never the serial run.

WHY THIS EXISTS
---------------
A serial ``pytest tests --ignore=tests/kb`` takes about five and a half hours on this suite;
the wave gate's four partitioned children take under two. Corrections kept paying the serial
price because the partitioner was reachable only through ``wave_gate.py wave``, which also
checks baselines and goldens and refuses the revision-freshness deferral. This command is the
partitioned suite on its own:

* collection exactly as the gate collects (`wave_gate.collect_nodes`), then the committed node
  manifest's floor, required nodes and tombstones (`wave_gate.check_collection`) — a required
  test that disappeared fails here with ``PYTEST_NODE_MISSING``, not at the wave;
* the gate's partition over ``SUITE_PROCESSES`` children, balanced by the committed weights;
* the gate's ONE reconciliation (`wave_gate.reconcile_suite`): every child exits 0 with one
  parseable summary, every collected node ran exactly once by identity, the committed skip cap
  holds.

WHAT IT IS NOT
--------------
Not a gate. No baseline, no manifest transition, no goldens, no plan fingerprint: the wave and
the ``Python 3.11 non-KB`` check certify a tree, this does not. It says so on its last line:

* ``FULL LOCAL RUN`` — a clean tree, strict, unchanged during the run. Suite only.
* ``PARTIAL — CERTIFIES NOTHING`` — a dirty tree, or ``BOOMI_DEFER_REVISION_GATES=1``.

THE DEFERRAL
------------
This is the "local pytest correction run" the completion rules allow to carry
``BOOMI_DEFER_REVISION_GATES=1`` (exactly ``1``; any other value is refused, and so is the
variable under ``GITHUB_ACTIONS=true``). Every ``wave_gate.py`` subcommand still refuses it.
Under it the children receive it explicitly — the gate's own child environment never carries
it — and the freshness tests it skips are reconciled by identity against the set the corpus
plugin marks (a deferred ``--collect-only``), so they neither hide inside the skip cap nor
break it. The cap does not move.

The tree must not change while it runs: HEAD and the worktree fingerprint are taken before and
after, and a difference fails the run (``WORKTREE_DIRTY``) — a tree edited mid-run tested no
single tree. ``scripts/regen_baselines.py --install`` takes the same lock exclusively, so it
cannot write under a running suite.

``--durations-out DIR`` writes the per-node durations of a green run (junit ``time``) to a
record outside the repository, keyed to the tree; ``regen_baselines.py --durations`` turns a
record of a clean run into ``tests/fixtures/wave_gate/suite_weights.json``.

Exit codes: 0 green (full or partial — the label says which), 1 an executed check failed,
2 the run could not be meaningful (usage, lock, environment). The first stderr token of a
failure is its code.
"""

from __future__ import annotations

import argparse
import errno
import fcntl
import hashlib
import json
import os
import re
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import wave_gate as gate  # noqa: E402 - the gate is this command's library

DEFER_ENV = gate.DEFER_ENV
CI_ENV = "GITHUB_ACTIONS"
LOCK_NAME = "boomi-baselines.lock"
DURATIONS_KIND = "suite-durations"
PROG = "correction_suite"

_FRESHNESS_LINE = "freshness check skipped: "
_PENDING_LINE = "not yet in the packaged corpus (deferred)"
#: The input identity in a pending line (`tests/_revision_corpus.py` writes
#: "... (deferred): a <kind> input (<digest>) from <node>"); two children reaching the same
#: input report it twice, so the count is by identity, never by line.
_PENDING_IDENTITY = re.compile(r"\(deferred\): a (\S+) input \(([^)]*)\)")


class Refusal(Exception):
    """This command's own refusal: a code, a message, an exit status."""

    def __init__(self, code, message, status=2):
        Exception.__init__(self, message)
        self.code = code
        self.message = message
        self.status = status


def deferral_mode(environ):
    """True for exactly ``"1"``, False when absent; any other value is refused."""
    if DEFER_ENV not in environ:
        return False
    value = environ[DEFER_ENV]
    if value != "1":
        raise Refusal("CORRECTION_DEFERRAL_INVALID",
                      "{0}={1!r}: only the exact value '1' defers; unset it for a strict "
                      "run".format(DEFER_ENV, value))
    if environ.get(CI_ENV) == "true":
        raise Refusal("CORRECTION_UNDER_CI",
                      "{0} is set under CI; the deferral is for local correction runs "
                      "only".format(DEFER_ENV))
    return True


def tree_lock(repo, exclusive):
    """The baseline lock of THIS worktree, in its own git dir.

    The suite holds it SHARED for its whole run; ``regen_baselines.py`` takes it EXCLUSIVE
    to install into the same worktree. Per worktree, not per repository: an install into one
    checkout cannot disturb a suite running in another. Non-blocking: a held lock is a
    refusal, never a wait. Returns the open file, which keeps the lock until it is closed or
    the process exits.
    """
    common = subprocess.run(["git", "rev-parse", "--absolute-git-dir"],
                            cwd=str(repo), capture_output=True, text=True)
    if common.returncode != 0 or not common.stdout.strip():
        raise Refusal("CORRECTION_LOCK_UNAVAILABLE",
                      "cannot locate the git dir: {0}".format(common.stderr.strip()))
    path = os.path.join(common.stdout.strip(), LOCK_NAME)
    handle = open(path, "a+")
    try:
        fcntl.flock(handle, (fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH) | fcntl.LOCK_NB)
    except OSError as exc:
        handle.close()
        if exc.errno in (errno.EAGAIN, errno.EACCES, errno.EWOULDBLOCK):
            raise Refusal("CORRECTION_LOCK_HELD",
                          "{0} is held {1}: {2}".format(
                              path, "by a running suite or install" if exclusive
                              else "by a baseline install", "retry when it is released"))
        raise
    return handle


def _head(repo):
    return gate._git(repo, "rev-parse", "HEAD").stdout.strip()


def _dirty(repo):
    proc = subprocess.run(["git", "status", "--porcelain", "--untracked-files=normal"],
                          cwd=str(repo), capture_output=True)
    gate._refuse_unreadable(proc, "git status")
    return bool(proc.stdout.strip())


def deferred_nodes(repo):
    """The node ids the deferral skips, from a deferred ``--collect-only`` of the suite.

    The corpus plugin prints one ``freshness check skipped: <node> (<artifact>)`` line per
    marked test in a deferred run's summary (`tests/test_revision_freshness_deferral.py`
    pins that it lists exactly the tests the deferral skips).
    """
    env = gate._pytest_env()
    env[DEFER_ENV] = "1"
    proc = subprocess.run(gate._pytest_argv("--collect-only", "-q"), cwd=str(repo),
                          capture_output=True, text=True, env=env)
    if proc.returncode != 0:
        tail = "\n".join((proc.stdout + proc.stderr).splitlines()[-30:])
        raise gate._invalid("PYTEST_COLLECTION_FAILED",
                            "the deferred collection exited {0}:\n{1}".format(proc.returncode, tail))
    marked = []
    for line in proc.stdout.splitlines():
        text = line.strip()
        if not text.startswith(_FRESHNESS_LINE):
            continue
        node, sep, _artifact = text[len(_FRESHNESS_LINE):].rpartition(" (")
        if not sep or not node:
            raise gate._invalid("PYTEST_COLLECTION_FAILED",
                                "unparseable deferral line: {0!r}".format(text))
        marked.append(node)
    if len(set(marked)) != len(marked):
        raise gate._invalid("PYTEST_COLLECTION_DUPLICATE",
                            "the deferral lists a node twice: {0}".format(marked))
    return frozenset(marked)


def durations(children, collected):
    """``{node id: seconds}`` from every child's junit ``time`` (the projection is injective;
    `reconcile_suite` has asserted it before this runs)."""
    by_projection = {gate._junit_projection(node): node for node in collected}
    out = {}
    for child in children:
        for case in gate._junit_testcases(child["report_fd"]):
            node = by_projection.get((case.get("classname") or "", case.get("name") or ""))
            if node is None:
                continue
            try:
                out[node] = float(case.get("time") or 0.0)
            except ValueError:
                out[node] = 0.0
    return out


def require_outside_repo(path, repo, flag):
    """The ONE containment check for every output destination these commands are given.

    Called at entry, before any collection, producer or directory creation, so a bad
    destination neither touches the checkout nor wastes a multi-hour run. ``realpath`` follows
    the part of ``path`` that exists, so a symlink into the checkout is refused too. Returns
    the absolute path."""
    absolute = os.path.abspath(path)
    real = os.path.realpath(absolute)
    real_repo = os.path.realpath(repo)
    if real == real_repo or real.startswith(real_repo + os.sep):
        raise Refusal("SCRATCH_INSIDE_REPO",
                      "{0} {1} is inside the repository; these commands never write "
                      "there".format(flag, absolute))
    return absolute


def durations_name(record):
    """A strict clean record is named for its commit alone; any other run also names its mode
    and tree, so a partial run never replaces the strict record of the same commit."""
    if not record["dirty"] and not record["deferred"]:
        return "suite-durations-{0}.json".format(record["head"][:12])
    mode = "-".join(m for m, on in (("dirty", record["dirty"]), ("deferred", record["deferred"])) if on)
    return "suite-durations-{0}-{1}-{2}.json".format(record["head"][:12], mode, record["tree"][:12])


def write_durations(directory, repo, record):
    """Write the durations record into ``directory`` (created if absent), never inside the
    repository. Returns the path written."""
    directory = require_outside_repo(directory, repo, "--durations-out")
    os.makedirs(directory, exist_ok=True)
    path = os.path.join(directory, durations_name(record))
    tmp = path + ".partial-{0}".format(os.getpid())
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(record, handle, indent=0, sort_keys=True)
        handle.write("\n")
    os.replace(tmp, path)
    return path


def pending_inputs(children):
    """The distinct corpus inputs the children reported pending, from the lines captured
    while streaming (never the bounded tail), one entry per ``(kind, digest)``."""
    found = {}
    for child in children:
        for line in child.get("captured", ()):
            match = _PENDING_IDENTITY.search(line)
            key = match.groups() if match else ("?", line.strip())
            found.setdefault(key, line.strip())
    return [found[key] for key in sorted(found)]


def run(repo, *, defer, durations_out=None, emit=gate._emit):
    """One correction run. Returns ``(verdict line, summary)``; raises on failure."""
    if durations_out is not None:
        require_outside_repo(durations_out, repo, "--durations-out")
    before = gate._status(repo)
    head = _head(repo)
    dirty = _dirty(repo)
    tmpdir = gate.make_scratch_dir(repo)
    failure = None
    try:
        nodes_manifest = gate._load_current(repo)["pytest-nodes"]
        collected = gate.collect_nodes(repo, tmpdir)
        gate.check_collection(nodes_manifest, collected)
        emit("{0}: collection ok ({1} tests, {2} required)".format(
            PROG, len(collected), len(nodes_manifest.active)))
        marked = deferred_nodes(repo) if defer else None
        plan = gate.plan_suite_partition(collected.order, gate.load_suite_weights(repo),
                                         gate.SUITE_PROCESSES)
        env = gate._pytest_env()
        if defer:
            env[DEFER_ENV] = "1"
        children = gate._run_suite_children(repo, plan, tmpdir, env=env, capture=_PENDING_LINE)
        summary = gate.reconcile_suite(nodes_manifest, collected, children, tmpdir,
                                       deferred=marked)
        pending = pending_inputs(children)
        measured = durations(children, collected)
    except BaseException as exc:  # noqa: BLE001 - re-raised after the closing fingerprint
        failure = exc
    finally:
        tmpdir.dispose()

    changed = gate._status(repo) != before
    hygiene = gate._invalid(
        "WORKTREE_DIRTY",
        "the tree changed while the suite ran (HEAD {0}); the run tested no single "
        "tree".format(head))
    if failure is not None:
        if changed:  # reported alongside, never in place of, the original failure
            gate._report("{0} {1}".format(hygiene.code, hygiene.message))
        raise failure
    if changed:
        raise hygiene

    reasons = []
    if dirty:
        reasons.append("dirty tree")
    if defer:
        reasons.append("{0}=1: {1} freshness check(s) skipped, {2} input(s) not yet in the "
                       "packaged corpus".format(DEFER_ENV, summary.get("deferred", 0),
                                                len(pending)))
    for line in pending:
        emit("{0}: pending: {1}".format(PROG, line))
    counts = "{0} passed, {1} skipped (cap {2})".format(
        summary["passed"], summary["skipped"], nodes_manifest.header["maximum_skipped"])
    if reasons:
        verdict = ("{0}: PARTIAL — CERTIFIES NOTHING ({1}). {2} on HEAD {3}.".format(
            PROG, "; ".join(reasons), counts, head))
    else:
        verdict = ("{0}: FULL LOCAL RUN — {1} on clean HEAD {2}. Suite only: not the wave "
                   "and not a landing check.".format(PROG, counts, head))

    if durations_out is not None:
        record = {
            "kind": DURATIONS_KIND, "schema_version": 1, "head": head, "dirty": dirty,
            "deferred": bool(defer), "processes": gate.SUITE_PROCESSES,
            "tree": hashlib.sha256(before.encode("utf-8", "surrogateescape")).hexdigest(),
            "durations": {node: round(seconds, 3) for node, seconds in sorted(measured.items())},
        }
        emit("{0}: durations written to {1}".format(PROG, write_durations(durations_out, repo, record)))
    return verdict, summary


def build_parser():
    parser = argparse.ArgumentParser(prog="correction_suite.py", description=__doc__.split("\n")[0])
    parser.add_argument("--repo", default=None, help="repository (default: the one holding this script)")
    parser.add_argument("--durations-out", metavar="DIR", default=None,
                        help="write the run's per-node durations record here (outside the repo)")
    return parser


def main(argv=None):
    try:
        args = build_parser().parse_args(argv)
    except SystemExit as exc:
        return 0 if exc.code in (0, None) else 2
    try:
        defer = deferral_mode(os.environ)
        repo = gate._repo_root(args.repo)
        lock = tree_lock(repo, exclusive=False)
        try:
            verdict, _summary = run(repo, defer=defer, durations_out=args.durations_out)
        finally:
            lock.close()
        gate._emit(verdict)
        return 0
    except Refusal as refusal:
        gate._report("{0} {1}".format(refusal.code, refusal.message))
        return refusal.status
    except gate.GateFailure as failure:
        gate._report("{0} {1}".format(failure.code, failure.message))
        gate._emit("{0}: FAILED — see the code above.".format(PROG))
        return gate.exit_status_for(failure)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
