#!/usr/bin/env python3
"""Regenerate the derived baselines — once per stable candidate, in one locked command.

WHY THIS EXISTS
---------------
Five committed artifacts are derived from the source, and the suite fails the moment one is
stale: the packaged revision corpus, the two authoring-contract snapshots, the M12.12
reachability inventory (with its markdown tables), the pytest node manifest and the suite
weights. They used to be rebuilt by hand, one producer at a time, after every correction —
derived files appear in 94 of the 258 commits of the 2026-11-26 review range, and a single
corpus harvest takes 66 minutes to five hours. A harvest run on a tree that changed under it
had to be repeated. This command replaces that with one refresh of one frozen candidate:

1. **Freeze.** The candidate is ``HEAD`` of a CLEAN checkout. Every producer runs in a
   detached worktree of exactly that commit, outside the repository, so nothing the producers
   do can touch the checkout and nothing done to the checkout can reach the producers.
2. **Produce, in dependency order** (:data:`PRODUCERS`; why that order, in each entry). A
   producer that writes anything other than its declared outputs fails the run.
3. **Show semantic diffs.** The corpus as decoded inputs (added and removed records by
   entry point), JSON as indented JSON, the node manifest as appended and tombstoned rows.
4. **Stage** every output with its digest in a bundle outside the repository.
5. **Install** (``--install``, or later ``--install-from BUNDLE``) only if the checkout is
   still clean at the candidate commit — the inputs still match — and the staged bytes are
   the ones produced. All outputs or none, under the baseline lock held exclusively, so no
   install lands under a running ``correction_suite.py``.

The node manifest is rebuilt from the ``--base`` commit's manifest plus the candidate's
collection (rows for tests added and removed inside the range never appear) and checked with
the gate's own ``validate_transition`` before it is staged. The weights come only from a
``correction_suite.py --durations-out`` record of a clean, strict run of this very candidate;
without one they are left alone, and the command says so.

``BOOMI_DEFER_REVISION_GATES`` is refused whatever its value, and no producer inherits it:
producers are where deferred freshness debt is paid. Goldens and the golden manifest are not
derived here — their bytes are evidence, and their rows carry judgment (owner, disposition).

Exit codes: 0 done (or nothing to change), 1 a producer or a validation failed, 2 the run
could not be meaningful (usage, lock, dirty checkout, the deferral, inputs changed).
"""

from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import correction_suite  # noqa: E402 - shares the baseline lock
import wave_gate as gate  # noqa: E402

DEFER_ENV = gate.DEFER_ENV
PROG = "regen_baselines"
REGEN_LOCK_NAME = "boomi-regen.lock"
BUNDLE_KIND = "regen-baselines-bundle"
BUNDLE_INDEX = "bundle.json"

CORPUS = "src/boomi_mcp/authoring/revision_corpus_v1.json.gz"
CONTRACT_SNAPSHOT = "tests/fixtures/authoring_contract/process_ir_authoring_v1.contract.json"
SCHEMA_SNAPSHOT = "tests/fixtures/authoring_contract/process_ir_authoring_v1.schema.json"
INVENTORY = "tests/fixtures/m12_12/legacy_reachability_inventory.json"
INVENTORY_DOC = "specs/M12_COMPATIBILITY_INVENTORY.md"

#: Minimum measured seconds for a node to be listed in the weights file; an unlisted node
#: weighs `wave_gate._DEFAULT_NODE_WEIGHT`, which is this value.
WEIGHT_FLOOR = gate._DEFAULT_NODE_WEIGHT


class Refusal(Exception):
    def __init__(self, code, message, status=2):
        Exception.__init__(self, message)
        self.code = code
        self.message = message
        self.status = status


def _fail(code, message):
    return Refusal(code, message, status=1)


# ---------------------------------------------------------------------------
# producers, in dependency order
# ---------------------------------------------------------------------------

def _script(*argv):
    def run(ctx):
        proc = subprocess.run([sys.executable, *argv], cwd=ctx["worktree"], env=ctx["env"],
                              capture_output=True, text=True)
        if proc.returncode != 0:
            tail = "\n".join((proc.stdout + proc.stderr).splitlines()[-40:])
            raise _fail("REGEN_PRODUCER_FAILED",
                        "`{0}` exited {1}:\n{2}".format(" ".join(argv), proc.returncode, tail))
        return proc.stdout.strip().splitlines()[-1:] if proc.stdout.strip() else []
    return run


def produce_nodes(ctx):
    """The node manifest: the base's rows, then one appended active row per newly collected
    node in collection order. Rebuilt from the BASE, never from the working file, so a test
    added and removed within the range has no row at all (a born tombstone is illegal).

    A required node that no longer collects is NOT a regeneration outcome
    (`specs/ENDGAME_VERIFICATION_GATE.md`): it is an explicit ``active → tombstone`` edit by
    the change that deletes the test. So a missing node is tombstoned only when the
    candidate's own committed manifest already tombstones it; any other missing node fails the
    run (``REGEN_NODE_MISSING``) — a module skipped at import time, say, is not a deletion.
    The header is the CANDIDATE's, so a skip cap the candidate tightened stays tightened; only
    the two derived floors are recomputed."""
    def committed(sha):
        raw = gate._git(ctx["repo"], "show", "{0}:{1}".format(sha, gate.NODES_MANIFEST)).stdout
        return gate.parse_manifest(raw.encode("utf-8"), "pytest-nodes")

    base = committed(ctx["base"])
    own = committed(ctx["candidate"])
    retired = {row["node_id"] for row in own.rows if row["state"] == "tombstone"}
    scratch = gate.make_scratch_dir(ctx["worktree"])
    try:
        collected = gate.collect_nodes(ctx["worktree"], scratch)
    finally:
        scratch.dispose()
    missing = sorted(row["node_id"] for row in base.rows
                     if row["state"] == "active" and row["node_id"] not in collected
                     and row["node_id"] not in retired)
    if missing:
        raise _fail("REGEN_NODE_MISSING",
                    "{0} required node(s) no longer collect and the candidate's manifest does "
                    "not tombstone them. Retiring a test is an explicit edit in the change that "
                    "deletes it; a test that merely failed to collect must be fixed. First 10: "
                    "{1}".format(len(missing), missing[:10]))
    rows, known = [], {}
    for row in base.rows:
        row = dict(row)
        known[row["node_id"]] = row
        if row["state"] == "active" and row["node_id"] not in collected:
            row["state"] = "tombstone"  # explicitly retired by the candidate (checked above)
        rows.append(row)
    # A collected node is never flipped above, so a tombstone here is one the BASE recorded.
    revived = sorted(n for n in collected if n in known and known[n]["state"] == "tombstone")
    if revived:
        raise _fail("REGEN_NODE_REVIVED",
                    "{0} collected node(s) were retired by a tombstone at the base; a retired "
                    "identity cannot return. Rename the test. First 10: {1}".format(
                        len(revived), revived[:10]))
    for node in collected.order:
        if node not in known:
            rows.append({"kind": "test", "id": "pytest-{0:06d}".format(len(rows) + 1),
                         "node_id": node, "state": "active"})
    header = dict(own.header)
    header["minimum_active"] = sum(1 for row in rows if row["state"] == "active")
    header["minimum_collected"] = len(collected)
    text = "".join(json.dumps(obj) + "\n" for obj in [header] + rows)
    current = gate.parse_manifest(text.encode("utf-8"), "pytest-nodes")
    gate.validate_transition(base, current, "pytest-nodes")
    with open(os.path.join(ctx["worktree"], gate.NODES_MANIFEST), "w", encoding="utf-8") as handle:
        handle.write(text)
    ctx["collected"] = collected
    appended = len(rows) - len(base.rows)
    tombstoned = sum(1 for was, now in zip(base.rows, rows) if was["state"] != now["state"])
    return ["{0} appended, {1} tombstoned against base {2}".format(appended, tombstoned, ctx["base"][:12])]


def load_durations(path, candidate):
    """A `correction_suite.py --durations-out` record of a clean, strict run of ``candidate``."""
    try:
        with open(path, encoding="utf-8") as handle:
            record = json.load(handle)
    except (OSError, ValueError) as exc:
        raise Refusal("REGEN_DURATIONS_INVALID", "cannot read {0}: {1}".format(path, exc))
    if not isinstance(record, dict) or record.get("kind") != correction_suite.DURATIONS_KIND:
        raise Refusal("REGEN_DURATIONS_INVALID", "{0} is not a suite-durations record".format(path))
    if record.get("head") != candidate or record.get("dirty") is not False \
            or record.get("deferred") is not False:
        raise Refusal("REGEN_DURATIONS_INVALID",
                      "{0} measured head {1} (dirty={2}, deferred={3}); weights come only from "
                      "a clean, strict run of the candidate {4}".format(
                          path, record.get("head"), record.get("dirty"), record.get("deferred"),
                          candidate))
    durations = record.get("durations")
    if not isinstance(durations, dict):
        raise Refusal("REGEN_DURATIONS_INVALID", "{0} has no durations".format(path))
    return durations


def produce_weights(ctx):
    """Every collected node measured at or above :data:`WEIGHT_FLOOR`, two decimals, in the
    committed layout (`json.dumps(indent=0)`, sorted keys)."""
    collected = ctx.get("collected")
    if collected is None:
        raise Refusal("REGEN_USAGE_INVALID", "weights need the nodes producer in the same run")
    weights = {}
    for node, seconds in ctx["durations"].items():
        value = round(float(seconds), 2)
        if node in collected and value >= WEIGHT_FLOOR:
            weights[node] = value
    document = {"kind": gate.SUITE_WEIGHTS_KIND, "schema_version": gate.SCHEMA_VERSION,
                "weights": dict(sorted(weights.items()))}
    text = json.dumps(document, indent=0) + "\n"
    gate.parse_suite_weights(text.encode("utf-8"))
    with open(os.path.join(ctx["worktree"], gate.SUITE_WEIGHTS), "w", encoding="utf-8") as handle:
        handle.write(text)
    return ["{0} weighted nodes, {1:.0f} s".format(len(weights), sum(weights.values()))]


#: ``(name, outputs, run, why it sits here)`` — the order is the dependency order.
PRODUCERS = (
    ("corpus", (CORPUS,), _script("tests/_revision_corpus.py", "--write"),
     "first: the compiler revision hashes it, and the inventory freezes served artifacts "
     "that carry that revision; its harvest runs only the covered #184 modules, none of "
     "which reads another output here"),
    ("snapshots", (CONTRACT_SNAPSHOT, SCHEMA_SNAPSHOT),
     _script("tests/_authoring_contract_snapshots.py", "--write"),
     "pure functions of the source; rendered by the same module the freshness tests use"),
    ("inventory", (INVENTORY, INVENTORY_DOC), _script("scripts/rebaseline_m12_12_inventory.py"),
     "after the corpus: it digests served artifacts whose revisions move with it"),
    ("nodes", (gate.NODES_MANIFEST,), produce_nodes,
     "after every source producer: a parametrized id may be derived from a fixture"),
    ("weights", (gate.SUITE_WEIGHTS,), produce_weights,
     "last: filtered to the nodes the candidate collects"),
)
NAMES = tuple(name for name, _o, _r, _w in PRODUCERS)


# ---------------------------------------------------------------------------
# semantic diffs
# ---------------------------------------------------------------------------

_CORPUS_DIFF = r"""
import collections, json, sys
from boomi_mcp.authoring import revision_corpus as c
def load(path):
    if not path:
        return {}
    return {c.digest(r): r for r in c.parse_corpus(open(path, 'rb').read())}
old, new = load(sys.argv[1]), load(sys.argv[2])
added = sorted(set(new) - set(old)); removed = sorted(set(old) - set(new))
def by_entry(keys, records):
    return dict(collections.Counter(str(records[k].get('entry')) for k in keys))
print(json.dumps({"old": len(old), "new": len(new), "added": len(added), "removed": len(removed),
                  "added_by_entry": by_entry(added, new), "removed_by_entry": by_entry(removed, old),
                  "added_sample": [[k[:16], new[k].get('entry')] for k in added[:10]],
                  "removed_sample": [[k[:16], old[k].get('entry')] for k in removed[:10]]},
                 sort_keys=True))
"""


def _capped(lines, limit):
    lines = list(lines)
    if len(lines) > limit:
        return lines[:limit] + ["... {0} more diff lines".format(len(lines) - limit)]
    return lines


def _text_diff(path, old, new, limit):
    return _capped((line.rstrip("\n") for line in difflib.unified_diff(
        old.splitlines(True), new.splitlines(True), "a/" + path, "b/" + path, n=1)), limit)


def semantic_diff(ctx, path, old_bytes, new_bytes, limit):
    if path == CORPUS:
        with tempfile.TemporaryDirectory() as scratch:
            names = []
            for label, blob in (("old", old_bytes), ("new", new_bytes)):
                name = ""
                if blob is not None:
                    name = os.path.join(scratch, label + ".gz")
                    with open(name, "wb") as handle:
                        handle.write(blob)
                names.append(name)
            proc = subprocess.run([sys.executable, "-c", _CORPUS_DIFF, *names],
                                  cwd=ctx["worktree"], env=ctx["env"], capture_output=True, text=True)
        if proc.returncode != 0:
            raise _fail("REGEN_DIFF_FAILED", "cannot decode the corpus: " + proc.stderr[-2000:])
        summary = json.loads(proc.stdout)
        return ["decoded inputs: {old} -> {new} records, {added} added, {removed} removed".format(**summary),
                "  added by entry: {0}".format(summary["added_by_entry"]),
                "  removed by entry: {0}".format(summary["removed_by_entry"])] + [
                    "  + {0} {1}".format(*row) for row in summary["added_sample"]] + [
                    "  - {0} {1}".format(*row) for row in summary["removed_sample"]]
    old = (old_bytes or b"").decode("utf-8")
    new = new_bytes.decode("utf-8")
    if path == gate.NODES_MANIFEST:
        before = {json.loads(l).get("node_id"): json.loads(l) for l in old.splitlines()[1:]}
        after = [json.loads(l) for l in new.splitlines()[1:]]
        appended = [r["node_id"] for r in after if r["node_id"] not in before]
        retired = [r["node_id"] for r in after if r["node_id"] in before
                   and before[r["node_id"]]["state"] != r["state"]]
        header = _text_diff(path, old.splitlines(True)[0] if old else "", new.splitlines(True)[0], limit)
        return (["{0} rows appended, {1} tombstoned".format(len(appended), len(retired))]
                + ["  + " + n for n in appended[:limit // 2]]
                + ["  † " + n for n in retired[:limit // 2]] + header)
    if path.endswith(".json"):
        def pretty(text):
            return json.dumps(json.loads(text), indent=1, sort_keys=True) + "\n" if text else ""
        return _text_diff(path, pretty(old), pretty(new), limit)
    return _text_diff(path, old, new, limit)


# ---------------------------------------------------------------------------
# freeze, produce, stage, install
# ---------------------------------------------------------------------------

def _lock(repo, name):
    common = gate._git(repo, "rev-parse", "--path-format=absolute", "--git-common-dir").stdout.strip()
    import fcntl

    handle = open(os.path.join(common, name), "a+")
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        handle.close()
        raise Refusal("REGEN_LOCK_HELD", "another regeneration holds {0}".format(name))
    return handle


def _clean_head(repo):
    """``HEAD`` of a clean checkout, or a refusal: the candidate must be a commit."""
    if correction_suite._dirty(repo):
        raise Refusal("REGEN_CHECKOUT_DIRTY",
                      "the checkout has uncommitted or untracked changes; commit the candidate "
                      "first — a regeneration describes one commit")
    return correction_suite._head(repo)


def _producer_env():
    env = dict(os.environ)
    env.pop(DEFER_ENV, None)
    env["PYTHONPATH"] = "src"
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env["PY_COLORS"] = "0"
    return env


def _sha256(blob):
    return hashlib.sha256(blob).hexdigest()


def _show(repo, sha, path):
    proc = subprocess.run(["git", "show", "{0}:{1}".format(sha, path)], cwd=str(repo), capture_output=True)
    return proc.stdout if proc.returncode == 0 else None


def prepare(repo, base, selected, *, durations=None, bundle_dir=None, diff_lines=120,
            keep_worktree=False, emit=gate._emit):
    """Freeze HEAD, run ``selected`` producers on it, show diffs, stage a bundle.

    Returns the bundle directory (``None`` when nothing differs)."""
    candidate = _clean_head(repo)
    base_sha = gate._resolve_commit(repo, base)
    if subprocess.run(["git", "merge-base", "--is-ancestor", base_sha, candidate],
                      cwd=str(repo)).returncode != 0:
        raise Refusal("REGEN_BASE_INVALID", "--base {0} is not an ancestor of the candidate {1}".format(
            base_sha, candidate))
    if bundle_dir is not None:
        bundle_dir = correction_suite.require_outside_repo(bundle_dir, repo, "--bundle")
    holder = tempfile.mkdtemp(prefix="regen-baselines-")
    try:
        correction_suite.require_outside_repo(holder, repo, "the temporary directory")
    except correction_suite.Refusal:
        shutil.rmtree(holder, ignore_errors=True)
        raise
    worktree = os.path.join(holder, "candidate")
    gate._git(repo, "worktree", "add", "--detach", worktree, candidate)
    ctx = {"repo": repo, "worktree": worktree, "base": base_sha, "candidate": candidate,
           "env": _producer_env(), "durations": durations}
    try:
        declared = []
        for name, outputs, produce, why in PRODUCERS:
            if name not in selected:
                continue
            emit("{0}: {1} — {2}".format(PROG, name, why))
            for line in produce(ctx):
                emit("{0}:   {1}".format(PROG, line))
            declared.extend(outputs)
        status = gate._git(worktree, "status", "--porcelain", "--untracked-files=all").stdout
        touched = sorted(line[3:] for line in status.splitlines() if line.strip())
        undeclared = [path for path in touched if path not in declared]
        if undeclared:
            raise _fail("REGEN_UNDECLARED_WRITE",
                        "a producer wrote outside its declared outputs: {0}".format(undeclared))
        outputs = {}
        for path in declared:
            with open(os.path.join(worktree, path), "rb") as handle:
                new = handle.read()
            old = _show(repo, candidate, path)
            if new == old:
                continue
            emit("{0}: --- {1} changed ---".format(PROG, path))
            for line in semantic_diff(ctx, path, old, new, diff_lines):
                emit(line)
            outputs[path] = new
        if not outputs:
            emit("{0}: every selected baseline is already current on {1}".format(PROG, candidate))
            if not keep_worktree:
                ctx["discard_holder"] = True
            return None
        bundle_dir = bundle_dir or os.path.join(holder, "bundle")
        os.makedirs(os.path.join(bundle_dir, "files"), exist_ok=True)
        index = {"kind": BUNDLE_KIND, "schema_version": 1, "candidate": candidate,
                 "base": base_sha, "producers": [n for n in NAMES if n in selected], "outputs": {}}
        for path, blob in sorted(outputs.items()):
            target = os.path.join(bundle_dir, "files", path)
            os.makedirs(os.path.dirname(target), exist_ok=True)
            with open(target, "wb") as handle:
                handle.write(blob)
            index["outputs"][path] = _sha256(blob)
        with open(os.path.join(bundle_dir, BUNDLE_INDEX), "w", encoding="utf-8") as handle:
            json.dump(index, handle, indent=1, sort_keys=True)
            handle.write("\n")
        emit("{0}: staged {1} output(s) for {2} in {3}".format(PROG, len(outputs), candidate, bundle_dir))
        return bundle_dir
    finally:
        if keep_worktree:
            emit("{0}: candidate worktree kept at {1}".format(PROG, worktree))
        else:
            subprocess.run(["git", "worktree", "remove", "--force", worktree], cwd=str(repo),
                           capture_output=True)
            if ctx.get("discard_holder"):
                shutil.rmtree(holder, ignore_errors=True)
            else:
                try:  # a failed run, or one staged to an explicit --bundle, leaves it empty
                    os.rmdir(holder)
                except OSError:
                    pass  # it holds the default bundle


def installable_outputs(producers):
    """The paths a bundle staged by ``producers`` may install: their declared outputs only."""
    if not isinstance(producers, list) or not all(isinstance(n, str) for n in producers):
        raise Refusal("REGEN_BUNDLE_INVALID",
                      "the bundle's producers {0!r} are not a list of names; nothing was "
                      "installed".format(producers))
    names = set(producers)
    return {path for name, outputs, _run, _why in PRODUCERS if name in names for path in outputs}


def install(repo, bundle_dir, emit=gate._emit):
    """Copy a staged bundle into the checkout — only while its inputs still match."""
    try:
        with open(os.path.join(bundle_dir, BUNDLE_INDEX), encoding="utf-8") as handle:
            index = json.load(handle)
    except (OSError, ValueError) as exc:
        raise Refusal("REGEN_BUNDLE_INVALID", "cannot read the bundle index: {0}".format(exc))
    if index.get("kind") != BUNDLE_KIND or not isinstance(index.get("outputs"), dict):
        raise Refusal("REGEN_BUNDLE_INVALID", "{0} is not a regeneration bundle".format(bundle_dir))
    allowed = installable_outputs(index.get("producers"))
    real_repo = os.path.realpath(repo)
    for path in index["outputs"]:
        # The bundle names its destinations, so they are checked against what the named
        # producers declare BEFORE anything is read or written: the digests only prove the
        # bytes match the same index, never that the index is honest.
        if path not in allowed:
            raise Refusal("REGEN_BUNDLE_INVALID",
                          "{0!r} is not an output of the bundle's producers {1}; nothing was "
                          "installed".format(path, index.get("producers")))
        if not os.path.realpath(os.path.join(repo, path)).startswith(real_repo + os.sep):
            raise Refusal("REGEN_BUNDLE_INVALID",
                          "{0!r} resolves outside the checkout; nothing was installed".format(path))
    lock = correction_suite.tree_lock(repo, exclusive=True)
    try:
        head = correction_suite._head(repo)
        dirty = correction_suite._dirty(repo)
        if head != index["candidate"] or dirty:
            raise Refusal("REGEN_INPUTS_CHANGED",
                          "the checkout is {0}{1}, but the bundle was produced from {2}; its "
                          "inputs no longer match, so nothing was installed. Re-run on the "
                          "new candidate.".format(head, " with changes" if dirty else "",
                                                  index["candidate"]))
        staged = {}
        for path, digest in sorted(index["outputs"].items()):
            source = os.path.join(bundle_dir, "files", path)
            with open(source, "rb") as handle:
                blob = handle.read()
            if _sha256(blob) != digest:
                raise Refusal("REGEN_BUNDLE_INVALID", "{0} changed after it was staged".format(path))
            staged[path] = blob
        originals = {path: _show(repo, head, path) for path in staged}
        replaced, temps = [], {}
        try:
            for path, blob in staged.items():
                target = os.path.join(repo, path)
                temps[path] = target + ".regen-partial-{0}".format(os.getpid())
                with open(temps[path], "wb") as handle:
                    handle.write(blob)
                if os.path.exists(target):
                    shutil.copymode(target, temps[path])
            for path in staged:
                os.replace(temps[path], os.path.join(repo, path))
                del temps[path]  # registered until the rename has succeeded
                replaced.append(path)
        except BaseException:
            for path in replaced:  # all or none
                target = os.path.join(repo, path)
                if originals.get(path) is None:
                    os.unlink(target)  # an output HEAD does not have: remove it again
                else:
                    with open(target, "wb") as handle:
                        handle.write(originals[path])
            for temp in temps.values():
                if os.path.exists(temp):
                    os.unlink(temp)
            raise
    finally:
        lock.close()
    for path in sorted(staged):
        emit("{0}: installed {1}".format(PROG, path))
    emit("{0}: {1} output(s) installed on {2}; review and commit them.".format(
        PROG, len(staged), index["candidate"]))
    return sorted(staged)


def install_command(repo, bundle):
    """The runnable command that installs ``bundle`` into ``repo``, derived from every input
    that selects the install: the interpreter (the script is not executable in git), this
    script, the checkout as RESOLVED (``--repo``, always — the command may be pasted from any
    directory, and the script's own checkout need not be the target), and the bundle. Every
    part is shell-quoted. Nothing else in the printed line is free text."""
    return " ".join(shlex.quote(part) for part in (
        sys.executable, os.path.abspath(__file__), "--repo", os.path.abspath(str(repo)),
        "--install-from", os.path.abspath(bundle)))


class _Parser(correction_suite.CodedParser):
    usage_code = "REGEN_USAGE_INVALID"


def build_parser():
    parser = _Parser(prog="regen_baselines.py", description=__doc__.split("\n")[0])
    parser.add_argument("--repo", default=None)
    parser.add_argument("--base", help="the dev commit the slice started from (node manifest base)")
    parser.add_argument("--only", default=None,
                        help="comma-separated producers to run ({0}); default: all but weights, "
                             "plus weights when --durations is given".format(",".join(NAMES)))
    parser.add_argument("--durations", metavar="FILE", default=None,
                        help="a correction_suite.py --durations-out record of this candidate")
    parser.add_argument("--bundle", metavar="DIR", default=None, help="where to stage the outputs")
    parser.add_argument("--install", action="store_true", help="install right after staging")
    parser.add_argument("--install-from", metavar="DIR", default=None,
                        help="install a bundle staged earlier (runs no producer)")
    parser.add_argument("--diff-lines", type=int, default=120)
    parser.add_argument("--keep-worktree", action="store_true")
    return parser


def main(argv=None):
    try:
        try:
            args = build_parser().parse_args(argv)
        except SystemExit as exc:  # --help
            return 0 if exc.code in (0, None) else 2
        if DEFER_ENV in os.environ:
            raise Refusal("REVISION_DEFERRAL_PRESENT",
                          "{0} is set; producers are where deferred freshness debt is paid, so "
                          "they never run under it. Unset it and re-run.".format(DEFER_ENV))
        repo = gate._repo_root(args.repo)
        if args.install_from is not None:
            if args.install or args.base or args.only or args.durations:
                raise Refusal("REGEN_USAGE_INVALID", "--install-from takes no other option")
            install(repo, args.install_from)
            return 0
        if not args.base:
            raise Refusal("REGEN_USAGE_INVALID", "--base is required")
        if args.only:
            selected = [name.strip() for name in args.only.split(",") if name.strip()]
            unknown = [name for name in selected if name not in NAMES]
            if unknown:
                raise Refusal("REGEN_USAGE_INVALID", "unknown producer(s) {0}; known: {1}".format(
                    unknown, ", ".join(NAMES)))
        else:
            selected = [name for name in NAMES if name != "weights" or args.durations]
        if "weights" in selected and "nodes" not in selected:
            raise Refusal("REGEN_USAGE_INVALID", "weights need the nodes producer in the same run")
        if "weights" in selected and not args.durations:
            raise Refusal("REGEN_USAGE_INVALID", "weights need --durations")
        if args.bundle is not None:
            args.bundle = correction_suite.require_outside_repo(args.bundle, repo, "--bundle")
        regen_lock = _lock(repo, REGEN_LOCK_NAME)
        try:
            candidate = _clean_head(repo)
            durations = load_durations(args.durations, candidate) if args.durations else None
            if "weights" not in selected:
                gate._emit("{0}: weights left alone (no --durations record of {1})".format(
                    PROG, candidate))
            bundle = prepare(repo, args.base, selected, durations=durations,
                             bundle_dir=args.bundle, diff_lines=args.diff_lines,
                             keep_worktree=args.keep_worktree)
            if bundle is not None:
                if args.install:
                    install(repo, bundle)
                else:
                    gate._emit("{0}: dry run — install with: {1}".format(
                        PROG, install_command(repo, bundle)))
        finally:
            regen_lock.close()
        return 0
    except (Refusal, correction_suite.Refusal) as refusal:
        gate._report("{0} {1}".format(refusal.code, refusal.message))
        return refusal.status
    except gate.GateFailure as failure:
        gate._report("{0} {1}".format(failure.code, failure.message))
        return gate.exit_status_for(failure)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
