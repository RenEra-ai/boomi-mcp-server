"""`scripts/correction_suite.py` — the full local non-KB suite for a correction loop.

The command is the gate's partitioned suite without the gate: collection and the committed
node manifest's floor and required nodes, the four-child partition, the ONE shared
reconciliation. These tests drive it end to end over a throwaway repository that carries the
REAL corpus plugin (`tests/_revision_corpus.py`, copied with its `conftest.py` declaration),
so the deferral it hands its children is the deferral the suite really applies:

* a clean strict run is labelled ``FULL LOCAL RUN``; a dirty or deferred one ``PARTIAL``;
* a required node that is gone fails with ``PYTEST_NODE_MISSING`` (the planted failure);
* deferred skips are reconciled by identity against the marked set, never inside the cap;
* the tree must not change during the run; the lock refuses a concurrent install;
* every ``wave_gate.py`` subcommand still refuses the deferral.
"""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent


def _load(name, relative):
    spec = importlib.util.spec_from_file_location(name, _ROOT / relative)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


suite = _load("_correction_suite_under_test", "scripts/correction_suite.py")
gate = suite.gate


@pytest.fixture(autouse=True)
def _no_ambient_ci(monkeypatch):
    monkeypatch.delenv("GITHUB_SHA", raising=False)
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    monkeypatch.delenv(gate.DEFER_ENV, raising=False)
    monkeypatch.setenv("PYTEST_DISABLE_PLUGIN_AUTOLOAD", "1")


def _git(repo, *args):
    return subprocess.run(["git", *args], cwd=str(repo), check=True,
                          capture_output=True, text=True).stdout.strip()


def _commit_all(repo, message="c"):
    _git(repo, "add", "-A")
    _git(repo, "-c", "user.email=t@example.invalid", "-c", "user.name=t",
         "commit", "-q", "-m", message)
    return _git(repo, "rev-parse", "HEAD")


_SCHEMA = "tests/fixtures/authoring_contract/process_ir_authoring_v1.schema.json"

_NODES = [
    "tests/test_a.py::test_one",
    "tests/test_a.py::test_two",
    "tests/test_b.py::TestB::test_m",
    "tests/test_fresh.py::test_snapshot_is_fresh",
]


def _jsonl(objects):
    return "".join(json.dumps(o) + "\n" for o in objects)


def _manifest_text(nodes, cap=1):
    header = {"kind": "manifest", "schema_version": 1, "manifest": "pytest-nodes",
              "minimum_active": len(nodes), "minimum_collected": len(nodes),
              "maximum_skipped": cap, "bootstrap_base": "1" * 40}
    rows = [{"kind": "test", "id": "pytest-{0:06d}".format(i), "node_id": n, "state": "active"}
            for i, n in enumerate(nodes, 1)]
    return _jsonl([header] + rows)


def _toy_repo(tmp_path, required=None, cap=1):
    """A repository the command accepts: real plugin + conftest, a marked freshness test that
    reads its artifact through the plugin, one environment-conditional skip, both manifests,
    a weights file, committed and clean."""
    repo = tmp_path / "repo"
    tests = repo / "tests"
    (tests / "fixtures" / "wave_gate").mkdir(parents=True)
    (tests / "fixtures" / "authoring_contract").mkdir(parents=True)
    (repo / "src").mkdir()
    (repo / "src" / ".keep").write_text("", encoding="utf-8")
    shutil.copy(_ROOT / "tests" / "_revision_corpus.py", tests / "_revision_corpus.py")
    shutil.copy(_ROOT / "tests" / "conftest.py", tests / "conftest.py")
    (repo / _SCHEMA).write_text("frozen\n", encoding="utf-8")
    (tests / "test_a.py").write_text(
        "def test_one():\n    pass\n\ndef test_two():\n    pass\n", encoding="utf-8")
    (tests / "test_b.py").write_text(
        "import pytest\n\nclass TestB:\n    def test_m(self):\n        pass\n\n"
        "def test_skip():\n    pytest.skip('environment-conditional')\n", encoding="utf-8")
    (tests / "test_fresh.py").write_text(
        "import pytest\nimport _revision_corpus as rc\n\n"
        "@pytest.mark.revision_freshness(artifact=rc.SCHEMA_SNAPSHOT)\n"
        "def test_snapshot_is_fresh():\n"
        "    assert rc.committed_artifact(rc.SCHEMA_SNAPSHOT) == 'frozen\\n'\n",
        encoding="utf-8")
    (repo / gate.NODES_MANIFEST).write_text(
        _manifest_text(_NODES if required is None else required, cap), encoding="utf-8")
    (repo / gate.GOLDENS_MANIFEST).write_text(_jsonl([
        {"kind": "manifest", "schema_version": 1, "manifest": "goldens", "minimum_active": 0,
         "bootstrap_base": "1" * 40}]), encoding="utf-8")
    (repo / gate.SUITE_WEIGHTS).write_text(json.dumps(
        {"kind": "suite-weights", "schema_version": 1, "weights": {}}), encoding="utf-8")
    (repo / ".gitignore").write_text("__pycache__/\n", encoding="utf-8")
    subprocess.run(["git", "init", "-q", "-b", "main", str(repo)], check=True)
    _commit_all(repo)
    return repo


def _main(repo, capsys, *extra):
    status = suite.main(["--repo", str(repo), *extra])
    return status, capsys.readouterr().err


# ---------------------------------------------------------------------------
# end to end over real pytest children
# ---------------------------------------------------------------------------

def test_a_clean_strict_run_is_a_full_local_run_and_only_that(tmp_path, capsys):
    repo = _toy_repo(tmp_path)
    status, err = _main(repo, capsys)
    assert status == 0, err
    verdict = err.strip().splitlines()[-1]
    assert verdict.startswith("correction_suite: FULL LOCAL RUN"), verdict
    assert "4 passed, 1 skipped (cap 1)" in verdict
    assert "not the wave and not a landing check" in verdict
    assert "PARTIAL" not in err
    assert "non-KB suite green" not in err, "the gate's verdict line must never appear"


def test_a_missing_required_node_fails_the_run(tmp_path, capsys):
    """The planted failure: the manifest requires a node the tree no longer has."""
    repo = _toy_repo(tmp_path, required=_NODES + ["tests/test_a.py::test_removed"])
    status, err = _main(repo, capsys)
    assert status == 1, err
    assert "PYTEST_NODE_MISSING" in err
    assert "tests/test_a.py::test_removed" in err
    assert "FULL LOCAL RUN" not in err and "PARTIAL" not in err


def test_a_deferred_run_reconciles_its_skips_by_identity_and_is_partial(tmp_path, capsys,
                                                                       monkeypatch):
    """The deferred freshness skip is outside the cap (cap 1, and the environment skip
    already uses it), but only because junit reports exactly the marked node skipped with
    the deferral's reason — which also pins `DEFERRAL_SKIP_PREFIX` to the real plugin."""
    repo = _toy_repo(tmp_path)
    monkeypatch.setenv(gate.DEFER_ENV, "1")
    status, err = _main(repo, capsys)
    assert status == 0, err
    verdict = err.strip().splitlines()[-1]
    assert verdict.startswith("correction_suite: PARTIAL — CERTIFIES NOTHING"), verdict
    assert "1 freshness check(s) skipped" in verdict
    assert "3 passed, 2 skipped (cap 1)" in verdict


def test_the_deferred_set_is_the_one_the_plugin_marks(tmp_path):
    repo = _toy_repo(tmp_path)
    assert suite.deferred_nodes(str(repo)) == {"tests/test_fresh.py::test_snapshot_is_fresh"}


def test_a_dirty_tree_is_partial(tmp_path, capsys):
    repo = _toy_repo(tmp_path)
    (repo / "tests" / "test_a.py").write_text(
        "def test_one():\n    pass\n\ndef test_two():\n    assert 1\n", encoding="utf-8")
    status, err = _main(repo, capsys)
    assert status == 0, err
    verdict = err.strip().splitlines()[-1]
    assert "PARTIAL — CERTIFIES NOTHING (dirty tree)" in verdict, verdict


def test_a_failing_test_fails_the_run(tmp_path, capsys):
    repo = _toy_repo(tmp_path)
    (repo / "tests" / "test_a.py").write_text(
        "def test_one():\n    pass\n\ndef test_two():\n    assert 0\n", encoding="utf-8")
    status, err = _main(repo, capsys)
    assert status == 1, err
    assert "PYTEST_FAILED" in err


def test_a_tree_changed_during_the_run_tested_no_single_tree(tmp_path, capsys, monkeypatch):
    repo = _toy_repo(tmp_path)
    real = gate._run_suite_children

    def editing(*args, **kwargs):
        children = real(*args, **kwargs)
        (repo / "tests" / "test_a.py").write_text("def test_one():\n    pass\n", encoding="utf-8")
        return children

    monkeypatch.setattr(gate, "_run_suite_children", editing)
    status, err = _main(repo, capsys)
    assert status == 1, err
    assert "WORKTREE_DIRTY" in err


def test_the_durations_record_is_written_outside_the_repository_only(tmp_path, capsys):
    repo = _toy_repo(tmp_path)
    out = tmp_path / "durations"
    status, err = _main(repo, capsys, "--durations-out", str(out))
    assert status == 0, err
    (record_path,) = list(out.iterdir())
    record = json.loads(record_path.read_text(encoding="utf-8"))
    assert record["kind"] == suite.DURATIONS_KIND
    assert record["head"] == _git(repo, "rev-parse", "HEAD")
    assert (record["dirty"], record["deferred"]) == (False, False)
    assert set(record["durations"]) == set(_NODES) | {"tests/test_b.py::test_skip"}

    with pytest.raises(suite.Refusal) as refused:
        suite.write_durations(str(repo / "inside"), str(repo), record)
    assert refused.value.code == "SCRATCH_INSIDE_REPO"


def test_a_durations_destination_inside_the_repository_is_refused_before_collection(
        tmp_path, capsys, monkeypatch):
    repo = _toy_repo(tmp_path)

    def never(*_args, **_kwargs):
        raise AssertionError("collection ran before the destination was checked")

    monkeypatch.setattr(gate, "collect_nodes", never)
    status, err = _main(repo, capsys, "--durations-out", str(repo / "timings"))
    assert status == 2, err
    assert err.split()[0] == "SCRATCH_INSIDE_REPO"
    assert not (repo / "timings").exists(), "the refusal creates nothing"


def test_a_partial_run_never_replaces_the_strict_record_of_the_same_commit(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    out = tmp_path / "durations"
    base = {"kind": suite.DURATIONS_KIND, "schema_version": 1, "head": "a" * 40,
            "processes": 4, "durations": {}}
    strict = dict(base, dirty=False, deferred=False, tree="1" * 64)
    deferred = dict(base, dirty=False, deferred=True, tree="1" * 64)
    dirty = dict(base, dirty=True, deferred=False, tree="2" * 64)
    paths = {suite.write_durations(str(out), str(repo), r) for r in (strict, deferred, dirty)}
    assert len(paths) == 3
    assert json.loads((out / suite.durations_name(strict)).read_text())["deferred"] is False


def test_pending_inputs_are_counted_by_identity_from_the_streamed_capture():
    line = ("  not yet in the packaged corpus (deferred): a compile input (sha256:{0}) "
            "from tests/test_{1}.py::t")
    children = [
        {"captured": [line.format("aa", 1), line.format("bb", 1)], "tail": []},
        {"captured": [line.format("aa", 2)], "tail": []},
    ]
    assert [p.split("(sha256:")[1][:2] for p in suite.pending_inputs(children)] == ["aa", "bb"]


def test_the_capture_survives_a_tail_that_evicts_it(tmp_path):
    """The planted case the bounded tail loses: a pending line followed by more output than
    the tail keeps. It is captured while streaming, so the count still sees it."""
    repo = _toy_repo(tmp_path)
    marker = "not yet in the packaged corpus (deferred): a compile input (sha256:feed) from x"
    (repo / "tests" / "test_a.py").write_text(
        "def test_one():\n    pass\n\ndef test_two():\n    pass\n\n"
        "def pytest_terminal_summary_marker():\n    pass\n", encoding="utf-8")
    with (repo / "tests" / "conftest.py").open("a", encoding="utf-8") as handle:
        handle.write("\n\ndef pytest_terminal_summary(terminalreporter):\n"
                     "    terminalreporter.write_line({0!r})\n"
                     "    for i in range(600):\n"
                     "        terminalreporter.write_line('filler %d' % i)\n".format(marker))
    tmpdir = gate.make_scratch_dir(str(repo))
    try:
        plan = [{"args": None, "nodes": []}]
        (child,) = gate._run_suite_children(str(repo), plan, None, capture=suite._PENDING_LINE)
    finally:
        tmpdir.dispose()
    assert not any(marker in line for line in child["tail"]), "the tail must have evicted it"
    assert [line.strip() for line in child["captured"]] == [marker]
    assert len(suite.pending_inputs([child])) == 1


# ---------------------------------------------------------------------------
# refusals
# ---------------------------------------------------------------------------

def test_a_usage_error_leads_with_its_code(tmp_path, capsys):
    status, err = _main(tmp_path, capsys, "--bogus")
    assert status == 2
    assert err.split()[0] == "CORRECTION_USAGE_INVALID", err


@pytest.mark.parametrize("value", ["", "0", "true", " 1"])
def test_only_the_exact_value_1_defers(value, tmp_path, capsys, monkeypatch):
    monkeypatch.setenv(gate.DEFER_ENV, value)
    status, err = _main(tmp_path, capsys)
    assert status == 2
    assert err.split()[0] == "CORRECTION_DEFERRAL_INVALID"


def test_the_deferral_is_refused_under_ci(tmp_path, capsys, monkeypatch):
    monkeypatch.setenv(gate.DEFER_ENV, "1")
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    status, err = _main(tmp_path, capsys)
    assert status == 2
    assert err.split()[0] == "CORRECTION_UNDER_CI"


def test_a_held_install_lock_refuses_the_run(tmp_path, capsys):
    repo = _toy_repo(tmp_path)
    held = suite.tree_lock(str(repo), exclusive=True)
    try:
        status, err = _main(repo, capsys)
    finally:
        held.close()
    assert status == 2
    assert err.split()[0] == "CORRECTION_LOCK_HELD"


def test_two_suites_may_share_the_lock_but_an_install_may_not(tmp_path):
    repo = _toy_repo(tmp_path)
    first = suite.tree_lock(str(repo), exclusive=False)
    second = suite.tree_lock(str(repo), exclusive=False)
    try:
        with pytest.raises(suite.Refusal) as refused:
            suite.tree_lock(str(repo), exclusive=True)
        assert refused.value.code == "CORRECTION_LOCK_HELD"
    finally:
        first.close()
        second.close()


@pytest.mark.parametrize("command", [["manifests", "--base", "HEAD"], ["wave", "--base", "HEAD"],
                                     ["ci", "--base", "HEAD"]])
def test_every_gate_subcommand_still_refuses_the_deferral(command, monkeypatch, capsys):
    monkeypatch.setenv(gate.DEFER_ENV, "1")
    assert gate.main(command) == 2
    assert capsys.readouterr().err.split()[0] == "REVISION_DEFERRAL_PRESENT"


def test_the_gates_own_child_environment_never_carries_the_deferral(monkeypatch):
    monkeypatch.setenv(gate.DEFER_ENV, "1")
    assert gate.DEFER_ENV not in gate._pytest_env()


def test_the_deferral_skip_prefix_is_the_plugins_reason():
    import _revision_corpus as plugin

    assert gate.DEFERRAL_SKIP_PREFIX == "deferred by {0}=1".format(plugin.DEFER_ENV)
    assert suite.DEFER_ENV is gate.DEFER_ENV


# ---------------------------------------------------------------------------
# the shared reconciliation's deferral branch, by identity
# ---------------------------------------------------------------------------

def _children_with_reports(scratch, shares, skipped_by_deferral):
    """Fake reaped children whose junit marks ``skipped_by_deferral`` with the reason."""
    children = []
    for index, nodes in enumerate(shares, 1):
        name = "junit-{0}.xml".format(index)
        fd = scratch.create_owned(name)
        cases = []
        for node in nodes:
            classname, test = gate._junit_projection(node)
            inner = ('<skipped type="pytest.skip" message="{0} — owed"/>'.format(
                gate.DEFERRAL_SKIP_PREFIX) if node in skipped_by_deferral else "")
            cases.append('<testcase classname="{0}" name="{1}" time="0.1">{2}</testcase>'.format(
                classname, test, inner))
        os.write(fd, "<testsuites><testsuite>{0}</testsuite></testsuites>".format(
            "".join(cases)).encode("utf-8"))
        skipped = sum(1 for n in nodes if n in skipped_by_deferral)
        children.append({"index": index, "nodes": nodes, "report_fd": fd, "report_name": name,
                         "status": 0, "tail": [],
                         "summaries": ["{0} passed, {1} skipped in 1.00s".format(
                             len(nodes) - skipped, skipped)]})
    return children


def _reconcile(tmp_path, reported, marked, cap=0):
    order = ["tests/test_x.py::test_{0}".format(i) for i in range(6)]
    collected = gate.CollectedNodes(order)
    manifest = gate.parse_manifest(_manifest_text(order[:1], cap).encode("utf-8"), "pytest-nodes")
    repo = tmp_path / "repo"
    repo.mkdir(exist_ok=True)
    scratch = gate.make_scratch_dir(str(repo))
    try:
        children = _children_with_reports(scratch, [order[:3], order[3:]], reported)
        return gate.reconcile_suite(manifest, collected, children, scratch, deferred=marked)
    finally:
        scratch.dispose()


def test_exactly_the_marked_skips_are_accepted(tmp_path):
    marked = frozenset({"tests/test_x.py::test_1", "tests/test_x.py::test_4"})
    summary = _reconcile(tmp_path, marked, marked)
    assert (summary["passed"], summary["skipped"], summary["deferred"]) == (4, 2, 2)


@pytest.mark.parametrize("reported,marked", [
    ({"tests/test_x.py::test_1", "tests/test_x.py::test_2"}, {"tests/test_x.py::test_1"}),
    ({"tests/test_x.py::test_1"}, {"tests/test_x.py::test_1", "tests/test_x.py::test_4"}),
], ids=["an-unmarked-skip-claims-the-reason", "a-marked-test-ran-or-vanished"])
def test_a_deferral_skip_set_that_is_not_the_marked_set_is_refused(tmp_path, reported, marked):
    with pytest.raises(gate.GateFailure) as refused:
        _reconcile(tmp_path, frozenset(reported), frozenset(marked))
    assert refused.value.code == "PYTEST_EXECUTION_UNRECONCILED"


def test_without_a_deferral_the_same_skips_count_against_the_cap(tmp_path):
    """The gate path (``deferred=None``) is unchanged: the reason buys nothing there."""
    marked = frozenset({"tests/test_x.py::test_1"})
    order = ["tests/test_x.py::test_{0}".format(i) for i in range(6)]
    collected = gate.CollectedNodes(order)
    manifest = gate.parse_manifest(_manifest_text(order[:1], 0).encode("utf-8"), "pytest-nodes")
    repo = tmp_path / "repo"
    repo.mkdir()
    scratch = gate.make_scratch_dir(str(repo))
    try:
        children = _children_with_reports(scratch, [order[:3], order[3:]], marked)
        with pytest.raises(gate.GateFailure) as refused:
            gate.reconcile_suite(manifest, collected, children, scratch)
    finally:
        scratch.dispose()
    assert refused.value.code == "PYTEST_SKIPPED_EXCEEDS_CAP"


def test_the_suite_lock_belongs_to_one_worktree(tmp_path):
    """An install into one checkout cannot disturb a suite in another worktree of the same
    repository, so a suite there must not block it — while the same worktree still does."""
    repo = _toy_repo(tmp_path)
    other = tmp_path / "other"
    _git(repo, "worktree", "add", "-q", "--detach", str(other), "HEAD")
    running = suite.tree_lock(str(repo), exclusive=False)
    try:
        suite.tree_lock(str(other), exclusive=True).close()
        with pytest.raises(suite.Refusal):
            suite.tree_lock(str(repo), exclusive=True)
    finally:
        running.close()
