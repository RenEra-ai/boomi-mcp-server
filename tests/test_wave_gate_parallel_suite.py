"""The wave gate's split suite: the full non-KB suite as `SUITE_PROCESSES` pytest children.

The single-process run took 5 h 50 min on a hosted runner against a 360-minute cap, while
three of the runner's four vCPUs idled. The gate now partitions the collected nodes across
concurrent children (`plan_suite_partition`) and holds the WHOLE to every bound it held one
process to. These tests are the negative matrix for that: a partition that drops or doubles
a node, a child that runs a different selection, skips that are under the cap per child but
over it in total, a child that fails while the others pass, a rewritten selection file — each
must be refused with the code the single-process run uses for the same defect.

The positive path is the gate's own green run at HEAD, plus one end-to-end case here that
drives real pytest children over a throwaway suite.
"""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent


def _load_gate():
    spec = importlib.util.spec_from_file_location(
        "_wave_gate_parallel_under_test", _ROOT / "scripts" / "wave_gate.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


gate = _load_gate()


@pytest.fixture(autouse=True)
def _no_ambient_ci(monkeypatch):
    """As in `test_wave_gate.py`: a gate path under test must not see the runner's own CI."""
    monkeypatch.delenv("GITHUB_SHA", raising=False)
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    monkeypatch.delenv(gate.DEFER_ENV, raising=False)


def _manifest(collected, cap=30):
    header = {
        "kind": "manifest", "schema_version": 1, "manifest": "pytest-nodes",
        "minimum_active": 1, "minimum_collected": 1, "maximum_skipped": cap,
        "bootstrap_base": "1" * 40,
    }
    row = {"kind": "test", "id": "pytest-000001", "node_id": sorted(collected)[0], "state": "active"}
    raw = "\n".join(json.dumps(x, separators=(",", ":")) for x in (header, row)) + "\n"
    return gate.parse_manifest(raw.encode("utf-8"), "pytest-nodes")


def _order(modules):
    """``{module: test count}`` -> node ids in that (collection) order."""
    return [
        "tests/{0}.py::test_{1}".format(module, index)
        for module, count in modules.items()
        for index in range(count)
    ]


# ---------------------------------------------------------------------------
# the partition
# ---------------------------------------------------------------------------

def test_the_partition_assigns_every_node_exactly_once_in_collection_order():
    order = _order({"test_a": 5, "test_b": 3, "test_c": 7, "test_d": 1})
    plan = gate.plan_suite_partition(order, {}, 3)
    assigned = [node for child in plan for node in child["nodes"]]
    assert sorted(assigned) == sorted(order) and len(assigned) == len(order)
    for child in plan:
        positions = [order.index(node) for node in child["nodes"]]
        assert positions == sorted(positions), "a child must run its share in collection order"


def test_a_light_module_runs_whole_and_is_selected_by_its_path():
    order = _order({"test_a": 4, "test_b": 4, "test_c": 4, "test_d": 4})
    plan = gate.plan_suite_partition(order, {}, 2)
    for child in plan:
        assert all("::" not in arg for arg in child["args"]), child["args"]
        modules = {gate._node_module(node) for node in child["nodes"]}
        assert sorted(child["args"]) == sorted(modules)


def test_a_module_heavier_than_half_a_share_is_split_into_its_nodes():
    """Otherwise one module bounds the wall clock: the measured suite has a single module
    of 107 of its 346 minutes."""
    order = _order({"test_heavy": 6, "test_a": 2, "test_b": 2})
    weights = {node: 100.0 for node in order if "test_heavy" in node}
    plan = gate.plan_suite_partition(order, weights, 3)
    heavy_owners = {index for index, child in enumerate(plan)
                    for node in child["nodes"] if "test_heavy" in node}
    assert len(heavy_owners) >= 2, plan
    for child in plan:
        for arg in child["args"]:
            if "test_heavy" in arg:
                assert "::" in arg, "a split module is selected node by node"


def test_a_split_module_is_cut_into_contiguous_runs_of_its_order():
    """QA-cispeed-r1-02. Splitting into single nodes scattered neighbours across processes,
    so a test that leaks state into the next one stopped sharing an interpreter with it.
    Contiguous chunks keep every neighbour pair together except at the cut points."""
    order = _order({"test_heavy": 40, "test_a": 2})
    weights = {node: 10.0 for node in order if "test_heavy" in node}
    plan = gate.plan_suite_partition(order, weights, 4)
    heavy = [n for n in order if "test_heavy" in n]
    owner = {n: i for i, child in enumerate(plan) for n in child["nodes"]}
    # Segments of consecutive nodes that landed in the same child.
    segments, current = [], [heavy[0]]
    for a, b in zip(heavy, heavy[1:]):
        if owner[a] == owner[b]:
            current.append(b)
        else:
            segments.append(current)
            current = [b]
    segments.append(current)
    # Chunks are a quarter of a fair share, so at most 4 x processes of them; a node-by-node
    # split would cut almost every neighbour pair (39 of 39 possible here).
    assert 2 <= len(segments) <= 4 * 4, len(segments)
    assert all(len(segment) >= 2 for segment in segments[:-1]), [len(x) for x in segments]


def test_a_kb_node_never_reaches_a_selection():
    """QA-cispeed-r1-01. `--ignore` does not stop an explicit node id, so the partition
    refuses one rather than trusting the flag."""
    order = _order({"test_a": 2}) + ["tests/kb/test_k.py::test_kb"]
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.plan_suite_partition(order, {}, 2)
    assert excinfo.value.code == "PYTEST_EXECUTION_UNRECONCILED"


def test_the_partition_is_a_pure_function_of_order_weights_and_count():
    order = _order({"test_a": 9, "test_b": 30, "test_c": 2, "test_d": 11})
    weights = {order[3]: 7.5, order[20]: 40.0, order[44]: 3.0}
    first = gate.plan_suite_partition(order, weights, 4)
    assert first == gate.plan_suite_partition(list(order), dict(weights), 4)


def test_more_processes_than_work_yields_fewer_children_not_empty_ones():
    plan = gate.plan_suite_partition(_order({"test_a": 1, "test_b": 1}), {}, 4)
    assert len(plan) == 2 and all(child["nodes"] for child in plan)


def test_the_split_balances_by_weight():
    order = _order({"test_a": 10, "test_b": 10, "test_c": 10, "test_d": 10})
    weights = {node: (50.0 if node.startswith("tests/test_a.py") else 1.0) for node in order}
    plan = gate.plan_suite_partition(order, weights, 4)
    loads = sorted(sum(weights[n] for n in child["nodes"]) for child in plan)
    assert loads[-1] <= 2 * (sum(weights.values()) / 4), loads


def test_the_committed_suite_partition_constants_are_the_contract():
    """Four children, from a committed constant — never `os.cpu_count()`, so every host groups
    the same tests into the same processes."""
    assert gate.SUITE_PROCESSES == 4
    source = (_ROOT / "scripts" / "wave_gate.py").read_text(encoding="utf-8")
    code = "\n".join(line for line in source.splitlines() if not line.lstrip().startswith("#"))
    assert "cpu_count(" not in code.replace("`os.cpu_count()`", "")
    assert "processes=SUITE_PROCESSES, weights=load_suite_weights(repo)" in source


# ---------------------------------------------------------------------------
# the weights file
# ---------------------------------------------------------------------------

def _weights_doc(**overrides):
    document = {"kind": "suite-weights", "schema_version": 1,
                "weights": {"tests/test_a.py::test_x": 1.5}}
    document.update(overrides)
    return json.dumps(document).encode("utf-8")


def test_the_committed_weights_file_parses():
    weights = gate.load_suite_weights(str(_ROOT))
    assert weights and all(value >= 0 for value in weights.values())


@pytest.mark.parametrize("raw", [
    b"not json",
    b'{"kind": "suite-weights", "schema_version": 1}',
    _weights_doc(kind="pytest-nodes"),
    _weights_doc(schema_version=2),
    _weights_doc(weights=[]),
    _weights_doc(weights={"not a node": 1.0}),
    _weights_doc(weights={"tests/test_a.py::t": -1}),
    _weights_doc(weights={"tests/test_a.py::t": "1"}),
    _weights_doc(weights={"tests/test_a.py::t": True}),
    b'{"kind": "suite-weights", "schema_version": 1, "weights": {"tests/a.py::t": NaN}}',
    b'{"kind": "suite-weights", "kind": "suite-weights", "schema_version": 1, "weights": {}}',
], ids=["not-json", "missing-weights", "wrong-kind", "wrong-schema-version",
        "weights-not-an-object", "key-not-a-node-id", "negative-weight", "string-weight",
        "boolean-weight", "nan-weight", "duplicate-key"])
def test_a_malformed_weights_file_is_refused(raw):
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.parse_suite_weights(raw)
    assert excinfo.value.code == "MANIFEST_FORMAT_INVALID"
    assert excinfo.value.status == 2


def test_a_missing_weights_file_is_refused(tmp_path):
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.load_suite_weights(str(tmp_path))
    assert excinfo.value.code == "MANIFEST_MISSING"


# ---------------------------------------------------------------------------
# the split run: every bound binds to the whole
# ---------------------------------------------------------------------------

class _FakeChildren:
    """Stands in for the pytest children: each reads its selection and writes its report.

    ``behave(index, nodes) -> (status, summary line, nodes to report)`` decides what child
    ``index`` (1-based, in spawn order) claims to have done.
    """

    def __init__(self, order, behave):
        self.order = order
        self.behave = behave
        self.spawned = []

    def install(self, monkeypatch):
        fake = self

        class _Proc:
            def __init__(self, argv, **_kwargs):
                fake.spawned.append(argv)
                index = len(fake.spawned)
                selection = next(arg[1:] for arg in argv if arg.startswith("@"))
                report = argv[argv.index("--junit-xml") + 1]
                with open(selection, encoding="utf-8") as handle:
                    args = handle.read().splitlines()
                nodes = [node for node in fake.order
                         if node in args or gate._node_module(node) in args]
                self._status, line, reported = fake.behave(index, nodes)
                cases = "".join(
                    '<testcase classname="{0}" name="{1}"/>'.format(*gate._junit_projection(n))
                    for n in reported)
                with open(report, "w", encoding="utf-8") as handle:
                    handle.write("<testsuites><testsuite>{0}</testsuite></testsuites>".format(cases))
                self.stdout = iter(["...\n", "== {0} ==\n".format(line)])

            def wait(self):
                return self._status

        monkeypatch.setattr(gate.subprocess, "Popen", _Proc)


def _split_run(tmp_path, monkeypatch, behave, modules=None, cap=30, processes=3):
    order = _order(modules or {"test_a": 4, "test_b": 4, "test_c": 4, "test_d": 4})
    collected = gate.CollectedNodes(order)
    _FakeChildren(order, behave).install(monkeypatch)
    repo = tmp_path / "repo"
    repo.mkdir()
    scratch = gate.make_scratch_dir(str(repo))
    try:
        return gate.run_suite(str(repo), _manifest(collected, cap), collected, scratch,
                              processes=processes, weights={})
    finally:
        scratch.dispose()


def _honest(index, nodes):
    return 0, "{0} passed in 1.00s".format(len(nodes)), nodes


def test_an_honest_split_run_is_accounted_as_one_suite(tmp_path, monkeypatch):
    result = _split_run(tmp_path, monkeypatch, _honest)
    assert (result["passed"], result["skipped"]) == (16, 0)


def test_skips_are_capped_over_the_whole_suite_not_per_child(tmp_path, monkeypatch):
    """Each child is under the cap; the suite is over it. A per-child cap would let N
    children launder N times the permitted skips."""
    def skipping(index, nodes):
        return 0, "{0} passed, 2 skipped in 1.00s".format(len(nodes) - 2), nodes

    with pytest.raises(gate.GateFailure) as excinfo:
        _split_run(tmp_path, monkeypatch, skipping, cap=3)
    assert excinfo.value.code == "PYTEST_SKIPPED_EXCEEDS_CAP"


def test_one_failing_child_fails_the_suite(tmp_path, monkeypatch):
    def second_fails(index, nodes):
        if index == 2:
            return 1, "{0} passed, 1 failed in 1.00s".format(len(nodes) - 1), nodes
        return _honest(index, nodes)

    with pytest.raises(gate.GateFailure) as excinfo:
        _split_run(tmp_path, monkeypatch, second_fails)
    assert excinfo.value.code == "PYTEST_FAILED"
    assert "pytest child 2/" in excinfo.value.message


def test_a_child_that_reports_failures_but_exits_zero_fails_the_suite(tmp_path, monkeypatch):
    def lying(index, nodes):
        if index == 1:
            return 0, "{0} passed, 1 failed in 1.00s".format(len(nodes) - 1), nodes
        return _honest(index, nodes)

    with pytest.raises(gate.GateFailure) as excinfo:
        _split_run(tmp_path, monkeypatch, lying)
    assert excinfo.value.code == "PYTEST_FAILED"


def test_a_child_with_no_summary_fails_the_suite(tmp_path, monkeypatch):
    def silent(index, nodes):
        return 0, "something went sideways", nodes

    with pytest.raises(gate.GateFailure) as excinfo:
        _split_run(tmp_path, monkeypatch, silent)
    assert excinfo.value.code == "PYTEST_SUMMARY_UNPARSEABLE"


def test_a_child_that_ran_one_node_twice_and_another_never_is_refused(tmp_path, monkeypatch):
    """The counts still add up; the identities do not."""
    def doubling(index, nodes):
        if index == 1:
            nodes = [nodes[0]] + nodes[:-1]
        return 0, "{0} passed in 1.00s".format(len(nodes)), nodes

    with pytest.raises(gate.GateFailure) as excinfo:
        _split_run(tmp_path, monkeypatch, doubling)
    assert excinfo.value.code == "PYTEST_EXECUTION_UNRECONCILED"


def test_a_child_that_ran_a_node_assigned_elsewhere_is_refused(tmp_path, monkeypatch):
    """Another child's node in this child's report, in place of one of its own: the UNION
    still covers everything once over, so only the per-child identity catches it."""
    stolen = {}

    def swapping(index, nodes):
        if index == 1:
            stolen["node"] = nodes[-1]
            return 0, "{0} passed in 1.00s".format(len(nodes)), nodes
        if index == 2:
            nodes = nodes[:-1] + [stolen["node"]]
        return 0, "{0} passed in 1.00s".format(len(nodes)), nodes

    with pytest.raises(gate.GateFailure) as excinfo:
        _split_run(tmp_path, monkeypatch, swapping)
    assert excinfo.value.code == "PYTEST_EXECUTION_UNRECONCILED"


def test_a_child_that_lost_a_node_is_refused_even_with_matching_counts(tmp_path, monkeypatch):
    def losing(index, nodes):
        if index == 3:
            return 0, "{0} passed in 1.00s".format(len(nodes)), nodes[:-1]
        return _honest(index, nodes)

    with pytest.raises(gate.GateFailure) as excinfo:
        _split_run(tmp_path, monkeypatch, losing)
    assert excinfo.value.code == "PYTEST_EXECUTION_UNRECONCILED"


def test_a_rewritten_selection_is_refused(tmp_path, monkeypatch):
    def rewriting(index, nodes):
        argv = fake.spawned[-1]
        selection = next(arg[1:] for arg in argv if arg.startswith("@"))
        with open(selection, "a", encoding="utf-8") as handle:
            handle.write("tests/test_z.py\n")
        return _honest(index, nodes)

    order = _order({"test_a": 4, "test_b": 4})
    collected = gate.CollectedNodes(order)
    fake = _FakeChildren(order, rewriting)
    fake.install(monkeypatch)
    repo = tmp_path / "repo"
    repo.mkdir()
    scratch = gate.make_scratch_dir(str(repo))
    try:
        with pytest.raises(gate.GateFailure) as excinfo:
            gate.run_suite(str(repo), _manifest(collected), collected, scratch,
                           processes=2, weights={})
    finally:
        scratch.dispose()
    assert excinfo.value.code == "PYTEST_EXECUTION_UNRECONCILED"


def test_a_child_whose_output_cannot_be_read_is_killed_and_fails_the_suite(tmp_path, monkeypatch):
    """A reader thread that dies would leave its child blocked on a full pipe: a hang until
    the job timeout instead of a verdict. The child is killed and the run is refused."""
    killed = []

    class _Undecodable:
        def __iter__(self):
            yield "...\n"
            raise UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid start byte")

    def undecodable(index, nodes):
        return _honest(index, nodes)

    order = _order({"test_a": 4, "test_b": 4})
    collected = gate.CollectedNodes(order)
    fake = _FakeChildren(order, undecodable)
    fake.install(monkeypatch)
    real = gate.subprocess.Popen

    def spawn(argv, **kwargs):
        proc = real(argv, **kwargs)
        if len(fake.spawned) == 2:
            proc.stdout = _Undecodable()
            proc.kill = lambda: killed.append(True)
        return proc

    monkeypatch.setattr(gate.subprocess, "Popen", spawn)
    repo = tmp_path / "repo"
    repo.mkdir()
    scratch = gate.make_scratch_dir(str(repo))
    try:
        with pytest.raises(gate.GateFailure) as excinfo:
            gate.run_suite(str(repo), _manifest(collected), collected, scratch,
                           processes=2, weights={})
    finally:
        scratch.dispose()
    assert excinfo.value.code == "PYTEST_FAILED"
    assert "could not be read" in excinfo.value.message
    assert killed == [True]


def test_a_failed_later_launch_kills_and_reaps_the_children_already_started(tmp_path, monkeypatch):
    """CDX review r1: if the second launch raised, the first child kept running past the
    gate's cleanup, neither killed nor waited for. Every started child is now reaped and the
    launch error propagates."""
    started = []

    class _Started:
        def __init__(self):
            self.stdout = iter(())
            self.killed = self.waited = False

        def kill(self):
            self.killed = True

        def wait(self):
            self.waited = True
            return 0

    def popen(argv, **kwargs):
        if started:
            raise OSError(24, "Too many open files")
        started.append(_Started())
        return started[-1]

    monkeypatch.setattr(gate.subprocess, "Popen", popen)
    collected = gate.CollectedNodes(_order({"test_a": 4, "test_b": 4}))
    repo = tmp_path / "repo"
    repo.mkdir()
    scratch = gate.make_scratch_dir(str(repo))
    try:
        with pytest.raises(OSError):
            gate.run_suite(str(repo), _manifest(collected), collected, scratch,
                           processes=2, weights={})
    finally:
        scratch.dispose()
    assert len(started) == 1 and started[0].killed and started[0].waited


def test_a_split_run_without_the_scratch_directory_is_refused():
    collected = gate.CollectedNodes(_order({"test_a": 2}))
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.run_suite("/nonexistent", _manifest(collected), collected, None, processes=2)
    assert excinfo.value.code == "PYTEST_EXECUTION_UNRECONCILED"


def test_collection_remembers_the_order_pytest_collected_in():
    out = "tests/b.py::t2\ntests/a.py::t1\ntests/b.py::t1\n\n3 tests collected in 0.01s\n"
    collected = gate.parse_collection_output(out)
    assert collected == {"tests/a.py::t1", "tests/b.py::t1", "tests/b.py::t2"}
    assert collected.order == ("tests/b.py::t2", "tests/a.py::t1", "tests/b.py::t1")


# ---------------------------------------------------------------------------
# end to end: real pytest children over a throwaway suite
# ---------------------------------------------------------------------------

def test_real_pytest_children_run_exactly_their_shares(tmp_path, monkeypatch):
    """Non-vacuity for the selection mechanism itself: the args file, a split module's
    parametrized ids (spaces, brackets and `::` inside the parameter), a whole-module path,
    and one junit report per child — reconciled against a real collection."""
    repo = tmp_path / "repo"
    (repo / "tests").mkdir(parents=True)
    (repo / "src").mkdir()
    (repo / "tests" / "test_heavy.py").write_text(
        "import pytest\n"
        "@pytest.mark.parametrize('v', ['a b', 'http://[::1]:9/x', '[ ]', 'c'])\n"
        "def test_p(v):\n    pass\n"
        "class TestK:\n    def test_m(self):\n        pass\n", encoding="utf-8")
    (repo / "tests" / "test_light.py").write_text(
        "import pytest\n"
        "def test_one():\n    pass\n"
        "def test_skip():\n    pytest.skip('environment-conditional')\n", encoding="utf-8")
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    monkeypatch.setenv("PYTEST_DISABLE_PLUGIN_AUTOLOAD", "1")

    scratch = gate.make_scratch_dir(str(repo))
    try:
        collected = gate.collect_nodes(str(repo), scratch)
        assert len(collected) == 7
        weights = {node: 10.0 for node in collected if "test_heavy" in node}
        plan = gate.plan_suite_partition(collected.order, weights, 3)
        assert any("::" in arg for child in plan for arg in child["args"]), \
            "the witness must split a module, or it proves nothing about node selection"
        summary = gate.run_suite(str(repo), _manifest(collected), collected, scratch,
                                 processes=3, weights=weights)
    finally:
        scratch.dispose()
    assert (summary["passed"], summary["skipped"]) == (6, 1)
