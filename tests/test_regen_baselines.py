"""`scripts/regen_baselines.py` — one locked refresh of the derived baselines per candidate.

Driven end to end over a throwaway repository with the producers that need no Boomi
package (the node manifest and the weights), plus the real producers' wiring and the
shared snapshot renderer checked against this repository:

* the candidate is a clean HEAD, produced in a detached worktree; the checkout is untouched;
* the node manifest is rebuilt from the base: new tests appended, a removed test tombstoned
  only when the candidate's manifest retires it explicitly (otherwise the run fails), the
  candidate's tightened skip cap kept, a test added and removed inside the range gets no
  row, a retired identity cannot return;
* outputs install only while the inputs still match — a changed input refuses the install
  (the planted failure), and so does a moved HEAD or a tampered bundle; a bundle installs
  only its producers' declared outputs, and a failed install leaves the checkout as it was;
* an output destination inside the checkout is refused before any producer runs;
* weights only from a clean, strict durations record of the candidate;
* the deferral is refused, and a producer that writes outside its outputs fails the run.
"""

from __future__ import annotations

import importlib.util
import json
import subprocess
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent


def _load(name, relative):
    spec = importlib.util.spec_from_file_location(name, _ROOT / relative)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


regen = _load("_regen_baselines_under_test", "scripts/regen_baselines.py")
gate = regen.gate


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
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


def _jsonl(objects):
    return "".join(json.dumps(o) + "\n" for o in objects)


def _manifest(rows, cap=5):
    active = sum(1 for _n, state in rows if state == "active")
    header = {"kind": "manifest", "schema_version": 1, "manifest": "pytest-nodes",
              "minimum_active": active, "minimum_collected": active, "maximum_skipped": cap,
              "bootstrap_base": "1" * 40}
    return _jsonl([header] + [
        {"kind": "test", "id": "pytest-{0:06d}".format(i), "node_id": n, "state": s}
        for i, (n, s) in enumerate(rows, 1)])


def _tests(repo, **bodies):
    for name, body in bodies.items():
        path = repo / "tests" / "{0}.py".format(name)
        if body is None:
            path.unlink()
        else:
            path.write_text(body, encoding="utf-8")


def _toy_repo(tmp_path, base_rows=None):
    """Base commit: tests/test_a.py (one, two) and a matching manifest."""
    repo = tmp_path / "repo"
    (repo / "tests" / "fixtures" / "wave_gate").mkdir(parents=True)
    (repo / "src").mkdir()
    (repo / "src" / ".keep").write_text("", encoding="utf-8")
    (repo / ".gitignore").write_text("__pycache__/\n", encoding="utf-8")
    _tests(repo, test_a="def test_one():\n    pass\n\ndef test_two():\n    pass\n")
    rows = base_rows or [("tests/test_a.py::test_one", "active"),
                         ("tests/test_a.py::test_two", "active")]
    (repo / gate.NODES_MANIFEST).write_text(_manifest(rows), encoding="utf-8")
    (repo / gate.SUITE_WEIGHTS).write_text(json.dumps(
        {"kind": "suite-weights", "schema_version": 1, "weights": {}}, indent=0) + "\n",
        encoding="utf-8")
    subprocess.run(["git", "init", "-q", "-b", "main", str(repo)], check=True)
    return repo, _commit_all(repo, "base")


def _candidate(repo, retire=True, cap=5):
    """Remove test_two, add test_b.py::test_three. With ``retire``, the change also makes
    the owning edit to the manifest — test_two tombstoned — leaving the floors stale for the
    regeneration to recompute."""
    _tests(repo, test_a="def test_one():\n    pass\n",
           test_b="def test_three():\n    pass\n")
    if retire:
        (repo / gate.NODES_MANIFEST).write_text(_manifest([
            ("tests/test_a.py::test_one", "active"),
            ("tests/test_a.py::test_two", "tombstone")], cap=cap), encoding="utf-8")
    return _commit_all(repo, "candidate")


def _rows(text):
    return [json.loads(line) for line in text.splitlines()]


def _prepare(repo, base, tmp_path, selected=("nodes",), **kwargs):
    return regen.prepare(str(repo), base, list(selected), bundle_dir=str(tmp_path / "bundle"),
                         emit=lambda _line: None, **kwargs)


# ---------------------------------------------------------------------------
# the node manifest, rebuilt from the base
# ---------------------------------------------------------------------------

def test_the_node_manifest_is_rebuilt_from_the_base_and_installed(tmp_path):
    repo, base = _toy_repo(tmp_path)
    candidate = _candidate(repo)
    before = (repo / gate.NODES_MANIFEST).read_text(encoding="utf-8")

    bundle = _prepare(repo, base, tmp_path)
    assert bundle is not None
    assert (repo / gate.NODES_MANIFEST).read_text(encoding="utf-8") == before, \
        "producing must never touch the checkout"
    assert not any(line for line in _git(repo, "worktree", "list").splitlines()[1:]), \
        "the candidate worktree is removed"

    installed = regen.install(str(repo), bundle, emit=lambda _line: None)
    assert installed == [gate.NODES_MANIFEST]
    text = (repo / gate.NODES_MANIFEST).read_text(encoding="utf-8")
    header, *rows = _rows(text)
    assert [(r["id"], r["node_id"], r["state"]) for r in rows] == [
        ("pytest-000001", "tests/test_a.py::test_one", "active"),
        ("pytest-000002", "tests/test_a.py::test_two", "tombstone"),
        ("pytest-000003", "tests/test_b.py::test_three", "active"),
    ]
    assert (header["minimum_active"], header["minimum_collected"]) == (2, 2)
    base_manifest = gate.parse_manifest(_git(repo, "show", base + ":" + gate.NODES_MANIFEST)
                                        .encode("utf-8") + b"\n", "pytest-nodes")
    gate.validate_transition(base_manifest, gate.parse_manifest(text.encode("utf-8"), "pytest-nodes"),
                             "pytest-nodes")
    assert _git(repo, "rev-parse", "HEAD") == candidate


def test_a_node_that_stops_collecting_without_a_retirement_fails_the_run(tmp_path):
    """The gate spec: a node that no longer collects is an explicit tombstone by the change
    that deletes it, never a regeneration outcome. A module skipped at import time looks
    exactly like this, and must not lower the floor."""
    repo, base = _toy_repo(tmp_path)
    _candidate(repo, retire=False)
    with pytest.raises(regen.Refusal) as refused:
        _prepare(repo, base, tmp_path)
    assert refused.value.code == "REGEN_NODE_MISSING"
    assert "tests/test_a.py::test_two" in refused.value.message


def test_a_skip_cap_the_candidate_tightened_stays_tightened(tmp_path):
    repo, base = _toy_repo(tmp_path)
    _candidate(repo, cap=3)
    bundle = _prepare(repo, base, tmp_path)
    regen.install(str(repo), bundle, emit=lambda _line: None)
    header = _rows((repo / gate.NODES_MANIFEST).read_text(encoding="utf-8"))[0]
    assert header["maximum_skipped"] == 3, "regenerating from the base must not restore 5"


def test_a_refresh_never_moves_an_id_the_candidate_already_appended(tmp_path):
    """A second refresh in the same slice: the row appended by the first keeps its id even
    though a node added later collects before it, and the new node goes at the end."""
    repo, base = _toy_repo(tmp_path)
    _tests(repo, test_c="def test_z():\n    pass\n")
    (repo / gate.NODES_MANIFEST).write_text(_manifest([
        ("tests/test_a.py::test_one", "active"), ("tests/test_a.py::test_two", "active"),
        ("tests/test_c.py::test_z", "active")]), encoding="utf-8")
    _commit_all(repo, "first refresh")
    _tests(repo, test_b="def test_three():\n    pass\n")  # collects before test_c
    _commit_all(repo, "a later test, manifest stale")
    bundle = _prepare(repo, base, tmp_path)
    regen.install(str(repo), bundle, emit=lambda _line: None)
    rows = _rows((repo / gate.NODES_MANIFEST).read_text())[1:]
    assert [(r["id"], r["node_id"]) for r in rows[2:]] == [
        ("pytest-000003", "tests/test_c.py::test_z"),
        ("pytest-000004", "tests/test_b.py::test_three")]


def test_a_test_added_and_removed_inside_the_range_gets_no_row(tmp_path):
    repo, base = _toy_repo(tmp_path)
    _tests(repo, test_c="def test_transient():\n    pass\n")
    _commit_all(repo, "add transient")
    _tests(repo, test_c=None)
    (repo / gate.NODES_MANIFEST).write_text(_manifest([
        ("tests/test_a.py::test_one", "active"), ("tests/test_a.py::test_two", "active"),
        ("tests/test_c.py::test_transient", "active")]), encoding="utf-8")
    _commit_all(repo, "remove transient, stale manifest")
    bundle = _prepare(repo, base, tmp_path)
    regen.install(str(repo), bundle, emit=lambda _line: None)
    nodes = [r["node_id"] for r in _rows((repo / gate.NODES_MANIFEST).read_text())[1:]]
    assert "tests/test_c.py::test_transient" not in nodes


def test_a_current_manifest_needs_no_refresh(tmp_path):
    repo, base = _toy_repo(tmp_path)
    assert _prepare(repo, base, tmp_path) is None


def test_a_retired_identity_cannot_return(tmp_path):
    repo, base = _toy_repo(tmp_path, base_rows=[
        ("tests/test_a.py::test_one", "active"), ("tests/test_a.py::test_two", "active"),
        ("tests/test_a.py::test_gone", "tombstone")])
    _tests(repo, test_a="def test_one():\n    pass\n\ndef test_two():\n    pass\n\n"
                        "def test_gone():\n    pass\n")
    _commit_all(repo, "revive")
    with pytest.raises(regen.Refusal) as refused:
        _prepare(repo, base, tmp_path)
    assert refused.value.code == "REGEN_NODE_REVIVED"


# ---------------------------------------------------------------------------
# install only while the inputs still match
# ---------------------------------------------------------------------------

def test_a_changed_input_refuses_the_install(tmp_path):
    """The planted failure: an input edited after production. Nothing is written."""
    repo, base = _toy_repo(tmp_path)
    _candidate(repo)
    bundle = _prepare(repo, base, tmp_path)
    before = (repo / gate.NODES_MANIFEST).read_bytes()
    _tests(repo, test_b="def test_three():\n    pass\n\ndef test_four():\n    pass\n")
    with pytest.raises(regen.Refusal) as refused:
        regen.install(str(repo), bundle, emit=lambda _line: None)
    assert refused.value.code == "REGEN_INPUTS_CHANGED"
    assert (repo / gate.NODES_MANIFEST).read_bytes() == before


def test_a_moved_head_refuses_the_install(tmp_path):
    repo, base = _toy_repo(tmp_path)
    _candidate(repo)
    bundle = _prepare(repo, base, tmp_path)
    _tests(repo, test_b="def test_three():\n    pass\n\ndef test_four():\n    pass\n")
    _commit_all(repo, "a later correction")
    with pytest.raises(regen.Refusal) as refused:
        regen.install(str(repo), bundle, emit=lambda _line: None)
    assert refused.value.code == "REGEN_INPUTS_CHANGED"


def test_a_tampered_bundle_refuses_the_install(tmp_path):
    repo, base = _toy_repo(tmp_path)
    _candidate(repo)
    bundle = Path(_prepare(repo, base, tmp_path))
    staged = bundle / "files" / gate.NODES_MANIFEST
    staged.write_text(staged.read_text() + "\n", encoding="utf-8")
    with pytest.raises(regen.Refusal) as refused:
        regen.install(str(repo), str(bundle), emit=lambda _line: None)
    assert refused.value.code == "REGEN_BUNDLE_INVALID"


@pytest.mark.parametrize("path", ["tests/test_a.py", "../escaped.json"])
def test_a_bundle_installs_only_its_producers_declared_outputs(tmp_path, path):
    """The digests prove only that the bytes match the same index; the index names the
    destinations, so a destination outside the producers' declared outputs is refused before
    anything is written."""
    repo, base = _toy_repo(tmp_path)
    _candidate(repo)
    bundle = Path(_prepare(repo, base, tmp_path))
    index = json.loads((bundle / regen.BUNDLE_INDEX).read_text())
    staged = bundle / "files" / path
    staged.parent.mkdir(parents=True, exist_ok=True)
    staged.write_text("x = 1\n", encoding="utf-8")
    index["outputs"][path] = regen._sha256(b"x = 1\n")
    (bundle / regen.BUNDLE_INDEX).write_text(json.dumps(index), encoding="utf-8")
    before = {p: (repo / p).read_bytes() for p in ("tests/test_a.py", gate.NODES_MANIFEST)}
    with pytest.raises(regen.Refusal) as refused:
        regen.install(str(repo), str(bundle), emit=lambda _line: None)
    assert refused.value.code == "REGEN_BUNDLE_INVALID"
    assert {p: (repo / p).read_bytes() for p in before} == before
    assert not (tmp_path / "escaped.json").exists()


def test_a_failed_install_leaves_the_checkout_as_it_was(tmp_path, monkeypatch):
    """All or none: an output replaced before the failure is restored, an output HEAD does
    not have is removed again, and no temporary file is left behind."""
    repo, base = _toy_repo(tmp_path)
    _tests(repo, test_a="def test_one():\n    pass\n",
           test_b="def test_three():\n    pass\n")
    (repo / gate.NODES_MANIFEST).write_text(_manifest([
        ("tests/test_a.py::test_one", "active"),
        ("tests/test_a.py::test_two", "tombstone")]), encoding="utf-8")
    (repo / gate.SUITE_WEIGHTS).unlink()  # the candidate carries no weights file
    candidate = _commit_all(repo, "candidate without weights")
    durations = regen.load_durations(_durations(tmp_path / "d.json", candidate), candidate)
    bundle = _prepare(repo, base, tmp_path, selected=("nodes", "weights"), durations=durations)
    manifest_before = (repo / gate.NODES_MANIFEST).read_bytes()

    real_replace = regen.os.replace

    def failing(src, dst):
        if dst.endswith(gate.NODES_MANIFEST):  # sorted after suite_weights.json
            raise OSError("disk full")
        return real_replace(src, dst)

    monkeypatch.setattr(regen.os, "replace", failing)
    with pytest.raises(OSError):
        regen.install(str(repo), bundle, emit=lambda _line: None)
    monkeypatch.setattr(regen.os, "replace", real_replace)
    assert not (repo / gate.SUITE_WEIGHTS).exists(), "a newly created output is removed again"
    assert (repo / gate.NODES_MANIFEST).read_bytes() == manifest_before
    assert _git(repo, "status", "--porcelain", "--untracked-files=all") == ""


def test_an_install_is_refused_while_a_suite_holds_the_lock(tmp_path):
    repo, base = _toy_repo(tmp_path)
    _candidate(repo)
    bundle = _prepare(repo, base, tmp_path)
    suite_lock = regen.correction_suite.tree_lock(str(repo), exclusive=False)
    try:
        with pytest.raises(regen.correction_suite.Refusal) as refused:
            regen.install(str(repo), bundle, emit=lambda _line: None)
    finally:
        suite_lock.close()
    assert refused.value.code == "CORRECTION_LOCK_HELD"


def test_a_dirty_checkout_is_not_a_candidate(tmp_path):
    repo, base = _toy_repo(tmp_path)
    _tests(repo, test_b="def test_three():\n    pass\n")
    with pytest.raises(regen.Refusal) as refused:
        _prepare(repo, base, tmp_path)
    assert refused.value.code == "REGEN_CHECKOUT_DIRTY"


def test_a_base_off_the_candidate_history_is_refused(tmp_path):
    repo, base = _toy_repo(tmp_path)
    _git(repo, "checkout", "-q", "-b", "side")
    side = _candidate(repo)
    _git(repo, "checkout", "-q", "main")
    with pytest.raises(regen.Refusal) as refused:
        _prepare(repo, side, tmp_path)
    assert refused.value.code == "REGEN_BASE_INVALID"


def test_a_producer_writing_outside_its_outputs_fails_the_run(tmp_path, monkeypatch):
    repo, base = _toy_repo(tmp_path)

    def sneaky(ctx):
        (Path(ctx["worktree"]) / "tests" / "test_a.py").write_text("x = 1\n", encoding="utf-8")
        return []

    monkeypatch.setattr(regen, "PRODUCERS", (("sneaky", ("tests/fixtures/x.json",), sneaky, "test"),))
    with pytest.raises(regen.Refusal) as refused:
        _prepare(repo, base, tmp_path, selected=("sneaky",))
    assert refused.value.code == "REGEN_UNDECLARED_WRITE"


# ---------------------------------------------------------------------------
# weights, the deferral, the CLI
# ---------------------------------------------------------------------------

def _durations(path, head, dirty=False, deferred=False):
    path.write_text(json.dumps({
        "kind": "suite-durations", "schema_version": 1, "head": head, "dirty": dirty,
        "deferred": deferred, "processes": 4, "tree": "0" * 64,
        "durations": {"tests/test_a.py::test_one": 1.234, "tests/test_b.py::test_three": 0.01,
                      "tests/test_a.py::test_two": 9.0}}), encoding="utf-8")
    return str(path)


def test_weights_come_from_a_clean_strict_run_of_the_candidate(tmp_path):
    repo, base = _toy_repo(tmp_path)
    candidate = _candidate(repo)
    durations = regen.load_durations(_durations(tmp_path / "d.json", candidate), candidate)
    bundle = _prepare(repo, base, tmp_path, selected=("nodes", "weights"), durations=durations)
    regen.install(str(repo), bundle, emit=lambda _line: None)
    text = (repo / gate.SUITE_WEIGHTS).read_text(encoding="utf-8")
    assert json.loads(text)["weights"] == {"tests/test_a.py::test_one": 1.23}, \
        "below the floor and no-longer-collected nodes are dropped"
    assert text == json.dumps(json.loads(text), indent=0) + "\n"


@pytest.mark.parametrize("field", ["head", "dirty", "deferred"])
def test_weights_from_any_other_run_are_refused(tmp_path, field):
    candidate = "c" * 40
    kwargs = {"head": "d" * 40} if field == "head" else {"head": candidate, field: True}
    with pytest.raises(regen.Refusal) as refused:
        regen.load_durations(_durations(tmp_path / "d.json", **kwargs), candidate)
    assert refused.value.code == "REGEN_DURATIONS_INVALID"


@pytest.mark.parametrize("value", ["1", "", "0"])
def test_the_deferral_is_refused_whatever_its_value(tmp_path, monkeypatch, capsys, value):
    monkeypatch.setenv(gate.DEFER_ENV, value)
    assert regen.main(["--repo", str(tmp_path), "--base", "HEAD"]) == 2
    assert capsys.readouterr().err.split()[0] == "REVISION_DEFERRAL_PRESENT"


def test_producers_never_inherit_the_deferral(monkeypatch):
    monkeypatch.setenv(gate.DEFER_ENV, "1")
    assert gate.DEFER_ENV not in regen._producer_env()


def test_the_cli_stages_then_installs_from_the_bundle(tmp_path, capsys):
    repo, base = _toy_repo(tmp_path)
    _candidate(repo)
    out = tmp_path / "staged"
    assert regen.main(["--repo", str(repo), "--base", base, "--only", "nodes",
                       "--bundle", str(out)]) == 0
    err = capsys.readouterr().err
    assert "dry run" in err and "1 appended, 1 tombstoned against base" in err, err
    assert "1 rows appended, 0 tombstoned" in err, "the diff is against the candidate's own file"
    assert _git(repo, "status", "--porcelain") == ""
    assert regen.main(["--repo", str(repo), "--install-from", str(out)]) == 0
    assert _git(repo, "status", "--porcelain").split() == ["M", gate.NODES_MANIFEST]


def test_a_bundle_destination_inside_the_checkout_is_refused_before_any_producer(
        tmp_path, capsys, monkeypatch):
    repo, base = _toy_repo(tmp_path)
    _candidate(repo)
    ran = []
    monkeypatch.setattr(regen, "PRODUCERS", (
        ("nodes", (gate.NODES_MANIFEST,), lambda ctx: ran.append(1) or [], "test"),))
    assert regen.main(["--repo", str(repo), "--base", base, "--only", "nodes",
                       "--bundle", str(repo / "staged")]) == 2
    assert capsys.readouterr().err.split()[0] == "SCRATCH_INSIDE_REPO"
    assert ran == [] and not (repo / "staged").exists()
    with pytest.raises(regen.correction_suite.Refusal) as refused:
        regen.prepare(str(repo), base, ["nodes"], bundle_dir=str(repo / "staged"),
                      emit=lambda _line: None)
    assert refused.value.code == "SCRATCH_INSIDE_REPO"
    assert ran == [] and not any(
        line for line in _git(repo, "worktree", "list").splitlines()[1:])


def test_the_printed_install_command_runs_as_written(tmp_path, capsys):
    """The witness: stage a bundle for a checkout that is NOT this script's own, with a space
    in the bundle path, then execute the printed line verbatim through a shell from an
    unrelated directory. It must install into that checkout."""
    repo, base = _toy_repo(tmp_path)
    _candidate(repo)
    out = tmp_path / "a bundle"
    assert regen.main(["--repo", str(repo), "--base", base, "--only", "nodes",
                       "--bundle", str(out)]) == 0
    err = capsys.readouterr().err
    (line,) = [l for l in err.splitlines() if "dry run" in l]
    command = line.split("install with: ", 1)[1]
    own_manifest = (_ROOT / gate.NODES_MANIFEST).read_bytes()  # may be uncommitted: compare bytes
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    env = dict(regen.os.environ)
    env.pop(gate.DEFER_ENV, None)
    proc = subprocess.run(command, shell=True, cwd=str(elsewhere), env=env,
                          capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    assert _git(repo, "status", "--porcelain").split() == ["M", gate.NODES_MANIFEST]
    assert (_ROOT / gate.NODES_MANIFEST).read_bytes() == own_manifest, \
        "this script's own checkout is never the target"


@pytest.mark.parametrize("producers", [[{"nodes": 1}], [["nodes"]], "nodes", None])
def test_a_bundle_with_malformed_producer_names_is_refused_with_a_code(tmp_path, producers):
    repo, base = _toy_repo(tmp_path)
    _candidate(repo)
    bundle = Path(_prepare(repo, base, tmp_path))
    index = json.loads((bundle / regen.BUNDLE_INDEX).read_text())
    index["producers"] = producers
    (bundle / regen.BUNDLE_INDEX).write_text(json.dumps(index), encoding="utf-8")
    with pytest.raises(regen.Refusal) as refused:
        regen.install(str(repo), str(bundle), emit=lambda _line: None)
    assert refused.value.code == "REGEN_BUNDLE_INVALID"


def test_weights_without_a_durations_record_are_a_usage_error(tmp_path, capsys):
    repo, base = _toy_repo(tmp_path)
    assert regen.main(["--repo", str(repo), "--base", base, "--only", "nodes,weights"]) == 2
    assert capsys.readouterr().err.split()[0] == "REGEN_USAGE_INVALID"


# ---------------------------------------------------------------------------
# the real producers' wiring, against this repository
# ---------------------------------------------------------------------------

def test_the_producers_run_in_dependency_order_and_declare_the_committed_artifacts():
    import _revision_corpus as plugin

    assert regen.NAMES == ("corpus", "snapshots", "inventory", "nodes", "weights")
    outputs = {path for _n, paths, _r, _w in regen.PRODUCERS for path in paths}
    assert set(plugin.FRESHNESS_ARTIFACTS) <= outputs
    assert (_ROOT / regen.CORPUS) == plugin.CORPUS_PATH
    assert all((_ROOT / path).is_file() for path in outputs)


def test_the_snapshot_renderer_spells_the_plugins_paths():
    import _authoring_contract_snapshots as snapshots
    import _revision_corpus as plugin

    assert snapshots.CONTRACT_SNAPSHOT == plugin.CONTRACT_SNAPSHOT == regen.CONTRACT_SNAPSHOT
    assert snapshots.SCHEMA_SNAPSHOT == plugin.SCHEMA_SNAPSHOT == regen.SCHEMA_SNAPSHOT
    assert set(snapshots.RENDERERS) == {plugin.CONTRACT_SNAPSHOT, plugin.SCHEMA_SNAPSHOT}


def test_the_corpus_diff_reports_decoded_inputs(tmp_path):
    from boomi_mcp.authoring import revision_corpus as corpus

    old = corpus.serialize_corpus([{"entry": "compile", "n": 1}, {"entry": "compile", "n": 2}])
    new = corpus.serialize_corpus([{"entry": "compile", "n": 2}, {"entry": "resolve", "n": 3}])
    ctx = {"worktree": str(_ROOT), "env": regen._producer_env()}
    lines = regen.semantic_diff(ctx, regen.CORPUS, old, new, 50)
    assert lines[0] == "decoded inputs: 2 -> 2 records, 1 added, 1 removed", lines
    assert "{'resolve': 1}" in lines[1] and "{'compile': 1}" in lines[2], lines


def test_a_usage_error_leads_with_its_code(tmp_path, capsys):
    assert regen.main(["--repo", str(tmp_path), "--bogus"]) == 2
    assert capsys.readouterr().err.split()[0] == "REGEN_USAGE_INVALID"


def test_no_run_leaves_a_temporary_holder_behind(tmp_path, monkeypatch):
    """A failed run and a run staged to an explicit --bundle both leave nothing in the
    temporary directory."""
    scratch = tmp_path / "tmp"
    scratch.mkdir()
    monkeypatch.setattr(regen.tempfile, "tempdir", str(scratch))
    repo, base = _toy_repo(tmp_path)
    _candidate(repo, retire=False)
    with pytest.raises(regen.Refusal):
        _prepare(repo, base, tmp_path)  # REGEN_NODE_MISSING
    assert list(scratch.iterdir()) == []
    _git(repo, "reset", "-q", "--hard", base)
    _candidate(repo)
    assert _prepare(repo, base, tmp_path) is not None  # explicit bundle_dir
    assert list(scratch.iterdir()) == []


_INDEX_MUTATIONS = {
    "an-extra-field": lambda i: i.update(note="x"),
    "a-missing-field": lambda i: i.pop("base"),
    "another-kind": lambda i: i.update(kind="something-else"),
    "a-future-schema": lambda i: i.update(schema_version=2),
    "a-boolean-schema": lambda i: i.update(schema_version=True),
    "a-short-candidate": lambda i: i.update(candidate=i["candidate"][:12]),
    "an-uppercase-base": lambda i: i.update(base=i["base"].upper()),
    "a-non-string-base": lambda i: i.update(base=None),
    "an-unknown-producer": lambda i: i.update(producers=["nodes", "goldens"]),
    "producers-out-of-order": lambda i: i.update(producers=["weights", "nodes"]),
    "duplicate-producers": lambda i: i.update(producers=["nodes", "nodes"]),
    "no-producers": lambda i: i.update(producers=[]),
    "no-outputs": lambda i: i.update(outputs={}),
    "outputs-not-a-mapping": lambda i: i.update(outputs=[gate.NODES_MANIFEST]),
    "a-malformed-digest": lambda i: i["outputs"].update({gate.NODES_MANIFEST: "abc"}),
    "an-output-of-another-producer": lambda i: i["outputs"].update({regen.INVENTORY: "0" * 64}),
}


@pytest.mark.parametrize("mutation", sorted(_INDEX_MUTATIONS))
def test_every_field_of_the_bundle_index_is_validated_before_anything_is_written(
        tmp_path, mutation):
    """The structural fix for the trusted-bundle-index class: one validator over the whole
    index (`check_bundle_index`), so every field the writer emits is checked by the reader.
    Each case is a concrete index the validator must exclude."""
    repo, base = _toy_repo(tmp_path)
    _candidate(repo)
    bundle = Path(_prepare(repo, base, tmp_path))
    index = json.loads((bundle / regen.BUNDLE_INDEX).read_text())
    regen.check_bundle_index(dict(index, outputs=dict(index["outputs"])), str(repo))  # genuine: passes
    _INDEX_MUTATIONS[mutation](index)
    (bundle / regen.BUNDLE_INDEX).write_text(json.dumps(index), encoding="utf-8")
    before = (repo / gate.NODES_MANIFEST).read_bytes()
    with pytest.raises(regen.Refusal) as refused:
        regen.install(str(repo), str(bundle), emit=lambda _line: None)
    assert refused.value.code == "REGEN_BUNDLE_INVALID"
    assert (repo / gate.NODES_MANIFEST).read_bytes() == before


def test_the_bundle_index_fields_are_the_ones_the_writer_emits(tmp_path):
    repo, base = _toy_repo(tmp_path)
    _candidate(repo)
    bundle = Path(_prepare(repo, base, tmp_path))
    index = json.loads((bundle / regen.BUNDLE_INDEX).read_text())
    assert tuple(sorted(index)) == regen.BUNDLE_FIELDS
