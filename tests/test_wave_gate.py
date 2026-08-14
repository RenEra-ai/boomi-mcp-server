"""The wave gate's negative matrix: every fail-closed path, proven to trip.

A gate is only worth its runtime if it goes RED for the things it claims to
catch. Everything here is a NEGATIVE: a manifest, a baseline, an event payload
or a collection that must be refused, with the exact diagnostic code and exit
status it must be refused with. The positive path is covered by the gate's own
green run at HEAD and by ``test_wave_gate_goldens.py``.

Each case builds a throwaway git repository under ``tmp_path`` and drives the
real script. Nothing here touches the real worktree — which is also the property
``check_worktree_unchanged`` enforces at runtime.
"""

from __future__ import annotations

import hashlib
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
        "_wave_gate_under_test", _ROOT / "scripts" / "wave_gate.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


gate = _load_gate()

_ZERO = "0" * 40
_FAKE_BASE = "1" * 40


@pytest.fixture(autouse=True)
def _no_ambient_github_sha(monkeypatch):
    """Scrub `GITHUB_SHA` for every test in this module.

    This suite RUNS inside the workflow it is testing, and that workflow exports
    `GITHUB_SHA` to the wave-gate step, which passes it into pytest. Every test
    here that drives `check_checkout_matches_event` against a SYNTHETIC repo
    would otherwise read the outer checkout's sha and compare it against a
    temp-repo commit — green locally, red on the runner, for a reason that has
    nothing to do with the behaviour under test. (Reproduced: two tests failed
    under `GITHUB_SHA=1111...`.)

    An autouse fixture rather than a `delenv` in each test: remembering to scrub
    it per test is the same enumeration that has already been got wrong twice.
    Tests that WANT the variable set it explicitly with `monkeypatch.setenv`,
    which still wins because it runs after this fixture.
    """
    monkeypatch.delenv("GITHUB_SHA", raising=False)


# ---------------------------------------------------------------------------
# Fixtures: synthetic manifests and throwaway repositories
# ---------------------------------------------------------------------------

def _node_header(active=2, collected=2, base=_FAKE_BASE, skipped=30):
    return {
        "kind": "manifest", "schema_version": 1, "manifest": "pytest-nodes",
        "minimum_active": active, "minimum_collected": collected,
        "maximum_skipped": skipped, "bootstrap_base": base,
    }


def _golden_header(active=2, base=_FAKE_BASE):
    return {
        "kind": "manifest", "schema_version": 1, "manifest": "goldens",
        "minimum_active": active, "bootstrap_base": base,
    }


def _node_row(index, node_id=None, state="active"):
    return {
        "kind": "test", "id": "pytest-{0:06d}".format(index),
        "node_id": node_id or "tests/test_x.py::test_{0}".format(index),
        "state": state,
    }


def _golden_row(index, name=None, disposition="survivor", owner="repository",
                state="active", input_case=None, renderer="process-component-v1"):
    name = name or "g{0}.xml".format(index)
    return {
        "kind": "golden", "id": "golden-{0:06d}".format(index),
        "input_case": input_case or "case:{0}".format(index),
        "renderer": renderer,
        "expected_file": "tests/fixtures/golden_xml/{0}".format(name),
        "owner": owner, "disposition": disposition, "state": state,
    }


def _serialize(header, rows):
    lines = [json.dumps(header, separators=(",", ":"))]
    lines += [json.dumps(row, separators=(",", ":")) for row in rows]
    return ("\n".join(lines) + "\n").encode("utf-8")


def _default_nodes(base=_FAKE_BASE):
    return _serialize(_node_header(2, 2, base), [_node_row(1), _node_row(2)])


def _default_goldens(base=_FAKE_BASE):
    return _serialize(_golden_header(2, base), [_golden_row(1), _golden_row(2)])


def _run_git(repo, *args):
    subprocess.run(
        ["git", *args], cwd=str(repo), check=True,
        capture_output=True, text=True,
    )


def _commit(repo, message):
    _run_git(repo, "add", "-A")
    _run_git(
        repo, "-c", "user.email=gate@example.invalid", "-c", "user.name=gate",
        "commit", "-q", "--allow-empty", "-m", message,
    )
    return subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=str(repo),
        capture_output=True, text=True, check=True,
    ).stdout.strip()


def _git_out(repo, *args):
    return subprocess.run(
        ["git", *args], cwd=str(repo), capture_output=True, text=True, check=True,
    ).stdout.strip()


def _head(repo):
    return subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=str(repo),
        capture_output=True, text=True, check=True,
    ).stdout.strip()


def _write(repo, rel, payload):
    target = repo / rel
    target.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(payload, bytes):
        target.write_bytes(payload)
    else:
        target.write_text(payload, encoding="utf-8")


def _new_repo(tmp_path, name="repo"):
    repo = tmp_path / name
    repo.mkdir()
    _run_git(repo, "init", "-q", "-b", "main")
    (repo / "tests" / "fixtures" / "golden_xml").mkdir(parents=True)
    (repo / "tests" / "fixtures" / "wave_gate").mkdir(parents=True)
    _write(repo, "README.md", "seed\n")
    return repo


def _seeded(tmp_path, nodes=None, goldens=None, goldens_files=("g1.xml", "g2.xml")):
    """A repo whose BASE commit already carries both manifests.

    This is the ordinary (non-bootstrap) state: the base has manifests, so every
    transition rule applies. Returns ``(repo, base_sha)``.
    """
    repo = _new_repo(tmp_path)
    base_seed = _commit(repo, "seed")
    for name in goldens_files:
        _write(repo, "tests/fixtures/golden_xml/{0}".format(name), "<x/>\n")
    _write(repo, gate.NODES_MANIFEST, nodes or _default_nodes(base_seed))
    _write(repo, gate.GOLDENS_MANIFEST, goldens or _default_goldens(base_seed))
    base = _commit(repo, "manifests")
    return repo, base


def _gate(repo, *args):
    """Run the real CLI; return ``(exit_status, stderr)``."""
    proc = subprocess.run(
        [sys.executable, str(_ROOT / "scripts" / "wave_gate.py"),
         "--repo", str(repo), *args],
        capture_output=True, text=True,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
    )
    return proc.returncode, proc.stderr


def _manifests(repo, base, *extra):
    return _gate(repo, "manifests", "--base", base, *extra)


def _expect(result, status, code):
    got_status, stderr = result
    assert got_status == status, (got_status, stderr)
    assert code in stderr, stderr


# ===========================================================================
# Baseline resolution
# ===========================================================================

def test_no_baseline_at_all_is_refused():
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.resolve_baseline(str(_ROOT))
    assert excinfo.value.code == "BASELINE_EVENT_INVALID"
    assert excinfo.value.status == 2


def test_base_and_event_together_are_refused(tmp_path):
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.resolve_baseline(str(_ROOT), event_path=str(tmp_path / "e.json"),
                              base="HEAD")
    assert excinfo.value.code == "BASELINE_EVENT_INVALID"


def test_the_wave_subcommand_requires_an_explicit_base(tmp_path):
    """No inferred HEAD^, no branch, no remote: exit 2, never a guessed range.

    The CODE is asserted too, not just the status: argparse also exits 2, so a
    regression to its uncoded failure would leave a status-only test green and
    the promised diagnostic silently gone.
    """
    repo, _base = _seeded(tmp_path)
    _expect(_gate(repo, "wave"), 2, "BASELINE_EVENT_INVALID")


@pytest.mark.parametrize("event_name", ["", "schedule", "workflow_dispatch"])
def test_an_unknown_event_name_is_refused(tmp_path, event_name):
    payload = tmp_path / "event.json"
    payload.write_text("{}", encoding="utf-8")
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.resolve_baseline(str(_ROOT), event_path=str(payload),
                              event_name=event_name)
    assert excinfo.value.code == "BASELINE_EVENT_INVALID"


def test_an_unreadable_or_non_object_event_is_refused(tmp_path):
    missing = tmp_path / "nope.json"
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.resolve_baseline(str(_ROOT), event_path=str(missing), event_name="push")
    assert excinfo.value.code == "BASELINE_EVENT_INVALID"

    listy = tmp_path / "list.json"
    listy.write_text("[]", encoding="utf-8")
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.resolve_baseline(str(_ROOT), event_path=str(listy), event_name="push")
    assert excinfo.value.code == "BASELINE_EVENT_INVALID"


@pytest.mark.parametrize(
    "before,code",
    [
        (None, "BASELINE_EVENT_INVALID"),
        ("", "BASELINE_EVENT_INVALID"),
        ("not-a-sha", "BASELINE_EVENT_INVALID"),
        ("ABCDEF" + "0" * 34, "BASELINE_EVENT_INVALID"),   # uppercase is not our form
        (_ZERO, "BASELINE_ZERO_SHA"),
    ],
    ids=["missing", "empty", "non-hex", "uppercase", "all-zero"],
)
def test_a_push_without_a_usable_before_is_refused(tmp_path, before, code):
    payload = tmp_path / "event.json"
    payload.write_text(json.dumps({} if before is None else {"before": before}),
                       encoding="utf-8")
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.resolve_baseline(str(_ROOT), event_path=str(payload), event_name="push")
    assert excinfo.value.code == code
    assert excinfo.value.status == 2


def test_a_push_before_that_does_not_resolve_is_refused(tmp_path):
    payload = tmp_path / "event.json"
    payload.write_text(json.dumps({"before": "a" * 40}), encoding="utf-8")
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.resolve_baseline(str(_ROOT), event_path=str(payload), event_name="push")
    assert excinfo.value.code == "BASELINE_UNAVAILABLE"


def test_a_push_uses_before_verbatim_and_never_a_merge_base(tmp_path):
    """The bug this rule exists for: on a push to ``dev`` a merge-base of HEAD
    against ``dev`` is HEAD itself, so the gate would compare the new tip with
    ITSELF and validate nothing."""
    repo, base = _seeded(tmp_path)
    head = _commit(repo, "later")
    payload = tmp_path / "event.json"
    payload.write_text(json.dumps({"before": base, "after": head}), encoding="utf-8")
    resolved = gate.resolve_baseline(str(repo), event_path=str(payload),
                                     event_name="push")
    assert resolved["sha"] == base
    assert resolved["sha"] != head
    # `before` doubles as the target: it IS the branch tip the push builds on.
    assert resolved["kind"] == "push" and resolved["target"] == base


def test_a_pull_request_without_head_or_base_is_refused(tmp_path):
    for payload_obj in ({"pull_request": {}},
                        {"pull_request": {"head": {"sha": _FAKE_BASE}}},
                        {"nothing": True}):
        payload = tmp_path / "event.json"
        payload.write_text(json.dumps(payload_obj), encoding="utf-8")
        with pytest.raises(gate.GateFailure) as excinfo:
            gate.resolve_baseline(str(_ROOT), event_path=str(payload),
                                  event_name="pull_request")
        assert excinfo.value.code == "BASELINE_EVENT_INVALID"


def test_a_pull_request_uses_the_unique_merge_base(tmp_path):
    repo, base = _seeded(tmp_path)
    _run_git(repo, "checkout", "-q", "-b", "feature")
    head = _commit(repo, "feature work")
    _run_git(repo, "checkout", "-q", "main")
    target = _commit(repo, "main moves on")
    payload = tmp_path / "event.json"
    payload.write_text(json.dumps(
        {"pull_request": {"head": {"sha": head}, "base": {"sha": target}}}
    ), encoding="utf-8")
    resolved = gate.resolve_baseline(str(repo), event_path=str(payload),
                                     event_name="pull_request")
    assert resolved["sha"] == base
    # The merge base is the transition baseline; the TARGET tip is carried
    # separately because only it can decide bootstrap eligibility.
    assert resolved["kind"] == "pull_request" and resolved["target"] == target


def test_a_pull_request_with_no_merge_base_is_refused(tmp_path):
    """Unrelated histories — the shape a shallow checkout also produces."""
    repo, _base = _seeded(tmp_path)
    head = _commit(repo, "head")
    _run_git(repo, "checkout", "-q", "--orphan", "island")
    _run_git(repo, "rm", "-rq", "--cached", ".")
    _write(repo, "island.txt", "x\n")
    orphan = _commit(repo, "orphan")
    payload = tmp_path / "event.json"
    payload.write_text(json.dumps(
        {"pull_request": {"head": {"sha": orphan}, "base": {"sha": head}}}
    ), encoding="utf-8")
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.resolve_baseline(str(repo), event_path=str(payload),
                              event_name="pull_request")
    assert excinfo.value.code == "BASELINE_MERGE_BASE_MISSING"


# ===========================================================================
# Bootstrap — the single, scoped exception
# ===========================================================================

def _bootstrap_repo(tmp_path, node_base=None, golden_base=None, both=True):
    repo = _new_repo(tmp_path)
    base = _commit(repo, "pre-manifest")
    for name in ("g1.xml", "g2.xml"):
        _write(repo, "tests/fixtures/golden_xml/{0}".format(name), "<x/>\n")
    _write(repo, gate.NODES_MANIFEST, _default_nodes(node_base or base))
    if both:
        _write(repo, gate.GOLDENS_MANIFEST, _default_goldens(golden_base or base))
    _commit(repo, "introduce manifests")
    return repo, base


def test_the_one_legal_bootstrap_is_accepted(tmp_path):
    repo, base = _bootstrap_repo(tmp_path)
    status, stderr = _manifests(repo, base, "--bootstrap")
    assert status == 0, stderr
    assert "BOOTSTRAP" in stderr


def test_a_LOCAL_bootstrap_without_the_flag_is_refused(tmp_path):
    """Locally the baseline is whatever the operator typed, so the exception
    needs them to say they meant it."""
    repo, base = _bootstrap_repo(tmp_path)
    _expect(_manifests(repo, base), 2, "BOOTSTRAP_NOT_ALLOWED")


def test_a_CI_bootstrap_needs_no_flag(tmp_path):
    """...but in CI there is no flag and nobody to pass one.

    The event payload is the evidence, and it is stronger than a flag: the
    push's ``before`` must itself equal the ``bootstrap_base`` both headers
    declare. Requiring the flag here too made the bootstrap UNREACHABLE in CI —
    the run that lands the manifests could never go green, which is the one run
    the exception exists for.
    """
    repo, base = _bootstrap_repo(tmp_path)
    event = tmp_path / "push.json"
    event.write_text(json.dumps({"before": base, "after": _head(repo)}),
                     encoding="utf-8")
    proc = subprocess.run(
        [sys.executable, str(_ROOT / "scripts" / "wave_gate.py"),
         "--repo", str(repo), "manifests", "--github-event", str(event),
         "--event-name", "push"],
        capture_output=True, text=True,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
    )
    assert proc.returncode == 0, proc.stderr
    assert "BOOTSTRAP" in proc.stderr


def test_a_CI_bootstrap_whose_event_baseline_is_not_the_declared_base_is_refused(tmp_path):
    """The CI arm is not a free pass: with no flag to give, the event baseline
    matching the declared ``bootstrap_base`` is the ONLY thing authorising it.

    Built so the manifests really are absent at the event's baseline — otherwise
    the run is not a bootstrap at all, it is an ordinary no-change transition,
    and it would pass for a reason that proves nothing.
    """
    repo = _new_repo(tmp_path)
    earlier = _commit(repo, "two commits before the manifests")
    declared = _commit(repo, "one commit before the manifests")
    for name in ("g1.xml", "g2.xml"):
        _write(repo, "tests/fixtures/golden_xml/{0}".format(name), "<x/>\n")
    _write(repo, gate.NODES_MANIFEST, _default_nodes(declared))
    _write(repo, gate.GOLDENS_MANIFEST, _default_goldens(declared))
    _commit(repo, "introduce manifests")

    event = tmp_path / "push.json"
    event.write_text(json.dumps({"before": earlier, "after": _head(repo)}),
                     encoding="utf-8")
    proc = subprocess.run(
        [sys.executable, str(_ROOT / "scripts" / "wave_gate.py"),
         "--repo", str(repo), "manifests", "--github-event", str(event),
         "--event-name", "push"],
        capture_output=True, text=True,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
    )
    assert proc.returncode == 2, proc.stderr
    assert "BOOTSTRAP_NOT_ALLOWED" in proc.stderr


def test_introducing_only_one_manifest_is_refused(tmp_path):
    """A half-introduced pair fails on the tree, before bootstrap is considered.

    The two manifests are one ledger: the gate reads both or refuses, so there
    is no state in which only the goldens are governed.
    """
    repo, base = _bootstrap_repo(tmp_path, both=False)
    _expect(_manifests(repo, base, "--bootstrap"), 2, "MANIFEST_MISSING")


def test_an_ordinary_pull_request_whose_target_moved_on_is_NOT_a_bootstrap(tmp_path):
    """Codex Stage-2 r2 [P1]. The target probe must not fire on normal work.

    An earlier revision ran it BEFORE deciding whether a bootstrap was being
    claimed at all, so any PR whose target had advanced — every PR, once the
    ledger has landed — was refused with BOOTSTRAP_NOT_ALLOWED even though its
    merge base already carried the manifests and it was an ordinary transition.
    """
    repo, base = _seeded(tmp_path)          # merge base HAS the manifests
    _run_git(repo, "checkout", "-q", "-b", "feature")
    head = _commit(repo, "feature work")
    _run_git(repo, "checkout", "-q", "main")
    target = _commit(repo, "target advances independently")
    _run_git(repo, "checkout", "-q", "feature")

    event = tmp_path / "pr.json"
    event.write_text(json.dumps(
        {"pull_request": {"head": {"sha": head}, "base": {"sha": target}}}
    ), encoding="utf-8")
    proc = subprocess.run(
        [sys.executable, str(_ROOT / "scripts" / "wave_gate.py"),
         "--repo", str(repo), "manifests", "--github-event", str(event),
         "--event-name", "pull_request"],
        capture_output=True, text=True,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
    )
    assert proc.returncode == 0, proc.stderr
    assert "BOOTSTRAP" not in proc.stderr


def test_a_local_bootstrap_is_an_explicit_operator_assertion(tmp_path):
    """The local arm asserts; it does not verify. And it says so.

    There is deliberately NO local check that the exception is still unspent.
    Eight successive formulations of "has this ledger landed?" were each defeated
    — ancestry-only; a commit-count rule; exempting `*/<branch>` mirrors;
    enumerating ref namespaces; `--abbrev-ref` ambiguity; matching the
    introducing COMMIT rather than the path; `--all --not <own_ref>` subtracting
    merged-in commits; and default history simplification pruning a
    `merge -s ours` addition. They failed because locally the OPERATOR chooses
    the baseline, so no rule separates "legitimately introducing" from "asserting
    a stale baseline" — and a false refusal blocks the introduction itself.

    So the flag is still required (the operator must mean it), the run is labelled
    loudly, and the judgment lives in the `ci` arms, where the baseline comes from
    the platform rather than from the person being checked.
    """
    repo, base = _bootstrap_repo(tmp_path)

    # Still required: silence is not consent.
    _expect(_manifests(repo, base), 2, "BOOTSTRAP_NOT_ALLOWED")

    status, stderr = _manifests(repo, base, "--bootstrap")
    assert status == 0, stderr
    assert "OPERATOR ASSERTION" in stderr
    assert "skips manifest transition validation" in stderr

    # Sharing it changes nothing locally — that is the point being pinned, so a
    # ninth landing heuristic is not reintroduced by accident.
    _run_git(repo, "branch", "dev")
    status, stderr = _manifests(repo, base, "--bootstrap")
    assert status == 0, stderr
    assert "OPERATOR ASSERTION" in stderr

    # ...while the CI arm DOES judge it: `before` is the tip, which has the
    # manifests, so no bootstrap is available and the transition rules apply.
    event = tmp_path / "push.json"
    event.write_text(json.dumps(
        {"before": _head(repo), "after": _head(repo)}
    ), encoding="utf-8")
    proc = subprocess.run(
        [sys.executable, str(_ROOT / "scripts" / "wave_gate.py"),
         "--repo", str(repo), "manifests", "--github-event", str(event),
         "--event-name", "push"],
        capture_output=True, text=True,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
    )
    assert proc.returncode == 0, proc.stderr
    assert "BOOTSTRAP" not in proc.stderr


def test_the_worktree_fingerprint_survives_an_awkward_untracked_filename(tmp_path):
    """`-z` status must be read as BYTES.

    Universal-newline translation rewrites a `\\r` inside a legal POSIX filename
    and strict UTF-8 decoding raises on a name that is merely bytes — either way
    the gate looks at the wrong path, or crashes, on a file the user may have.
    """
    repo, _base = _seeded(tmp_path)
    awkward = repo / "we\rird\tname.txt"
    awkward.write_bytes(b"one\n")
    before = gate._status(str(repo))
    awkward.write_bytes(b"two\n")
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.check_worktree_unchanged(before, gate._status(str(repo)))
    assert excinfo.value.code == "WORKTREE_DIRTY"


def test_a_half_bootstrap_is_refused(tmp_path):
    """Both manifests exist NOW, but only one existed at the baseline.

    This is the case ``check_bootstrap`` owns: the goldens ledger is already
    under the transition rules while the node ledger claims to be brand new, so
    "bootstrap" would exempt a manifest that has a history.
    """
    repo = _new_repo(tmp_path)
    _write(repo, "tests/fixtures/golden_xml/g1.xml", "<x/>\n")
    _write(repo, "tests/fixtures/golden_xml/g2.xml", "<x/>\n")
    seed = _commit(repo, "seed")
    _write(repo, gate.GOLDENS_MANIFEST, _default_goldens(seed))
    base = _commit(repo, "goldens only")
    _write(repo, gate.NODES_MANIFEST, _default_nodes(base))
    _commit(repo, "add the node manifest later")
    _expect(_manifests(repo, base, "--bootstrap"), 2, "BOOTSTRAP_NOT_ALLOWED")


def test_a_bootstrap_base_that_is_not_the_validated_baseline_is_refused(tmp_path):
    repo, base = _bootstrap_repo(tmp_path, node_base=_FAKE_BASE,
                                 golden_base=_FAKE_BASE)
    _expect(_manifests(repo, base, "--bootstrap"), 2, "BOOTSTRAP_NOT_ALLOWED")


def test_manifests_declaring_different_bootstrap_bases_are_refused(tmp_path):
    repo = _new_repo(tmp_path)
    base = _commit(repo, "pre-manifest")
    for name in ("g1.xml", "g2.xml"):
        _write(repo, "tests/fixtures/golden_xml/{0}".format(name), "<x/>\n")
    _write(repo, gate.NODES_MANIFEST, _default_nodes(base))
    _write(repo, gate.GOLDENS_MANIFEST, _default_goldens(_FAKE_BASE))
    _commit(repo, "introduce manifests")
    _expect(_manifests(repo, base, "--bootstrap"), 2, "BOOTSTRAP_NOT_ALLOWED")


def test_a_push_cannot_bootstrap_once_the_manifests_have_landed(tmp_path):
    """Codex Stage-2 [P1], the push half.

    The ancestry probe looks only BACKWARDS: once the manifests land, a baseline
    that predates them still finds both paths absent there, forever. What closes
    it is that a push's baseline IS the branch tip it builds on — so after the
    landing every later push sees them present and cannot reach the bootstrap
    branch at all, whatever a stale `bootstrap_base` header still says.
    """
    repo, base = _bootstrap_repo(tmp_path)
    landed = _commit(repo, "the landing commit is now the branch tip")

    # Rewrite an immutable field and COMMIT it — an event-mode run requires a
    # clean tree, and a real push carries committed content anyway.
    raw = (repo / gate.GOLDENS_MANIFEST).read_text(encoding="utf-8").splitlines()
    raw[1] = raw[1].replace('"owner":"repository"', '"owner":"#999"')
    _write(repo, gate.GOLDENS_MANIFEST, "\n".join(raw) + "\n")
    _commit(repo, "rewrite the landed ledger")

    # A later push: `before` is the tip, which HAS the manifests.
    event = tmp_path / "push.json"
    event.write_text(json.dumps({"before": landed, "after": _head(repo)}),
                     encoding="utf-8")

    proc = subprocess.run(
        [sys.executable, str(_ROOT / "scripts" / "wave_gate.py"),
         "--repo", str(repo), "manifests", "--github-event", str(event),
         "--event-name", "push"],
        capture_output=True, text=True,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
    )
    assert proc.returncode == 2, proc.stderr
    assert "MANIFEST_TRANSITION_ILLEGAL" in proc.stderr


def test_multi_commit_development_of_the_introduction_stays_bootstrappable(tmp_path):
    """The rule that is deliberately absent, pinned so it is not reintroduced.

    A commit-count rule ("at most one commit may touch a manifest") looks like it
    confines bootstrap to the introduction, but it refuses ordinary multi-commit
    work on the very change that introduces the ledger — the slice could not
    validate itself after its second commit. Whether the ledger has LANDED is the
    discriminator, not how many commits touched it.
    """
    repo, base = _bootstrap_repo(tmp_path)
    # A second commit on the same unlanded change, editing the ledger again.
    _write(repo, "tests/fixtures/golden_xml/g3.xml", "<x/>\n")
    rows = [_golden_row(1), _golden_row(2), _golden_row(3)]
    _write(repo, gate.GOLDENS_MANIFEST, _serialize(_golden_header(3, base), rows))
    _commit(repo, "second commit of the same introduction")

    status, stderr = _manifests(repo, base, "--bootstrap")
    assert status == 0, stderr
    assert "BOOTSTRAP" in stderr


def test_a_pull_request_with_a_stale_merge_base_cannot_bootstrap(tmp_path):
    """Codex Stage-2 [P1], the CI half — and the case the commit-count rule alone
    does NOT catch.

    A branch cut before the manifests landed keeps a merge base that predates
    them forever. `merge-base..HEAD` then still contains exactly ONE commit
    introducing them (the one merged in from the target), so a count-based rule
    reads it as a pristine introduction and skips every transition check. Only
    the TARGET tip can answer the question that matters.
    """
    repo = _new_repo(tmp_path)
    for name in ("g1.xml", "g2.xml"):
        _write(repo, "tests/fixtures/golden_xml/{0}".format(name), "<x/>\n")
    fork_point = _commit(repo, "before the manifests existed")

    # The branch that was cut early, and never touched the manifests itself.
    _run_git(repo, "checkout", "-q", "-b", "old-feature")
    feature_only = _commit(repo, "unrelated feature work")

    # Meanwhile the manifests land on the target branch...
    _run_git(repo, "checkout", "-q", "main")
    _write(repo, gate.NODES_MANIFEST, _default_nodes(fork_point))
    _write(repo, gate.GOLDENS_MANIFEST, _default_goldens(fork_point))
    target = _commit(repo, "manifests land on the target branch")

    # GitHub checks out `refs/pull/N/merge` — the MERGE of head into base — while
    # the event still reports the un-merged branch tip as `head.sha`. That is what
    # keeps the merge base stale: it stays `fork_point`, not `target`.
    _run_git(repo, "checkout", "-q", "old-feature")
    _run_git(
        repo, "-c", "user.email=gate@example.invalid", "-c", "user.name=gate",
        "merge", "-q", "--no-edit", target,
    )
    # ...but the PR's head ref itself is still the un-merged commit.
    _run_git(repo, "branch", "-f", "pr-head", feature_only)

    merge_base = subprocess.run(
        ["git", "merge-base", feature_only, target], cwd=str(repo),
        capture_output=True, text=True, check=True,
    ).stdout.strip()
    assert merge_base == fork_point, "the merge base must still predate the manifests"
    assert (repo / gate.NODES_MANIFEST).exists(), "the checkout has the manifests"

    event = tmp_path / "pr.json"
    event.write_text(json.dumps({"pull_request": {
        "head": {"sha": feature_only}, "base": {"sha": target},
        # GitHub names the test-merge it built; the checkout IS that commit.
        "merge_commit_sha": _head(repo),
    }}), encoding="utf-8")
    proc = subprocess.run(
        [sys.executable, str(_ROOT / "scripts" / "wave_gate.py"),
         "--repo", str(repo), "manifests", "--github-event", str(event),
         "--event-name", "pull_request"],
        capture_output=True, text=True,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
    )
    assert proc.returncode == 2, proc.stderr
    assert "BOOTSTRAP_NOT_ALLOWED" in proc.stderr
    assert "target branch" in proc.stderr


def test_bootstrap_cannot_be_reused_after_a_deletion(tmp_path):
    """The ancestry proof is what makes bootstrap one-time.

    Delete both manifests, then re-add them: the paths ARE in the baseline's
    ancestry, so this is not a bootstrap and the transition rules apply — which
    is precisely what stops a deletion being laundered into a fresh start.
    """
    repo, _base = _seeded(tmp_path)
    _run_git(repo, "rm", "-q", gate.NODES_MANIFEST, gate.GOLDENS_MANIFEST)
    deleted = _commit(repo, "delete manifests")
    _write(repo, gate.NODES_MANIFEST, _default_nodes(deleted))
    _write(repo, gate.GOLDENS_MANIFEST, _default_goldens(deleted))
    _commit(repo, "re-add manifests")
    _expect(_manifests(repo, deleted, "--bootstrap"), 2, "BOOTSTRAP_NOT_ALLOWED")


# ===========================================================================
# Strict format
# ===========================================================================

def _parse_fails(raw, name="pytest-nodes", code="MANIFEST_FORMAT_INVALID"):
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.parse_manifest(raw, name)
    assert excinfo.value.code == code, excinfo.value.message
    assert excinfo.value.status == 2
    return excinfo.value


def test_a_well_formed_manifest_parses():
    parsed = gate.parse_manifest(_default_nodes(), "pytest-nodes")
    assert len(parsed.rows) == 2
    assert len(parsed.active) == 2
    assert parsed.tombstoned == []


@pytest.mark.parametrize(
    "mutate,label",
    [
        (lambda raw: b"\xef\xbb\xbf" + raw, "bom"),
        (lambda raw: raw.replace(b"\n", b"\r\n"), "crlf"),
        (lambda raw: raw + b"\n", "blank-line"),
        (lambda raw: raw.rstrip(b"\n"), "no-final-newline"),
        (lambda raw: b"", "empty"),
        (lambda raw: raw.replace(b'{"kind":"test"', b'  {"kind":"test"', 1), "indented"),
        (lambda raw: raw + b"not json\n", "invalid-json"),
        (lambda raw: raw + b"[1,2]\n", "not-an-object"),
        (lambda raw: raw.replace(b'"state":"active"', b'"state":"active","state":"x"', 1), "duplicate-key"),
        (lambda raw: raw.replace(b'"kind":"test"', b'"kind":"nope"', 1), "wrong-row-kind"),
        (lambda raw: raw.replace(b'"state":"active"', b'"state":"retired"', 1), "bad-state"),
        (lambda raw: raw.replace(b'"schema_version":1', b'"schema_version":2', 1), "schema-version"),
        (lambda raw: raw.replace(b'"manifest":"pytest-nodes"', b'"manifest":"goldens"', 1), "wrong-manifest"),
        (lambda raw: raw.replace(b'"bootstrap_base":"' + _FAKE_BASE.encode(), b'"bootstrap_base":"short', 1), "bad-bootstrap-sha"),
        (lambda raw: raw.replace(b'"minimum_active":2', b'"minimum_active":"2"', 1), "floor-not-int"),
        (lambda raw: raw.replace(b'"minimum_active":2', b'"minimum_active":-1', 1), "floor-negative"),
        (lambda raw: raw.replace(b'"id":"pytest-000001"', b'"id":"pytest-1"', 1), "bad-id-form"),
        (lambda raw: raw.replace(b'"id":"pytest-000002"', b'"id":"pytest-000009"', 1), "non-contiguous-id"),
        (lambda raw: raw.replace(b'"node_id":"tests/test_x.py::test_2"', b'"node_id":"tests/test_x.py::test_1"', 1), "duplicate-node-id"),
        (lambda raw: raw.replace(b'"node_id":"tests/test_x.py::test_1"', b'"node_id":"tests/kb/test_k.py::test_1"', 1), "kb-node"),
        (lambda raw: raw.replace(b'"node_id":"tests/test_x.py::test_1"', b'"node_id":"not a node"', 1), "malformed-node"),
    ],
)
def test_a_malformed_node_manifest_is_refused(mutate, label):
    _parse_fails(mutate(_default_nodes()))


def test_a_floor_above_the_active_row_count_is_refused():
    raw = _serialize(_node_header(active=99), [_node_row(1)])
    _parse_fails(raw, code="MANIFEST_FLOOR_INVALID")


@pytest.mark.parametrize(
    "field,value",
    [
        ("renderer", "no-such-renderer"),
        ("disposition", "maybe"),
        ("owner", "some-team"),
        ("owner", "#0"),
        ("input_case", ""),
    ],
    ids=["renderer", "disposition", "owner-freetext", "owner-zero", "empty-case"],
)
def test_a_golden_row_with_a_bad_enum_or_owner_is_refused(field, value):
    row = _golden_row(1)
    row[field] = value
    _parse_fails(_serialize(_golden_header(1), [row]), "goldens")


@pytest.mark.parametrize(
    "expected_file",
    [
        "/etc/passwd",
        "tests/fixtures/golden_xml/../../../etc/passwd",
        "tests/fixtures/other/g.xml",
        "tests/fixtures/golden_xml/nested/g.xml",
        "tests/fixtures/golden_xml/g.txt",
        "tests\\fixtures\\golden_xml\\g.xml",
        " tests/fixtures/golden_xml/g.xml",
    ],
    ids=["absolute", "traversal", "wrong-dir", "nested", "wrong-suffix",
         "backslash", "whitespace"],
)
def test_a_golden_row_with_an_unsafe_expected_file_is_refused(expected_file):
    row = _golden_row(1)
    row["expected_file"] = expected_file
    _parse_fails(_serialize(_golden_header(1), [row]), "goldens")


def test_field_ORDER_is_part_of_the_format():
    row = _node_row(1)
    reordered = {"id": row["id"], "kind": row["kind"], "node_id": row["node_id"],
                 "state": row["state"]}
    failure = _parse_fails(_serialize(_node_header(1), [reordered]))
    assert "ORDER" in failure.message


def test_an_unknown_or_missing_field_is_refused():
    row = _node_row(1)
    row["extra"] = 1
    _parse_fails(_serialize(_node_header(1), [row]))
    row = _node_row(1)
    del row["state"]
    _parse_fails(_serialize(_node_header(1), [row]))


def test_repeated_renderer_owner_and_disposition_are_LEGAL():
    """Immutable is not the same as unique.

    Two goldens routinely share a renderer, an owner and a disposition; only
    ``input_case`` and ``expected_file`` identify a row's subject. Conflating the
    two sets made the real 60-row manifest unparseable.
    """
    rows = [_golden_row(1), _golden_row(2)]
    parsed = gate.parse_manifest(_serialize(_golden_header(2), rows), "goldens")
    assert len({r["renderer"] for r in parsed.rows}) == 1


def test_a_duplicate_expected_file_or_input_case_is_refused():
    rows = [_golden_row(1), _golden_row(2, name="g1.xml")]
    _parse_fails(_serialize(_golden_header(2), rows), "goldens")
    rows = [_golden_row(1), _golden_row(2, input_case="case:1")]
    _parse_fails(_serialize(_golden_header(2), rows), "goldens")


# ===========================================================================
# Transitions
# ===========================================================================

def _transition(base_rows, head_rows, name="pytest-nodes",
                base_header=None, head_header=None):
    header = _node_header if name == "pytest-nodes" else _golden_header
    base = gate.parse_manifest(
        _serialize(base_header or header(len([r for r in base_rows if r["state"] == "active"])), base_rows),
        name,
    )
    head = gate.parse_manifest(
        _serialize(head_header or header(len([r for r in head_rows if r["state"] == "active"])), head_rows),
        name,
    )
    appended, tombstoned, _born = gate.validate_transition(base, head, name)
    return appended, tombstoned


def _transition_fails(base_rows, head_rows, name="pytest-nodes",
                      code="MANIFEST_TRANSITION_ILLEGAL", **kwargs):
    with pytest.raises(gate.GateFailure) as excinfo:
        _transition(base_rows, head_rows, name, **kwargs)
    assert excinfo.value.code == code, excinfo.value.message
    assert excinfo.value.status == 2


def test_appending_an_active_row_is_legal():
    appended, tombstoned = _transition(
        [_node_row(1)], [_node_row(1), _node_row(2)]
    )
    assert (appended, tombstoned) == (1, 0)


def test_an_unchanged_manifest_is_legal():
    assert _transition([_node_row(1)], [_node_row(1)]) == (0, 0)


def test_deleting_a_row_is_refused():
    _transition_fails([_node_row(1), _node_row(2)], [_node_row(1)])


@pytest.mark.parametrize(
    "disposition", ["survivor", "transitional_oracle", "deletion_only"]
)
def test_deleting_a_golden_row_is_refused_for_every_disposition(disposition):
    """The headline case: deleting a golden AND its row in one change.

    Self-consistent against the tree, and therefore invisible to any check that
    only looks at HEAD. It is refused whatever the row's disposition says, and
    by whichever of the two independent rules sees it first — which one depends
    only on WHERE the row sat:

    * deleting the LAST row leaves a parseable file, so the base-to-head row
      comparison is what catches it;
    * deleting any EARLIER row also breaks the positional id sequence, so the
      format check refuses the file before a transition is even computed.

    Both are exit 2 and both are fail-closed; asserting one universal code here
    would be asserting something untrue about the implementation.
    """
    base = [_golden_row(1, disposition=disposition), _golden_row(2)]

    # Delete the last row -> illegal transition.
    _transition_fails(base, [_golden_row(1, disposition=disposition)], "goldens")

    # Delete the first row -> the surviving row's id no longer matches its
    # position, so the file itself is refused.
    _transition_fails(base, [_golden_row(2)], "goldens",
                      code="MANIFEST_FORMAT_INVALID")


def test_reordering_or_inserting_before_the_end_is_refused():
    base = [_node_row(1), _node_row(2)]
    swapped = [dict(_node_row(2), id="pytest-000001"),
               dict(_node_row(1), id="pytest-000002")]
    _transition_fails(base, swapped)


def test_a_resorted_regeneration_is_refused():
    """Codex Stage-2 r2 [P1]. The way this ledger actually gets broken.

    Nobody hand-edits a 9,000-row manifest; they regenerate it. A regeneration
    that sorts every collected node id and renumbers from 1 REPOINTS every row
    whose alphabetical position shifted — measured on this repo, adding 7 tests
    moved 321 existing ids onto different tests. It must be refused, and the
    documented procedure must preserve existing rows and append instead.
    """
    base = [
        _node_row(1, node_id="tests/a.py::alpha"),
        _node_row(2, node_id="tests/a.py::gamma"),
    ]
    # "beta" sorts between them, so a re-sorted regeneration shifts gamma.
    resorted = [
        _node_row(1, node_id="tests/a.py::alpha"),
        _node_row(2, node_id="tests/a.py::beta"),
        _node_row(3, node_id="tests/a.py::gamma"),
    ]
    _transition_fails(base, resorted,
                      base_header=_node_header(2, 2), head_header=_node_header(3, 3))

    # The append-only form of the same change is legal: existing ids keep their
    # node, the newcomer goes at the end regardless of alphabetical order.
    appended = base + [_node_row(3, node_id="tests/a.py::beta")]
    assert _transition(base, appended,
                       base_header=_node_header(2, 2),
                       head_header=_node_header(3, 3)) == (1, 0)


def test_repointing_an_existing_id_is_refused():
    base = [_node_row(1)]
    _transition_fails(base, [_node_row(1, node_id="tests/test_y.py::test_other")])


@pytest.mark.parametrize(
    "field,value",
    [
        ("input_case", "case:changed"),
        ("renderer", "process-xml-v1"),
        ("expected_file", "tests/fixtures/golden_xml/renamed.xml"),
        ("owner", "#999"),
        ("disposition", "deletion_only"),
    ],
)
def test_mutating_any_immutable_golden_field_is_refused(field, value):
    base = [_golden_row(1)]
    head = dict(_golden_row(1))
    head[field] = value
    _transition_fails(base, [head], "goldens")


def test_tombstoning_an_active_row_is_legal_and_counted():
    appended, tombstoned = _transition(
        [_node_row(1), _node_row(2)],
        [_node_row(1), _node_row(2, state="tombstone")],
        base_header=_node_header(2, 2),
        head_header=_node_header(1, 1),
    )
    assert (appended, tombstoned) == (0, 1)


def test_reactivating_a_tombstone_is_refused():
    _transition_fails(
        [_node_row(1, state="tombstone")], [_node_row(1)],
        base_header=_node_header(0, 0), head_header=_node_header(1, 1),
    )


def test_appending_an_already_tombstoned_row_is_LEGAL():
    """A push range that adds a test in one commit and retires it in a later one
    is exactly this from the range's endpoints.

    Refusing it made an ordinary multi-commit push illegal even though every
    individual commit transition was legal — reproduced on this very branch,
    where `manifests --base <4 commits back>` exited 2. Nothing is lost: a
    tombstoned row must separately have no artifact, so it stays accounted for.
    """
    assert _transition(
        [_node_row(1)], [_node_row(1), _node_row(2, state="tombstone")],
        base_header=_node_header(1, 1), head_header=_node_header(1, 1),
    ) == (0, 0)

    # ...and the floor still has to track it: a born-tombstoned row adds nothing
    # to the active count, so claiming it does is refused.
    _transition_fails(
        [_node_row(1)], [_node_row(1), _node_row(2, state="tombstone")],
        base_header=_node_header(1, 1), head_header=_node_header(2, 1),
        code="MANIFEST_FLOOR_INVALID",
    )


def test_an_immutable_header_field_cannot_move():
    _transition_fails(
        [_node_row(1)], [_node_row(1)],
        base_header=_node_header(1, 1, base=_FAKE_BASE),
        head_header=_node_header(1, 1, base="2" * 40),
    )


# ===========================================================================
# Floor arithmetic
# ===========================================================================

def test_the_active_floor_must_track_appends_and_tombstones_exactly():
    # An append that does not raise the floor.
    _transition_fails(
        [_node_row(1)], [_node_row(1), _node_row(2)],
        base_header=_node_header(1, 1), head_header=_node_header(1, 1),
        code="MANIFEST_FLOOR_INVALID",
    )
    # A floor inflated beyond the append.
    _transition_fails(
        [_node_row(1)], [_node_row(1), _node_row(2)],
        base_header=_node_header(1, 1), head_header=_node_header(5, 1),
        code="MANIFEST_FLOOR_INVALID",
    )
    # A floor silently lowered with no tombstone at all.
    _transition_fails(
        [_node_row(1), _node_row(2)], [_node_row(1), _node_row(2)],
        base_header=_node_header(2, 2), head_header=_node_header(1, 2),
        code="MANIFEST_FLOOR_INVALID",
    )


def test_the_collection_floor_may_rise_freely_but_only_fall_by_a_tombstone():
    # Raising it is always fine — a slice may pin growth deliberately.
    assert _transition(
        [_node_row(1)], [_node_row(1)],
        base_header=_node_header(1, 10), head_header=_node_header(1, 4000),
    ) == (0, 0)
    # Dropping it without a tombstone is not.
    _transition_fails(
        [_node_row(1)], [_node_row(1)],
        base_header=_node_header(1, 10), head_header=_node_header(1, 9),
        code="MANIFEST_FLOOR_INVALID",
    )
    # Dropping it by exactly the tombstoned count is.
    assert _transition(
        [_node_row(1), _node_row(2)],
        [_node_row(1), _node_row(2, state="tombstone")],
        base_header=_node_header(2, 10), head_header=_node_header(1, 9),
    ) == (0, 1)


# ===========================================================================
# Collection
# ===========================================================================

def _nodes_manifest(active_ids, floor_collected, cap=30):
    rows = [_node_row(i, node_id=n) for i, n in enumerate(active_ids, start=1)]
    return gate.parse_manifest(
        _serialize(_node_header(len(rows), floor_collected, skipped=cap), rows),
        "pytest-nodes",
    )


def test_extra_collected_tests_are_allowed():
    manifest = _nodes_manifest(["tests/a.py::t1"], 1)
    gate.check_collection(manifest, {"tests/a.py::t1", "tests/a.py::t2"})


def test_collection_below_the_floor_fails_even_when_every_required_node_is_present():
    """The floor trips ON ITS OWN.

    Required-node coverage and the floor are separate assertions on purpose: a
    collection can contain every manifested node and still be a fraction of the
    suite, which is exactly what a partial import produces.
    """
    manifest = _nodes_manifest(["tests/a.py::t1"], 5000)
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.check_collection(manifest, {"tests/a.py::t1"})
    assert excinfo.value.code == "PYTEST_COLLECTION_FLOOR"
    assert excinfo.value.status == 1


def test_a_missing_required_node_fails_even_above_the_floor():
    manifest = _nodes_manifest(["tests/a.py::t1", "tests/a.py::t2"], 1)
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.check_collection(manifest, {"tests/a.py::t1", "tests/z.py::other"})
    assert excinfo.value.code == "PYTEST_NODE_MISSING"
    assert excinfo.value.status == 1


def test_tombstoned_nodes_are_not_required():
    rows = [_node_row(1, node_id="tests/a.py::t1"),
            _node_row(2, node_id="tests/a.py::gone", state="tombstone")]
    manifest = gate.parse_manifest(_serialize(_node_header(1, 1), rows), "pytest-nodes")
    gate.check_collection(manifest, {"tests/a.py::t1"})


def test_a_tombstoned_node_that_still_collects_is_refused():
    """Codex Stage-2 [P1]. The mirror of the golden rule, which was missing.

    `check_golden_tree` already refuses a tombstoned row whose file survives.
    Without the same rule for nodes, a retirement can be split across two
    changes: tombstone a test that is still there (legally lowering both floors),
    then delete it later with no manifest edit at all, because the floor
    reduction was prepaid and a tombstoned node is not required. Both runs pass.
    """
    rows = [_node_row(1, node_id="tests/a.py::t1"),
            _node_row(2, node_id="tests/a.py::retired", state="tombstone")]
    manifest = gate.parse_manifest(_serialize(_node_header(1, 1), rows), "pytest-nodes")
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.check_collection(manifest, {"tests/a.py::t1", "tests/a.py::retired"})
    assert excinfo.value.code == "PYTEST_NODE_TOMBSTONED_BUT_PRESENT"
    assert excinfo.value.status == 1


_GOOD_COLLECTION = "tests/a.py::t1\ntests/a.py::t2\n\n2 tests collected in 0.10s\n"


def test_a_healthy_collection_parses():
    assert gate.parse_collection_output(_GOOD_COLLECTION) == {
        "tests/a.py::t1", "tests/a.py::t2"
    }


@pytest.mark.parametrize(
    "stdout,code",
    [
        ("tests/a.py::t1\n", "PYTEST_COLLECTION_FAILED"),
        ("2 tests collected in 0.1s\n", "PYTEST_COLLECTION_EMPTY"),
        ("tests/a.py::t1\n1 tests collected\n2 tests collected\n", "PYTEST_COLLECTION_FAILED"),
        ("tests/a.py::t1\ntests/a.py::t1\n2 tests collected\n", "PYTEST_COLLECTION_DUPLICATE"),
        ("tests/a.py::t1\n9999 tests collected\n", "PYTEST_COLLECTION_FAILED"),
    ],
    ids=["no-summary", "no-nodes", "two-summaries", "duplicate-node", "does-not-reconcile"],
)
def test_unreconcilable_collection_output_is_refused(stdout, code):
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.parse_collection_output(stdout)
    assert excinfo.value.code == code
    assert excinfo.value.status == 1


# ---------------------------------------------------------------------------
# Execution accounting — a collected test is not an executed test
# ---------------------------------------------------------------------------

class _FakeSuite:
    """Stands in for the pytest child so these run in milliseconds."""

    def __init__(self, text, status=0):
        self.text = text
        self.status = status

    def install(self, monkeypatch):
        class _Proc:
            def __init__(self, text, status):
                self.stdout = iter(text.splitlines(keepends=True))
                self._status = status

            def wait(self):
                return self._status

        monkeypatch.setattr(
            gate.subprocess, "Popen",
            lambda *a, **k: _Proc(self.text, self.status),
        )


def _summary(passed, skipped=0, failed=0):
    parts = ["{0} passed".format(passed)]
    if failed:
        parts.append("{0} failed".format(failed))
    if skipped:
        parts.append("{0} skipped".format(skipped))
    return "dots\n" + ", ".join(parts) + " in 12.00s\n"


def _run_suite(monkeypatch, text, status=0, cap=30, collected=100):
    _FakeSuite(text, status).install(monkeypatch)
    manifest = _nodes_manifest(["tests/a.py::t1"], 1, cap=cap)
    return gate.run_suite("/nonexistent", manifest, set(range(collected)))


def test_a_healthy_suite_run_is_accounted(monkeypatch):
    result = _run_suite(monkeypatch, _summary(98, skipped=2))
    assert (result["passed"], result["skipped"], result["failed"]) == (98, 2, 0)


def test_mass_skipping_is_refused_even_though_pytest_exits_zero(monkeypatch):
    """The finding this check exists for.

    pytest collects a skipped test, counts it, and exits 0 — so one module-level
    ``pytestmark = pytest.mark.skip`` neutralises every test in that module with
    collection, the floor, the required-node check and the exit code all green.
    """
    with pytest.raises(gate.GateFailure) as excinfo:
        _run_suite(monkeypatch, _summary(0, skipped=100), cap=30)
    assert excinfo.value.code == "PYTEST_SKIPPED_EXCEEDS_CAP"
    assert excinfo.value.status == 1


def test_environment_conditional_skips_below_the_cap_are_tolerated(monkeypatch):
    """The cap is not zero on purpose: `gcloud` is on PATH here and not on a
    runner, and this suite has 22 runtime ``pytest.skip()`` sites."""
    result = _run_suite(monkeypatch, _summary(80, skipped=20), cap=30)
    assert result["skipped"] == 20


def test_a_pass_count_below_the_derived_floor_is_refused(monkeypatch):
    """Deselection, a collection-time filter, or an aborted run can leave the
    skip count small AND the pass count far short."""
    with pytest.raises(gate.GateFailure) as excinfo:
        _run_suite(monkeypatch, _summary(10, skipped=1), cap=30, collected=100)
    assert excinfo.value.code == "PYTEST_PASSED_BELOW_FLOOR"


def test_a_zero_exit_that_still_reports_failures_is_refused(monkeypatch):
    with pytest.raises(gate.GateFailure) as excinfo:
        _run_suite(monkeypatch, _summary(90, failed=3), status=0)
    assert excinfo.value.code == "PYTEST_FAILED"


def test_a_nonzero_exit_is_refused(monkeypatch):
    with pytest.raises(gate.GateFailure) as excinfo:
        _run_suite(monkeypatch, _summary(100), status=1)
    assert excinfo.value.code == "PYTEST_FAILED"


def test_an_unparseable_suite_summary_is_not_a_pass(monkeypatch):
    """No accountable result is not the same as a good result."""
    with pytest.raises(gate.GateFailure) as excinfo:
        _run_suite(monkeypatch, "....\nsomething went sideways\n")
    assert excinfo.value.code == "PYTEST_SUMMARY_UNPARSEABLE"


def test_the_skip_cap_may_be_lowered_but_not_raised():
    """Raising the cap is the exact move that launders a mass-skip past the gate."""
    assert _transition(
        [_node_row(1)], [_node_row(1)],
        base_header=_node_header(1, 1, skipped=30),
        head_header=_node_header(1, 1, skipped=5),
    ) == (0, 0)
    _transition_fails(
        [_node_row(1)], [_node_row(1)],
        base_header=_node_header(1, 1, skipped=30),
        head_header=_node_header(1, 1, skipped=31),
        code="MANIFEST_FLOOR_INVALID",
    )


def test_a_pytest_warning_line_is_not_counted_as_a_test():
    """pytest's warning summary also emits lines beginning ``tests/``.

    A loose ``startswith('tests/')`` filter counted them, which inflates the
    collected total — the one direction a floor cannot catch.
    """
    stdout = (
        "tests/a.py::t1\n"
        "tests/test_recipe_security.py:642: UserWarning: something\n"
        "1 test collected in 0.01s\n"
    )
    assert gate.parse_collection_output(stdout) == {"tests/a.py::t1"}


# ===========================================================================
# Golden tree self-consistency
# ===========================================================================

def _goldens_manifest(rows, active=None):
    if active is None:
        active = len([r for r in rows if r["state"] == "active"])
    return gate.parse_manifest(_serialize(_golden_header(active), rows), "goldens")


def test_a_consistent_golden_tree_passes(tmp_path):
    repo, _base = _seeded(tmp_path)
    gate.check_golden_tree(str(repo), _goldens_manifest([_golden_row(1), _golden_row(2)]))


def test_a_declared_golden_that_is_missing_fails(tmp_path):
    repo, _base = _seeded(tmp_path)
    (repo / "tests/fixtures/golden_xml/g2.xml").unlink()
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.check_golden_tree(str(repo), _goldens_manifest([_golden_row(1), _golden_row(2)]))
    assert excinfo.value.code == "GOLDEN_FILE_MISSING"
    assert excinfo.value.status == 1


def test_an_undeclared_golden_file_fails(tmp_path):
    """A golden with no row would never be executed by the gate."""
    repo, _base = _seeded(tmp_path)
    _write(repo, "tests/fixtures/golden_xml/g3.xml", "<x/>\n")
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.check_golden_tree(str(repo), _goldens_manifest([_golden_row(1), _golden_row(2)]))
    assert excinfo.value.code == "GOLDEN_FILE_UNDECLARED"


def test_a_tombstoned_golden_whose_file_survives_fails(tmp_path):
    repo, _base = _seeded(tmp_path)
    rows = [_golden_row(1), _golden_row(2, state="tombstone")]
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.check_golden_tree(str(repo), _goldens_manifest(rows))
    assert excinfo.value.code == "GOLDEN_FILE_UNDECLARED"


def test_a_tombstoned_golden_whose_file_is_gone_passes(tmp_path):
    repo, _base = _seeded(tmp_path)
    (repo / "tests/fixtures/golden_xml/g2.xml").unlink()
    rows = [_golden_row(1), _golden_row(2, state="tombstone")]
    gate.check_golden_tree(str(repo), _goldens_manifest(rows))


def test_a_directory_in_the_golden_corpus_is_refused(tmp_path):
    """Codex Stage-2 [P2]. A skipped directory made set equality vacuous.

    Manifest rows must name a file directly under GOLDEN_DIR, so a nested XML
    can never be declared — and skipping the directory left it out of `on_disk`
    too, so the equality check passed while the nested golden was rendered by
    nothing.
    """
    repo, _base = _seeded(tmp_path)
    nested = repo / "tests/fixtures/golden_xml/nested"
    nested.mkdir()
    (nested / "new.xml").write_text("<x/>\n", encoding="utf-8")
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.check_golden_tree(str(repo), _goldens_manifest([_golden_row(1), _golden_row(2)]))
    assert excinfo.value.code == "GOLDEN_FILE_UNDECLARED"
    assert "directory" in excinfo.value.message


def test_a_symlinked_golden_is_refused(tmp_path):
    repo, _base = _seeded(tmp_path)
    link = repo / "tests/fixtures/golden_xml/g3.xml"
    link.symlink_to(repo / "tests/fixtures/golden_xml/g1.xml")
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.check_golden_tree(str(repo), _goldens_manifest([_golden_row(1), _golden_row(2)]))
    assert excinfo.value.code == "GOLDEN_FILE_UNDECLARED"


# ===========================================================================
# Golden rendering: determinism and byte equality
# ===========================================================================

def _fake_renders(monkeypatch, first, second):
    passes = iter([first, second])

    def fake(repo, goldens, tmpdir, hashseed):
        return next(passes)

    monkeypatch.setattr(gate, "_render_pass", fake)


def test_two_disagreeing_renders_are_nondeterministic(tmp_path, monkeypatch):
    repo, _base = _seeded(tmp_path)
    manifest = _goldens_manifest([_golden_row(1)])
    _fake_renders(monkeypatch, {"golden-000001": b"a"}, {"golden-000001": b"b"})
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.check_goldens(str(repo), manifest, str(tmp_path))
    assert excinfo.value.code == "GOLDEN_NONDETERMINISTIC"
    assert excinfo.value.status == 1


def test_stable_but_wrong_bytes_are_a_mismatch(tmp_path, monkeypatch):
    """Determinism is not correctness: both passes agreeing proves only that the
    renderer is stable, which a hard-coded constant also is."""
    repo, _base = _seeded(tmp_path)
    manifest = _goldens_manifest([_golden_row(1)])
    _fake_renders(monkeypatch, {"golden-000001": b"wrong"}, {"golden-000001": b"wrong"})
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.check_goldens(str(repo), manifest, str(tmp_path))
    assert excinfo.value.code == "GOLDEN_MISMATCH"


def test_matching_deterministic_bytes_pass(tmp_path, monkeypatch):
    repo, _base = _seeded(tmp_path)
    manifest = _goldens_manifest([_golden_row(1)])
    _fake_renders(monkeypatch, {"golden-000001": b"<x/>\n"}, {"golden-000001": b"<x/>\n"})
    assert gate.check_goldens(str(repo), manifest, str(tmp_path)) == 1


@pytest.mark.parametrize(
    "first",
    [{}, {"golden-000001": b"<x/>\n", "golden-000009": b"extra"}],
    ids=["missing", "extra"],
)
def test_a_render_pass_that_does_not_cover_the_manifest_fails(tmp_path, monkeypatch, first):
    repo, _base = _seeded(tmp_path)
    manifest = _goldens_manifest([_golden_row(1)])
    _fake_renders(monkeypatch, first, {"golden-000001": b"<x/>\n"})
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.check_goldens(str(repo), manifest, str(tmp_path))
    assert excinfo.value.code == "GOLDEN_OUTPUT_SET_MISMATCH"


# ===========================================================================
# The #153 plan-fingerprint seam
# ===========================================================================

_ALL_KINDS = gate.REQUIRED_MUTATION_KINDS


class _StubProvider:
    """A provider honouring the #153 contract: (digest, canonical_material)."""

    def __init__(self, relocatable=True, discriminating=True, cases=("c1",),
                 mutations=_ALL_KINDS, mutation_relocatable=True,
                 material_leaks_identity=False, colliding=False, raises=False,
                 bad_shape=False, indistinct_kinds=False, bad_iterable=None):
        self._relocatable = relocatable
        self._discriminating = discriminating
        self._cases = list(cases)
        self._mutations = list(mutations)
        self._mutation_relocatable = mutation_relocatable
        self._material_leaks_identity = material_leaks_identity
        self._colliding = colliding
        self._raises = raises
        self._bad_shape = bad_shape
        self._indistinct_kinds = indistinct_kinds
        self._bad_iterable = bad_iterable

    def cases(self):
        if self._raises:
            raise RuntimeError("provider exploded")
        if self._bad_iterable == "string":
            return "c1"
        if self._bad_iterable == "none":
            return None
        if self._bad_iterable == "generator":
            def _gen():
                yield "c1"
                raise RuntimeError("exploded mid-iteration")
            return _gen()
        return self._cases

    def mutations(self, case):
        return self._mutations

    @staticmethod
    def _digest(material):
        return "sha256:" + hashlib.sha256(material).hexdigest()

    def fingerprint(self, case, *, account, environment, mutation=None):
        if self._bad_shape:
            return "just-a-digest"
        body = case if mutation is None else "{0}:{1}".format(case, mutation)
        if mutation is not None and not self._discriminating:
            body = case
        if mutation is not None and self._indistinct_kinds:
            body = "{0}:changed".format(case)   # same plan for every kind
        material = body.encode()
        if self._material_leaks_identity:
            material = "{0}:{1}:{2}".format(body, account, environment).encode()
        digest = self._digest(material)

        if mutation is not None:
            if self._colliding:
                digest = self._digest(case.encode())   # material moved, digest did not
            if not self._mutation_relocatable:
                material = "{0}:{1}".format(body, account).encode()
                digest = self._digest(material)
            return digest, material
        if not self._relocatable:
            digest = self._digest("{0}:{1}".format(case, account).encode())
        return digest, material


def test_the_pending_seam_is_informational_by_default():
    assert gate.run_plan_fingerprint_checks(False) == "pending:#153"


def test_the_pending_seam_fails_when_required():
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.run_plan_fingerprint_checks(True)
    assert excinfo.value.code == "PLAN_FINGERPRINT_PENDING"
    assert excinfo.value.status == 1


def test_a_healthy_provider_passes():
    assert "checked" in gate.run_plan_fingerprint_checks(True, _StubProvider())


def test_a_provider_whose_fingerprint_moves_on_relocation_fails():
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.run_plan_fingerprint_checks(True, _StubProvider(relocatable=False))
    assert excinfo.value.code == "PLAN_FINGERPRINT_MISMATCH"


def test_a_provider_whose_fingerprint_ignores_semantics_fails():
    """Relocation-stability alone is satisfied by a constant.

    Both halves are asserted, so a fingerprint that never changes cannot pass by
    being trivially relocatable.
    """
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.run_plan_fingerprint_checks(True, _StubProvider(discriminating=False))
    assert excinfo.value.code == "PLAN_FINGERPRINT_MISMATCH"


def test_a_provider_whose_MUTATED_plans_are_not_relocatable_fails():
    """Codex Stage-2 [P2]. Relocatability is a property of every plan.

    Checking mutations under one identity only accepts a provider that is
    identity-independent for the base case and account-dependent the moment
    anything changes — not a relocatable fingerprint, just one that looks
    relocatable in the single place it was measured.
    """
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.run_plan_fingerprint_checks(
            True, _StubProvider(mutation_relocatable=False)
        )
    assert excinfo.value.code == "PLAN_FINGERPRINT_MISMATCH"
    assert "relocatable" in excinfo.value.message
    # The same provider passes every check the unmutated path makes, which is
    # what made this reachable.
    assert gate.run_plan_fingerprint_checks(True, _StubProvider()) .startswith("checked")


@pytest.mark.parametrize(
    "kwargs",
    [{"cases": ()}, {"mutations": ()}, {"mutations": ("semantic",)}],
    ids=["no-cases", "no-mutations", "only-semantic"],
)
def test_a_vacuous_provider_fails(kwargs):
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.run_plan_fingerprint_checks(True, _StubProvider(**kwargs))
    assert excinfo.value.code == "PLAN_FINGERPRINT_MISMATCH"


# ===========================================================================
# Hygiene and the exit contract
# ===========================================================================

def test_a_gate_that_changes_the_worktree_is_a_failure():
    gate.check_worktree_unchanged("", "")
    gate.check_worktree_unchanged(" M a\n", " M a\n")  # dirty in, dirty out: fine
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.check_worktree_unchanged("", "?? droppings\n")
    assert excinfo.value.code == "WORKTREE_DIRTY"
    assert excinfo.value.status == 1


def test_manifests_announces_that_it_is_not_a_gate(tmp_path):
    repo, base = _seeded(tmp_path)
    status, stderr = _manifests(repo, base)
    assert status == 0, stderr
    assert "NOT A GATE" in stderr


def test_the_ci_subcommand_refuses_a_bare_base(tmp_path):
    """``ci`` derives its baseline from the event, never from an argument."""
    repo, base = _seeded(tmp_path)
    status, _stderr = _gate(repo, "ci", "--base", base)
    assert status == 2


def test_there_is_no_mutation_or_skip_surface():
    """The gate must not be able to update a manifest or skip a check.

    An ``--update`` that regenerates the ledger would end the append-only
    contract on its first use, and a ``--skip`` makes every failure optional.
    """
    parser = gate.build_parser()
    text = parser.format_help()
    for banned in ("--update", "--skip", "--force", "--no-", "--minimum"):
        assert banned not in text, banned
    source = (_ROOT / "scripts" / "wave_gate.py").read_text(encoding="utf-8")
    assert 'add_argument("--update' not in source
    assert 'add_argument("--skip' not in source


def test_every_diagnostic_code_the_gate_can_raise_is_documented():
    """Codes are the contract CI logs and this matrix key on, so they are also
    the thing a reader looks up. A code that exists only in the source is one
    nobody can act on; a code documented but never raised is a promise the gate
    does not keep. Assert the two sets agree EXACTLY, in both directions.
    """
    import re as _re

    assert gate._contract("X", "m").status == 2
    assert gate._invalid("X", "m").status == 1

    source = (_ROOT / "scripts" / "wave_gate.py").read_text(encoding="utf-8")
    raised = set(_re.findall(r'_(?:contract|invalid)\(\s*\n?\s*"([A-Z_]+)"', source))
    assert len(raised) >= 20, sorted(raised)

    doc = (_ROOT / "docs" / "architecture" / "ENDGAME_VERIFICATION_GATE.md").read_text(
        encoding="utf-8"
    )
    # Scan ONLY the code roster, not the whole document: elsewhere the doc
    # legitimately back-ticks identifiers that are not diagnostics
    # (PLAN_FINGERPRINT_PROVIDER, PYTHONHASHSEED, SYNC_CASES, ...), and a
    # prefix filter cannot tell those apart from the codes they resemble.
    marker = "Every failure prints a stable diagnostic code as the first stderr token:"
    assert marker in doc, "the code roster's heading moved; re-anchor this test"
    roster = doc.split(marker, 1)[1].split("\n## ", 1)[0]
    documented = set(_re.findall(r"`([A-Z][A-Z_]{4,})`", roster))

    assert raised - documented == set(), sorted(raised - documented)
    assert documented - raised == set(), sorted(documented - raised)


def test_the_workflow_invokes_the_real_gate_and_isolates_push_runs():
    """The workflow is part of the contract, so pin the parts that fail open.

    * It must call `wave_gate.py ci` — not `pytest` directly, which can go green
      on a partial collection.
    * Nothing may soften a failure.
    * Pushes must NOT share a concurrency group (Codex Stage-2 [P2]): GitHub
      cancels a previously PENDING run when a new one enters the group whatever
      `cancel-in-progress` says, so three rapid pushes to `dev` would leave the
      middle commit with no verdict — on the branch whose protection is this
      check.
    """
    raw = (_ROOT / ".github" / "workflows" / "tests.yml").read_text(encoding="utf-8")
    # Comments legitimately NAME the softeners in order to say they are absent,
    # so scan the directives only.
    workflow = "\n".join(
        line for line in raw.splitlines() if not line.lstrip().startswith("#")
    )
    assert "scripts/wave_gate.py ci --github-event" in workflow
    assert "python-version: \"3.11\"" in workflow
    assert "requirements-dev.txt" in workflow
    assert "fetch-depth: 0" in workflow          # PR merge-base needs full history
    for softener in ("continue-on-error", "|| true", "if: always()"):
        assert softener not in workflow, softener
    # Push runs are keyed per commit, PR runs per ref.
    assert "github.event_name == 'push'" in workflow
    assert "github.sha" in workflow
    assert "cancel-in-progress: ${{ github.event_name == 'pull_request' }}" in workflow


# ===========================================================================
# §6 architect-review findings
# ===========================================================================

def test_the_checkout_must_be_the_tree_the_event_describes(tmp_path, monkeypatch):
    """§6 finding 1. Baseline from one place, evidence from another.

    Without this the event can describe a PR carrying an illegal rewrite while
    the gate validates some other, valid checkout. GitHub's own actions keep the
    two in step, which is exactly why it must be asserted rather than assumed.
    """
    repo, base = _seeded(tmp_path)
    head = _commit(repo, "the real head")
    other = _commit(repo, "a different commit that is checked out")

    # push arm: event says `after` is `head`, but HEAD is `other`.
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.check_checkout_matches_event(
            str(repo), {"kind": "push", "target": base, "after": head}
        )
    assert excinfo.value.code == "CHECKOUT_EVENT_MISMATCH"
    # ...and it passes when they agree.
    gate.check_checkout_matches_event(
        str(repo), {"kind": "push", "target": base, "after": other}
    )

    # PR arm: neither the head nor a merge of head+target.
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.check_checkout_matches_event(
            str(repo),
            {"kind": "pull_request", "target": base, "event_head": head},
        )
    assert excinfo.value.code == "CHECKOUT_EVENT_MISMATCH"


def test_a_pr_merge_checkout_is_accepted(tmp_path):
    """`refs/pull/N/merge` is the normal PR checkout and must not be refused."""
    repo, base = _seeded(tmp_path)
    _run_git(repo, "checkout", "-q", "-b", "feature")
    head = _commit(repo, "feature")
    _run_git(repo, "checkout", "-q", "main")
    target = _commit(repo, "target moves")
    _run_git(
        repo, "-c", "user.email=gate@example.invalid", "-c", "user.name=gate",
        "merge", "-q", "--no-edit", head,
    )
    gate.check_checkout_matches_event(
        str(repo),
        {"kind": "pull_request", "target": target, "event_head": head,
         "merge_sha": _git_out(repo, "rev-parse", "HEAD")},
    )


@pytest.mark.parametrize(
    "make_second,rev",
    [
        (lambda repo, sha: _run_git(repo, "tag", "ambiguous", sha), "ambiguous"),
        # `refs/<name>` is outside heads/tags/remotes entirely — the namespace
        # enumeration this replaced could not see it, and git silently resolved
        # to it.
        (lambda repo, sha: _run_git(repo, "update-ref", "refs/ambiguous", sha),
         "ambiguous"),
        # ...and ambiguity inside a revision EXPRESSION, which the enumeration
        # could not have parsed at all.
        (lambda repo, sha: _run_git(repo, "tag", "ambiguous", sha), "ambiguous~0"),
    ],
    ids=["branch-vs-tag", "branch-vs-bare-ref", "rev-expression"],
)
def test_an_ambiguous_local_baseline_is_refused(tmp_path, make_second, rev):
    """§6 finding 3, re-fixed. Ask git rather than enumerate namespaces."""
    repo, base = _seeded(tmp_path)
    other = _commit(repo, "a second commit")
    _run_git(repo, "branch", "ambiguous", base)
    make_second(repo, other)
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.resolve_baseline(str(repo), base=rev)
    assert excinfo.value.code == "BASELINE_UNAVAILABLE"
    assert "ambiguous" in excinfo.value.message


def test_an_unambiguous_local_baseline_is_accepted(tmp_path):
    repo, base = _seeded(tmp_path)
    assert gate.resolve_baseline(str(repo), base=base)["sha"] == base
    assert gate.resolve_baseline(str(repo), base="HEAD")["sha"] == base


def test_a_pr_bootstrap_must_be_anchored_to_the_target(tmp_path):
    """§6 finding 2a. Otherwise the PR is green and the resulting push is red."""
    repo = _new_repo(tmp_path)
    for name in ("g1.xml", "g2.xml"):
        _write(repo, "tests/fixtures/golden_xml/{0}".format(name), "<x/>\n")
    declared = _commit(repo, "the declared bootstrap base")
    _run_git(repo, "checkout", "-q", "-b", "feature")
    _write(repo, gate.NODES_MANIFEST, _default_nodes(declared))
    _write(repo, gate.GOLDENS_MANIFEST, _default_goldens(declared))
    head = _commit(repo, "introduce the manifests")
    _run_git(repo, "checkout", "-q", "main")
    target = _commit(repo, "target advances past the declared base")
    _run_git(repo, "checkout", "-q", "feature")

    event = tmp_path / "pr.json"
    event.write_text(json.dumps(
        {"pull_request": {"head": {"sha": head}, "base": {"sha": target}}}
    ), encoding="utf-8")
    proc = subprocess.run(
        [sys.executable, str(_ROOT / "scripts" / "wave_gate.py"),
         "--repo", str(repo), "manifests", "--github-event", str(event),
         "--event-name", "pull_request"],
        capture_output=True, text=True,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
    )
    assert proc.returncode == 2, proc.stderr
    assert "BOOTSTRAP_NOT_ALLOWED" in proc.stderr


def test_the_active_floor_must_EQUAL_the_row_count():
    """§6 finding 6a. Bootstrap skips transition arithmetic, so a `>=` floor
    would let the introducing change commit a permanently weakened number."""
    rows = [_node_row(1), _node_row(2)]
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.parse_manifest(_serialize(_node_header(1, 1), rows), "pytest-nodes")
    assert excinfo.value.code == "MANIFEST_FLOOR_INVALID"


def test_a_renderer_that_skips_is_a_failure_not_a_skip():
    """§6 finding 5. A skipped renderer would leave a golden unrendered while
    the suite stays green inside the skip cap."""
    import _wave_gate_golden_corpus as corpus

    original = dict(corpus.CASE_REGISTRY)
    try:
        def _skipping():
            pytest.skip("a producer helper opted out")

        corpus.CASE_REGISTRY["probe:skipping"] = ("process-xml-v1", _skipping)
        with pytest.raises(corpus.RendererMismatch) as excinfo:
            corpus.render_golden_case("probe:skipping", "process-xml-v1")
        assert "may not opt out" in str(excinfo.value)
    finally:
        corpus.CASE_REGISTRY.clear()
        corpus.CASE_REGISTRY.update(original)


def test_invalid_utf8_and_symlinked_manifests_are_refused(tmp_path):
    """§6 finding 7. Named negatives that were missing from the matrix."""
    _parse_fails(b'{"kind":"manifest"}\n\xff\xfe not utf-8\n')

    repo, _base = _seeded(tmp_path)
    target = repo / gate.NODES_MANIFEST
    payload = target.read_bytes()
    target.unlink()
    (repo / "elsewhere.jsonl").write_bytes(payload)
    target.symlink_to(repo / "elsewhere.jsonl")
    with pytest.raises(gate.GateFailure) as excinfo:
        gate._load_current(str(repo))
    assert excinfo.value.code == "MANIFEST_FORMAT_INVALID"
    assert "symlink" in excinfo.value.message


def test_multiple_merge_bases_are_refused(tmp_path):
    """§6 finding 7. A criss-cross merge has no unique baseline."""
    repo, base = _seeded(tmp_path)
    _run_git(repo, "checkout", "-q", "-b", "a")
    a1 = _commit(repo, "a1")
    _run_git(repo, "checkout", "-q", "main")
    b1 = _commit(repo, "b1")
    _run_git(repo, "checkout", "-q", "-b", "cross-a", a1)
    _run_git(repo, "-c", "user.email=g@e.invalid", "-c", "user.name=g",
             "merge", "-q", "--no-edit", b1)
    head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=str(repo),
                          capture_output=True, text=True, check=True).stdout.strip()
    _run_git(repo, "checkout", "-q", "-b", "cross-b", b1)
    _run_git(repo, "-c", "user.email=g@e.invalid", "-c", "user.name=g",
             "merge", "-q", "--no-edit", a1)
    target = subprocess.run(["git", "rev-parse", "HEAD"], cwd=str(repo),
                            capture_output=True, text=True, check=True).stdout.strip()

    event = tmp_path / "pr.json"
    event.write_text(json.dumps(
        {"pull_request": {"head": {"sha": head}, "base": {"sha": target}}}
    ), encoding="utf-8")
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.resolve_baseline(str(repo), event_path=str(event),
                              event_name="pull_request")
    assert excinfo.value.code == "BASELINE_MERGE_BASE_AMBIGUOUS"


def test_the_fingerprint_seam_demands_canonical_material():
    """§6 finding 4. A digest alone is satisfied by a constant."""
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.run_plan_fingerprint_checks(True, _StubProvider(bad_shape=True))
    assert excinfo.value.code == "PLAN_FINGERPRINT_MISMATCH"

    # Material that carries the account is not relocatable, even if the digest is.
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.run_plan_fingerprint_checks(
            True, _StubProvider(material_leaks_identity=True)
        )
    assert "material" in excinfo.value.message

    # Material moved but the digest did not — a collision the gate must refuse.
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.run_plan_fingerprint_checks(True, _StubProvider(colliding=True))
    assert excinfo.value.code == "PLAN_FINGERPRINT_MISMATCH"


def test_a_provider_that_raises_stays_on_the_diagnostic_path():
    """§6 finding 4. Every refusal carries a stable code, including this one."""
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.run_plan_fingerprint_checks(True, _StubProvider(raises=True))
    assert excinfo.value.code == "PLAN_FINGERPRINT_MISMATCH"
    assert "RuntimeError" in excinfo.value.message


def test_the_worktree_fingerprint_notices_an_in_place_edit(tmp_path):
    """§6 finding 8. Porcelain status alone is blind to editing a dirty file."""
    repo, _base = _seeded(tmp_path)
    tracked = repo / "README.md"
    tracked.write_text("seed\nmodified once\n", encoding="utf-8")
    before = gate._status(str(repo))
    tracked.write_text("seed\nmodified once\nand again\n", encoding="utf-8")
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.check_worktree_unchanged(before, gate._status(str(repo)))
    assert excinfo.value.code == "WORKTREE_DIRTY"


def test_the_worktree_fingerprint_is_content_not_line_counts(tmp_path):
    """A SAME-LENGTH rewrite of an already-dirty file, and any rewrite of an
    existing untracked file, both leave `--numstat` and porcelain identical."""
    repo, _base = _seeded(tmp_path)

    tracked = repo / "README.md"
    tracked.write_text("seed\naaaa\n", encoding="utf-8")
    before = gate._status(str(repo))
    tracked.write_text("seed\nbbbb\n", encoding="utf-8")   # same line count
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.check_worktree_unchanged(before, gate._status(str(repo)))
    assert excinfo.value.code == "WORKTREE_DIRTY"

    untracked = repo / "scratch.txt"
    untracked.write_text("one\n", encoding="utf-8")
    before = gate._status(str(repo))
    untracked.write_text("two\n", encoding="utf-8")          # same status letter
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.check_worktree_unchanged(before, gate._status(str(repo)))
    assert excinfo.value.code == "WORKTREE_DIRTY"


def test_the_worktree_fingerprint_discloses_no_file_content(tmp_path):
    """[Critical] The fingerprint is printed verbatim in a WORKTREE_DIRTY
    diagnostic, so an uncommitted credential would go straight into the log.

    Reproduced before the fix with `TOKEN=SUPER_SECRET_VALUE`. Digests compare
    exactly as well and disclose nothing.
    """
    secret = "SUPER_SECRET_VALUE_" + "x" * 8
    repo = _new_repo(tmp_path)
    tracked = repo / "conf.env"
    tracked.write_text("TOKEN=placeholder\n", encoding="utf-8")
    _commit(repo, "seed")
    tracked.write_text("TOKEN={0}\n".format(secret), encoding="utf-8")
    (repo / "extra.env").write_text("OTHER={0}\n".format(secret), encoding="utf-8")

    work = str(repo)
    fingerprint = gate._status(work)
    assert secret not in fingerprint, "the fingerprint leaks file content"
    # ...and it still notices a change to those same files.
    before = fingerprint
    tracked.write_text("TOKEN=different\n", encoding="utf-8")
    with pytest.raises(gate.GateFailure):
        gate.check_worktree_unchanged(before, gate._status(work))


def test_a_file_that_cannot_be_hashed_fails_closed(tmp_path, monkeypatch):
    """[Critical] A stable `<unreadable errno>` token made the CONTENT invisible:
    chmod / write / chmod produced identical snapshots across a real mutation.

    The read failure is SIMULATED rather than produced with `chmod(000)`: as UID
    0 — which container-based CI routinely is — the mode bits do not deny root,
    so a permission-based test would pass locally and go red on a runner for a
    reason that has nothing to do with the behaviour under test.
    """
    repo, _base = _seeded(tmp_path)
    blocked = repo / "locked.bin"
    blocked.write_bytes(b"one\n")

    real_open = open

    def _refusing_open(path, *args, **kwargs):
        if os.fsdecode(path).endswith("locked.bin"):
            raise PermissionError(13, "Permission denied")
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr("builtins.open", _refusing_open)
    with pytest.raises(gate.GateFailure) as excinfo:
        gate._status(str(repo))
    assert excinfo.value.code == "WORKTREE_DIRTY"
    assert "cannot fingerprint" in excinfo.value.message


def test_a_pr_checkout_must_be_the_merge_commit_the_event_names(tmp_path, monkeypatch):
    """[Standard] Parentage proves shape, not content: a commit with exactly
    {head, target} as parents but the TARGET's tree — omitting every change the
    PR makes — satisfied the old check."""
    repo, base = _seeded(tmp_path)
    _run_git(repo, "checkout", "-q", "-b", "feature")
    head = _commit(repo, "feature work")
    _run_git(repo, "checkout", "-q", "main")
    target = _commit(repo, "target")
    _run_git(repo, "-c", "user.email=g@e.invalid", "-c", "user.name=g",
             "merge", "-q", "--no-edit", "-s", "ours", head)   # target's tree
    impostor = _git_out(repo, "rev-parse", "HEAD")

    ctx = {"kind": "pull_request", "target": target, "event_head": head,
           "merge_sha": "9" * 40}
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.check_checkout_matches_event(str(repo), ctx)
    assert excinfo.value.code == "CHECKOUT_EVENT_MISMATCH"
    # The commit GitHub actually names is accepted.
    ctx["merge_sha"] = impostor
    gate.check_checkout_matches_event(str(repo), ctx)


def test_a_provider_GateFailure_cannot_carry_its_own_exit_status():
    """[Standard] `GateFailure(..., 0)` from a provider propagated out and exited
    the wave green."""
    class _Sneaky(_StubProvider):
        def cases(self):
            raise gate.GateFailure("PLAN_FINGERPRINT_MISMATCH", "green please", 0)

    with pytest.raises(gate.GateFailure) as excinfo:
        gate.run_plan_fingerprint_checks(True, _Sneaky())
    assert excinfo.value.status == 1


class _Proc:
    """A stand-in for a completed git process."""

    def __init__(self, stderr=b"", returncode=0, stdout=b""):
        self.stderr = stderr
        self.returncode = returncode
        self.stdout = stdout


def test_an_unreadable_directory_fails_closed():
    """[Critical] Exit code alone is not accountability.

    With an unreadable DIRECTORY, `git status` and `git diff` both exit 0, warn
    on stderr, and silently omit every file underneath — so a mutation in there
    produced an identical fingerprint and the per-file `open()` guard was never
    reached. Verified against real git: `status_rc=0` with
    `could not open directory 'locked/': Permission denied`.

    The warning is SIMULATED rather than produced with `chmod(000)`: as UID 0 —
    which container CI routinely is — the mode bits do not deny root, git emits
    nothing, and a permission-based test would go red on the runner for a reason
    unrelated to the behaviour under test.
    """
    with pytest.raises(gate.GateFailure) as excinfo:
        gate._refuse_unreadable(
            _Proc(stderr=b"warning: could not open directory 'locked/': "
                         b"Permission denied\n"),
            "git status",
        )
    assert excinfo.value.code == "WORKTREE_DIRTY"
    assert "could not account for the whole worktree" in excinfo.value.message


@pytest.mark.parametrize(
    "stderr",
    [b"", b"warning: confstr() failed: Operation timed out; using /tmp instead\n"],
    ids=["silent", "benign-process-warning"],
)
def test_benign_git_diagnostics_do_not_disable_the_gate(stderr):
    """Refusing on ANY stderr made the required gate refuse every invocation in
    environments that emit harmless process warnings alongside complete output.

    Over-matching disables the check entirely; under-matching leaves only the
    narrow residual that existed before. A gate that cannot run protects nothing.
    """
    gate._refuse_unreadable(_Proc(stderr=stderr), "git status")


def test_a_nonzero_git_exit_still_refuses():
    with pytest.raises(gate.GateFailure):
        gate._refuse_unreadable(_Proc(stderr=b"boom", returncode=128), "git diff")


def test_a_born_tombstoned_row_is_still_reported(tmp_path):
    """[Standard] It changes no floor, but it IS a retirement record.

    Excluding born-tombstones from the audit made the documented compensating
    control — "every tombstone transition is reported with its immutable owner
    and disposition" — quietly skip them.
    """
    base_rows = [_golden_row(1)]
    head_rows = [_golden_row(1), _golden_row(2, owner="#159",
                                             disposition="transitional_oracle",
                                             state="tombstone")]
    appended, tombstoned, born = gate.validate_transition(
        gate.parse_manifest(_serialize(_golden_header(1), base_rows), "goldens"),
        gate.parse_manifest(_serialize(_golden_header(1), head_rows), "goldens"),
        "goldens",
    )
    assert (appended, tombstoned) == (0, 0)
    assert [row["id"] for row in born] == ["golden-000002"]
    assert born[0]["owner"] == "#159"
    assert born[0]["disposition"] == "transitional_oracle"


def test_a_provider_raising_SystemExit_cannot_exit_green():
    """`SystemExit` derives from BaseException; catching only `Exception` let a
    provider terminate the wave with status 0 and skip final hygiene."""
    class _Exiting(_StubProvider):
        def cases(self):
            raise SystemExit(0)

    with pytest.raises(gate.GateFailure) as excinfo:
        gate.run_plan_fingerprint_checks(True, _Exiting())
    assert excinfo.value.code == "PLAN_FINGERPRINT_MISMATCH"
    assert excinfo.value.status == 1


def test_a_renderer_raising_unittest_SkipTest_is_a_failure():
    """pytest honours `unittest.SkipTest` as a skip, so re-raising it left a
    golden unrendered while the run stayed green under the cap."""
    import unittest

    import _wave_gate_golden_corpus as corpus

    original = dict(corpus.CASE_REGISTRY)
    try:
        def _skipping():
            raise unittest.SkipTest("opted out")

        corpus.CASE_REGISTRY["probe:unittest-skip"] = ("process-xml-v1", _skipping)
        with pytest.raises(corpus.RendererMismatch):
            corpus.render_golden_case("probe:unittest-skip", "process-xml-v1")
    finally:
        corpus.CASE_REGISTRY.clear()
        corpus.CASE_REGISTRY.update(original)


def test_a_null_merge_commit_sha_falls_back_to_GITHUB_SHA(tmp_path, monkeypatch):
    """`pull_request.merge_commit_sha` is NULLABLE.

    GitHub computes mergeability asynchronously and sends null until it settles,
    while `actions/checkout` still checks out `refs/pull/N/merge` — so requiring
    the payload field would reject legitimate PR runs before a test executed.
    Actions always sets `GITHUB_SHA` to the merge commit it built.
    """
    repo, base = _seeded(tmp_path)
    _run_git(repo, "checkout", "-q", "-b", "feature")
    head = _commit(repo, "feature work")
    _run_git(repo, "checkout", "-q", "main")
    target = _commit(repo, "target")
    _run_git(repo, "-c", "user.email=g@e.invalid", "-c", "user.name=g",
             "merge", "-q", "--no-edit", head)
    merge = _git_out(repo, "rev-parse", "HEAD")

    ctx = {"kind": "pull_request", "target": target, "event_head": head,
           "merge_sha": None}
    monkeypatch.setenv("GITHUB_SHA", merge)
    gate.check_checkout_matches_event(str(repo), ctx)      # accepted

    # ...but a GITHUB_SHA that is not this checkout is still refused.
    monkeypatch.setenv("GITHUB_SHA", "9" * 40)
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.check_checkout_matches_event(str(repo), ctx)
    assert excinfo.value.code == "CHECKOUT_EVENT_MISMATCH"

    # And GITHUB_SHA WINS when both are present and disagree: the payload's
    # merge sha is computed asynchronously and can lag the workflow context, so
    # trusting it first would accept a stale commit and reject the real one.
    ctx_stale = dict(ctx, merge_sha=merge)
    monkeypatch.setenv("GITHUB_SHA", "9" * 40)
    with pytest.raises(gate.GateFailure):
        gate.check_checkout_matches_event(str(repo), ctx_stale)
    monkeypatch.setenv("GITHUB_SHA", merge)
    gate.check_checkout_matches_event(str(repo), dict(ctx, merge_sha="9" * 40))


def test_a_pr_checkout_without_an_authoritative_merge_sha_is_refused(
    tmp_path, monkeypatch
):
    """Parentage is not evidence, so its absence is not a licence.

    Falling back to a parent-only check when the event carries no
    `merge_commit_sha` reopens the hole it was added to close: a commit with
    parents {head, target} and an arbitrary tree satisfies the shape while
    containing none of the PR's changes.
    """
    repo, base = _seeded(tmp_path)
    _run_git(repo, "checkout", "-q", "-b", "feature")
    head = _commit(repo, "feature work")
    _run_git(repo, "checkout", "-q", "main")
    target = _commit(repo, "target")
    _run_git(repo, "-c", "user.email=g@e.invalid", "-c", "user.name=g",
             "merge", "-q", "--no-edit", "-s", "ours", head)

    with pytest.raises(gate.GateFailure) as excinfo:
        gate.check_checkout_matches_event(
            str(repo),
            {"kind": "pull_request", "target": target, "event_head": head,
             "merge_sha": None},
        )
    assert excinfo.value.code == "CHECKOUT_EVENT_MISMATCH"
    assert "neither GITHUB_SHA" in excinfo.value.message


# ===========================================================================
# Repo review of the §6 fix delta
# ===========================================================================


def test_the_worktree_fingerprint_reaches_inside_untracked_directories(tmp_path):
    """[P1] `--untracked-files=normal` collapses a directory to one `?? dir/`.

    Rewriting a file inside it then left both snapshots identical, so the gate
    could mutate user bytes and still pass hygiene.
    """
    repo, _base = _seeded(tmp_path)
    nested = repo / "scratchdir"
    nested.mkdir()
    (nested / "note.txt").write_text("one\n", encoding="utf-8")
    before = gate._status(str(repo))
    (nested / "note.txt").write_text("two\n", encoding="utf-8")
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.check_worktree_unchanged(before, gate._status(str(repo)))
    assert excinfo.value.code == "WORKTREE_DIRTY"


def test_ambiguity_detection_does_not_depend_on_repo_config_or_locale(tmp_path):
    """[P2] `core.warnAmbiguousRefs=false` silenced the warning being parsed."""
    repo, base = _seeded(tmp_path)
    other = _commit(repo, "second")
    _run_git(repo, "config", "core.warnAmbiguousRefs", "false")
    _run_git(repo, "branch", "ambiguous", base)
    _run_git(repo, "tag", "ambiguous", other)
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.resolve_baseline(str(repo), base="ambiguous")
    assert excinfo.value.code == "BASELINE_UNAVAILABLE"


def test_an_event_run_requires_a_clean_tree(tmp_path):
    """[P1] Otherwise the gate validates bytes the event does not contain."""
    repo, base = _seeded(tmp_path)
    head = _commit(repo, "tip")
    (repo / "README.md").write_text("edited on the runner\n", encoding="utf-8")
    event = tmp_path / "push.json"
    event.write_text(json.dumps({"before": base, "after": head}), encoding="utf-8")
    proc = subprocess.run(
        [sys.executable, str(_ROOT / "scripts" / "wave_gate.py"),
         "--repo", str(repo), "manifests", "--github-event", str(event),
         "--event-name", "push"],
        capture_output=True, text=True,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
    )
    assert proc.returncode == 2, proc.stderr
    assert "CHECKOUT_EVENT_MISMATCH" in proc.stderr


@pytest.mark.parametrize(
    "after", [None, "", "not-a-sha", _ZERO], ids=["missing", "empty", "nonhex", "zero"]
)
def test_a_push_without_a_usable_after_is_refused(tmp_path, after):
    """[P2] Degrading `after` to None made the binding skip its own comparison."""
    repo, base = _seeded(tmp_path)
    payload = {"before": base}
    if after is not None:
        payload["after"] = after
    event = tmp_path / "push.json"
    event.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.resolve_baseline(str(repo), event_path=str(event), event_name="push")
    assert excinfo.value.code == "BASELINE_EVENT_INVALID"


def test_a_renderer_that_xfails_is_also_a_failure():
    """[P2] `XFailed` subclasses `Failed`; an exact-name match missed it, and an
    xfailed golden test is GREEN."""
    import _wave_gate_golden_corpus as corpus

    original = dict(corpus.CASE_REGISTRY)
    try:
        def _xfailing():
            pytest.xfail("a producer helper opted out")

        corpus.CASE_REGISTRY["probe:xfail"] = ("process-xml-v1", _xfailing)
        with pytest.raises(corpus.RendererMismatch):
            corpus.render_golden_case("probe:xfail", "process-xml-v1")
    finally:
        corpus.CASE_REGISTRY.clear()
        corpus.CASE_REGISTRY.update(original)


@pytest.mark.parametrize("shape", ["string", "none", "generator"])
def test_a_malformed_provider_iterable_stays_coded(shape):
    """[P2] `list(...)` outside the guard let a mid-iteration raise escape."""
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.run_plan_fingerprint_checks(True, _StubProvider(bad_iterable=shape))
    assert excinfo.value.code == "PLAN_FINGERPRINT_MISMATCH"


def test_the_digest_must_be_the_digest_of_the_material():
    """[P2] Otherwise the two tuple members can drift apart entirely."""
    class _Drifting(_StubProvider):
        def fingerprint(self, case, *, account, environment, mutation=None):
            return "sha256:" + "0" * 64, b"unrelated bytes"

    with pytest.raises(gate.GateFailure) as excinfo:
        gate.run_plan_fingerprint_checks(True, _Drifting())
    assert excinfo.value.code == "PLAN_FINGERPRINT_MISMATCH"
    assert "sha256" in excinfo.value.message


def test_mutation_kinds_must_produce_distinct_plans():
    """[P2] Declaring four names while ignoring which was asked for proves
    nothing about envelope, policy or revision discrimination."""
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.run_plan_fingerprint_checks(True, _StubProvider(indistinct_kinds=True))
    assert excinfo.value.code == "PLAN_FINGERPRINT_MISMATCH"
    assert "distinguishing" in excinfo.value.message


def test_the_committed_manifests_parse_and_agree_with_the_tree():
    """The real manifests, through the real parser, at the real HEAD."""
    nodes = gate.parse_manifest(
        (_ROOT / gate.NODES_MANIFEST).read_bytes(), "pytest-nodes"
    )
    goldens = gate.parse_manifest(
        (_ROOT / gate.GOLDENS_MANIFEST).read_bytes(), "goldens"
    )
    assert nodes.header["bootstrap_base"] == goldens.header["bootstrap_base"]
    assert len(goldens.active) >= goldens.header["minimum_active"]
    gate.check_golden_tree(str(_ROOT), goldens)
