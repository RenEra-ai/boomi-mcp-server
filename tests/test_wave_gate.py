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
import errno
import os
import pathlib
import shutil
import subprocess
import tempfile
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
    """Scrub `GITHUB_SHA` and `GITHUB_ACTIONS` for every test in this module.

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

    `GITHUB_ACTIONS` joins it for the same reason and by the same rule: the
    gate's `ci` binding treats `GITHUB_ACTIONS=true` as "a platform sha is
    MANDATORY", so leaving it ambient while scrubbing the sha would make every
    `ci_mode` test fail on the runner only. Scrubbing one and not the other is
    the enumeration this fixture exists to replace.
    """
    monkeypatch.delenv("GITHUB_SHA", raising=False)
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)


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


def test_a_local_bootstrap_is_derived_and_the_flag_only_confirms_intent(tmp_path):
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
    # The note must describe the gate ACCURATELY: eligibility is derived, and the
    # flag only confirms intent. An earlier wording claimed the flag "skips
    # manifest transition validation entirely", which misdescribed the gate's own
    # behaviour — the derivation (manifests absent at the baseline, never touched
    # in its ancestry) runs regardless of the flag. What a local run cannot check
    # is the operator's BASELINE CHOICE.
    assert "DERIVED" in stderr
    assert "confirms you meant it" in stderr
    assert "no transition to validate" in stderr

    # Sharing it changes nothing locally — that is the point being pinned, so a
    # ninth landing heuristic is not reintroduced by accident.
    _run_git(repo, "branch", "dev")
    status, stderr = _manifests(repo, base, "--bootstrap")
    assert status == 0, stderr
    assert "DERIVED" in stderr

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


def test_appending_an_already_tombstoned_row_is_refused():
    """A tombstone is a RETIREMENT RECORD; there is nothing to retire.

    Plan §"Append a tombstoned row → fail". An identity that was never in the
    manifest cannot be introduced already retired — the row would permanently
    reserve an id for something that never collected, with floors unchanged.

    A push that adds a test in one commit and removes it in a later one needs no
    row at all: from the range's endpoints that test simply never existed. An
    earlier revision admitted such rows to make that push legal, which solved a
    problem that does not exist.
    """
    _transition_fails(
        [_node_row(1)], [_node_row(1), _node_row(2, state="tombstone")],
        base_header=_node_header(1, 1), head_header=_node_header(1, 1),
        code="MANIFEST_TRANSITION_ILLEGAL",
    )
    # Claiming it as an active append is refused too — the floor check runs
    # first and already rejects a floor that no active row supports.
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
            # A genuinely non-relocatable fingerprint carries the identity in its
            # CANONICAL BYTES; the digest still derives from them.
            material = "{0}:{1}".format(case, account).encode()
            digest = self._digest(material)
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


def test_ci_requires_exactly_one_baseline_selector(tmp_path, monkeypatch, capsys):
    """``ci`` takes EXACTLY ONE of ``--base`` / ``--github-event`` (#171).

    The two accepted forms are the workflow's two arms — the ``dev`` event arm
    and the ``scratch/**`` preflight arm. The two EXCLUDED states are the
    non-vacuity witnesses that "exactly one" is enforced rather than merely
    described: a rule nobody can violate proves nothing.

    Argparse's own sentence is deliberately not asserted — only the exit status,
    the stable first-token code, both option names, and the meaningful fragment.
    """
    repo, base = _seeded(tmp_path)

    # 1. `--base` parses AND threads through the execution seam far enough to
    #    resolve and emit `(local)`. A parser-only assertion would not notice a
    #    handler-threading regression, so stop at the manifest phase rather than
    #    running a whole synthetic suite.
    parsed = gate.build_parser().parse_args(["--repo", str(repo), "ci", "--base", base])
    assert parsed.base == base and parsed.github_event is None

    seen = {}

    def _sentinel(repo_arg, baseline, *, is_local, bootstrap_flag, target=None):
        seen.update(baseline=baseline, is_local=is_local, target=target)
        raise gate._contract("MANIFEST_MISSING", "sentinel: stop after the baseline seam")

    monkeypatch.setattr(gate, "run_manifest_phase", _sentinel)
    assert gate.main(["--repo", str(repo), "ci", "--base", base]) == 2
    err = capsys.readouterr().err
    assert "wave_gate: baseline {0} (local)".format(base) in err, err
    assert seen == {"baseline": base, "is_local": True, "target": None}
    monkeypatch.undo()

    # 2. `--github-event` parses with `base is None`.
    event = tmp_path / "push.json"
    event.write_text(json.dumps({"before": base, "after": _head(repo)}),
                     encoding="utf-8")
    parsed = gate.build_parser().parse_args(
        ["--repo", str(repo), "ci", "--github-event", str(event)]
    )
    assert parsed.github_event == str(event) and parsed.base is None

    # 3. BOTH — the mutual-exclusion witness.
    status, err = _gate(repo, "ci", "--base", base, "--github-event", str(event))
    assert status == 2, err
    assert err.split()[0] == "GATE_USAGE_INVALID", err
    assert "--base" in err and "--github-event" in err, err
    assert "not allowed" in err, err

    # 4. NEITHER — the required-group witness. Bare `ci` must not fall through to
    #    the resolver: the argparse layer is the documented refusal here.
    status, err = _gate(repo, "ci")
    assert status == 2, err
    assert err.split()[0] == "GATE_USAGE_INVALID", err
    assert "--base" in err and "--github-event" in err, err
    assert "required" in err, err


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
    # Codes the gate emits without raising (the last-resort diagnostic fallback)
    # are part of the same stderr contract and must be documented too.
    raised |= set(gate.EMIT_ONLY_CODES)

    # BIDIRECTIONAL PIN. `DIAGNOSTIC_CODES` is what the fallback reporter trusts,
    # so it must equal what the gate can actually raise — a shape-only check let
    # a provider-supplied `UNDECLARED_CODE` reach stderr in the position the
    # machine contract reserves for a documented code.
    assert gate.DIAGNOSTIC_CODES == raised, sorted(
        gate.DIAGNOSTIC_CODES.symmetric_difference(raised)
    )

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


#: Uppercase tokens the audit ledger legitimately uses that are NOT diagnostic codes —
#: env vars, open(2) flags, a checkpoint outcome, prose placeholders, and words that
#: appear in quoted gate output (`BOOTSTRAP` is a log line, not a failure code). Kept
#: explicit and asserted disjoint from `DIAGNOSTIC_CODES` below, so this list can never
#: be used to silence a code that really exists.
_LEDGER_NON_DIAGNOSTIC_TOKENS = frozenset({
    "BLIND",
    "BOOTSTRAP",
    "CONTINUE",
    "DIAGNOSTIC_CODES",
    "EMPTY",
    # FIXED document filenames the ledgers cite, tokenized inside compound inline
    # spans. These are singular documents, so listing them does not grow with the
    # work. `ISSUE_*_AUDIT_LEDGER` stems are deliberately NOT here: those DO grow
    # one-per-slice, so they are derived from the files on disk in the scanner
    # itself — a hand-list of them goes stale the moment a ledger is added, which
    # is exactly what happened when #171's arrived.
    "AUDIT_LEDGER_TEMPLATE",
    "ENDGAME_VERIFICATION_GATE",
    # Actions default variables. The #171 ledger names them when recording the
    # checkout binding and the workflow's two routes — environment variables the
    # gate CONSUMES, never diagnostics it can emit.
    "GITHUB_ACTIONS",
    "GITHUB_EVENT_NAME",
    "GITHUB_EVENT_PATH",
    "GITHUB_REF",
    "GITHUB_SHA",
    "O_EXCL",
    "O_NOFOLLOW",
    "PYTHONHASHSEED",
    # Collector trailer keywords, not diagnostics: the ledger quotes `STATUS:` and
    # `SCOPE:` when recording a review round's attestation.
    "SCOPE",
    "STATUS",
    "UPPER_SNAKE",
})


def test_audit_ledger_revisions_are_append_only_and_fully_declared():
    """A ledger revision must ADD a row, and the supersession map must know about it.

    The append-only rule was violated three times in #171, each time while fixing the
    previous violation, because it was enforced by remembering to sweep — and each sweep
    covered the rows a finding NAMED rather than the rows the batch touched. Sweeping by
    memory demonstrably does not work, so the checkable half is mechanised here.

    What this DOES check, from the ledger text alone:

    * a revision row (`<id>a`) has its original `<id>` present, so a revision can never
      quietly replace what it revises;
    * every revision row present is declared in the ledger's own supersession map, so the
      defect-class tally — which reads that map — cannot silently diverge from the rows;
    * no row id appears twice, so an "edit" cannot masquerade as a second row.

    What it deliberately does NOT check, stated rather than implied: that a pre-existing
    row is BYTE-IDENTICAL to its previously committed form. That needs the prior commit as
    the authority, and a test that reaches into git history would fail on a shallow
    checkout and on the first commit of any new ledger. The byte-identity half stays a
    review obligation; this test removes the failure mode that actually recurred — a
    revision landing without its original, or without being declared.
    """
    import re as _re

    ledgers = sorted((_ROOT / "docs" / "architecture").glob("ISSUE_*_AUDIT_LEDGER.md"))
    assert ledgers, "no ledgers found — this check would be vacuous"

    checked = 0
    for path in ledgers:
        text = path.read_text(encoding="utf-8")
        ids = [
            ln.split("|")[1].strip()
            for ln in text.splitlines()
            if ln.startswith("| ") and ln.count("|") > 8
        ]
        ids = [i for i in ids if i and i not in ("ID", "---") and not i.startswith("**")]

        duplicates = {i for i in ids if ids.count(i) > 1}
        assert duplicates == set(), "{0}: duplicate row ids {1}".format(
            path.name, sorted(duplicates)
        )

        present = set(ids)

        def _supersedes(rid):
            """The row a revision id revises — its immediate PREDECESSOR, not the stem.

            Revisions chain: `X` → `Xa` → `Xb`. `Xb` supersedes `Xa`, not `X`, because a
            revision merges onto the row it revises and `Xa` may itself have changed a
            cell. Deriving the stem instead would declare a mapping that skips a link,
            and the tally reading that map would apply the wrong cells.
            """
            stem, letter = rid[:-1], rid[-1]
            prior = stem + chr(ord(letter) - 1)
            return prior if letter > "a" and prior in present else stem

        revisions = {
            i for i in present
            if _re.fullmatch(r".+[a-z]", i) and _supersedes(i) in present
        }
        # A trailing-letter id whose stem is NOT present is either a typo or an
        # in-place replacement of the original — both are what this test exists to catch.
        orphans = {
            i for i in present
            if _re.fullmatch(r"(?:INH-)?[A-Z]+\d*-\d+[a-z]", i) and i[:-1] not in present
        }
        assert orphans == set(), (
            "{0}: revision rows whose ORIGINAL is missing — a revision must add a row, "
            "never replace one: {1}".format(path.name, sorted(orphans))
        )

        if revisions:
            # Parse the BOUNDED map, not the whole document: a mapping quoted anywhere
            # else — in a finding row's prose, say — would otherwise satisfy this while
            # the map the tally actually reads stays incomplete.
            block = _re.search(
                r"\*\*Supersession map\*\*(.+?)(?:\n\n|\n\*|\Z)", text, _re.S
            )
            assert block, "{0}: revision rows exist but no supersession map".format(path.name)
            declared = set(_re.findall(r"`([^`]+?) → ([^`]+?)`", block.group(1)))
            assert declared == {(r, _supersedes(r)) for r in revisions}, (
                "{0}: the supersession map and the revision rows disagree — declared "
                "{1}, rows imply {2}".format(
                    path.name, sorted(declared),
                    sorted((r, _supersedes(r)) for r in revisions),
                )
            )
            checked += 1

        # The half that had to become mechanical. A pre-existing row must be BYTE
        # IDENTICAL to its last committed form: five separate #171 findings were the
        # same in-place edit, each made while fixing the previous one, because the rule
        # was enforced by remembering to sweep. Git is the authority for "what was
        # committed", so ask it. Where history is unavailable (a shallow checkout, or a
        # ledger in its first commit) this SKIPS rather than fails — an absent authority
        # is not evidence of compliance, and is recorded as such in the ledger.
        rel = str(path.relative_to(_ROOT))
        prior = subprocess.run(
            ["git", "log", "--format=%H", "-n", "2", "--", rel],
            cwd=str(_ROOT), capture_output=True, text=True,
        )
        shas = prior.stdout.split() if prior.returncode == 0 else []
        if len(shas) >= 2:
            was = subprocess.run(
                ["git", "show", "{0}:{1}".format(shas[1], rel)],
                cwd=str(_ROOT), capture_output=True, text=True,
            )
            if was.returncode == 0:
                def _rows(blob):
                    out = {}
                    for ln in blob.splitlines():
                        if ln.startswith("| ") and ln.count("|") > 8:
                            rid = ln.split("|")[1].strip()
                            if rid and rid not in ("ID", "---") and not rid.startswith("**"):
                                out[rid] = ln
                    return out
                before, after = _rows(was.stdout), _rows(text)
                mutated = sorted(
                    rid for rid in before if rid in after and before[rid] != after[rid]
                )
                assert mutated == [], (
                    "{0}: these rows were EDITED IN PLACE rather than superseded by an "
                    "appended revision row: {1}. The ledger is append-only — restore the "
                    "committed text and append `<id>a`/`<id>b` instead.".format(
                        path.name, mutated
                    )
                )

    assert checked, (
        "no ledger exercised the revision path — the assertions above would be vacuous"
    )


def test_diagnostic_codes_named_in_the_audit_ledger_exist():
    """The ledger must not name a diagnostic the gate cannot emit.

    #152's ledger claimed a seeded golden mutation would produce `GOLDEN_MISMATCH`.
    It cannot: `run_suite()` precedes the wave-only `check_goldens()`, so the suite's
    own golden test fails first and the gate reports `PYTEST_FAILED`. The claim read
    as authoritative for three review rounds because nothing checked it.

    This pins the part that CAN be mechanised — that every code named is a real
    member of `DIAGNOSTIC_CODES`. It deliberately does NOT claim to verify that a
    named code is the RIGHT code for the scenario described; only running the
    scenario establishes that, which is why the ledger quotes measured output.
    """
    import re as _re

    ledger = (
        _ROOT / "docs" / "architecture" / "ISSUE_152_AUDIT_LEDGER.md"
    ).read_text(encoding="utf-8")

    # Candidates are extracted CASE-TOLERANTLY and judged afterwards. An earlier
    # revision matched `[A-Z][A-Z0-9_]{4,}`, which cannot see a malformed spelling
    # at all: `PYTEST_NODE_MISSINg` produced no token, so it vanished instead of
    # failing — the precise defect the test exists to catch. A token is a candidate
    # if it carries an underscore and at least four capitals; MEASURED: all 41
    # members of `DIAGNOSTIC_CODES` contain an underscore, so nothing real is lost,
    # while ordinary snake_case (`st_dev`, `minimum_active`) has no capitals and
    # CamelCase (`GateFailure`) has no underscore.
    _CANDIDATE = r"[A-Za-z][A-Za-z0-9_]{4,}"

    # A typo can also DELETE the underscore (`PYTEST_FAILED` -> `PYTESTFAILED`), which
    # the shape rule below would discard before ever checking membership. So a token
    # is also judged when it collides with a real code after normalisation — that
    # collision is precisely what makes it a near-miss rather than an unrelated word.
    _normal = {c.replace("_", "").upper(): c for c in gate.DIAGNOSTIC_CODES}

    def _judge(tokens):
        out = set()
        for tok in tokens:
            # NEAR-MISS FIRST, before any shape filter. A typo can also drop the
            # CASE (`golden_mismatch`), which has no capitals at all — an earlier
            # revision applied the >=4-capitals threshold first and so discarded
            # exactly the malformed spellings this exists to report.
            near_miss = (
                tok.replace("_", "").upper() in _normal
                and tok not in gate.DIAGNOSTIC_CODES
            )
            if near_miss:
                out.add(tok)
                continue
            # Otherwise a token must LOOK like a diagnostic to be judged at all,
            # or ordinary prose would flood this.
            if "_" in tok and sum(c.isupper() for c in tok) >= 4:
                out.add(tok)
        return out - _LEDGER_NON_DIAGNOSTIC_TOKENS

    # The two forms are scanned SEPARATELY, not merged, so each can be asserted on
    # its own. Merging them made the fenced-coverage assertion vacuous: the codes it
    # named also appear in inline spans, so it passed even when the fence scan found
    # nothing at all.
    def inline_codes(text):
        # Fenced blocks are EXCISED first. A ``` fence is three backticks, and an
        # inline-span pattern reads them as ordinary delimiters — which pairs every
        # following backtick out of phase and silently drops whole regions. Measured
        # on this ledger: `GOLDEN_NONDETERMINISTIC` was present in the file and
        # invisible to the scan. Fences are covered by `fenced_codes()` anyway.
        text = _re.sub(r"```.*?```", "", text, flags=_re.S)
        # Then tokenize INSIDE each span rather than requiring the whole span to be
        # one candidate: the ledger writes compound spans like
        # `code: MANIFEST_FLOOR_INVALID, status: 2`. Newline-tolerant and bounded,
        # because a Markdown span may wrap across a source line.
        # Match a RUN of backticks and require the same run to close it. Markdown
        # allows ``a `literal` `` spans, and the ledger uses one; a single-backtick
        # pattern pairs those delimiters out of phase and drops what follows.
        toks = []
        for _delim, span in _re.findall(r"(`+)(.+?)\1", text, _re.S):
            toks += _re.findall(r"\b({0})\b".format(_CANDIDATE), span)
        return _judge(toks)

    def fenced_codes(text):
        # Inside a fence the tokens are bare — this is where the ledger quotes real
        # gate output, and an inline-only scan left it entirely unchecked.
        toks = []
        for block in _re.findall(r"```.*?\n(.*?)```", text, _re.S):
            toks += _re.findall(r"\b({0})\b".format(_CANDIDATE), block)
        return _judge(toks)

    def named_codes(text):
        return inline_codes(text) | fenced_codes(text)

    # The allowlist may never hide a real code.
    assert _LEDGER_NON_DIAGNOSTIC_TOKENS & gate.DIAGNOSTIC_CODES == set(), sorted(
        _LEDGER_NON_DIAGNOSTIC_TOKENS & gate.DIAGNOSTIC_CODES
    )

    # Ledger filenames are allowed by DERIVATION from the files on disk, never by
    # hand-listing. Ledgers cite each other — #152's close note names #171's — so a
    # hand-list needs a new entry for every ledger that ever exists, and #171 is
    # where that enumeration first came due. The authority is the glob, which the
    # all-ledgers loop below already consults; hoisted here so BOTH scans read it
    # rather than one reading a stale copy of it.
    all_ledgers = sorted(
        (_ROOT / "docs" / "architecture").glob("ISSUE_*_AUDIT_LEDGER.md")
    )
    assert any(p.name == "ISSUE_152_AUDIT_LEDGER.md" for p in all_ledgers)
    derived_stems = {p.stem for p in all_ledgers}

    unknown = named_codes(ledger) - gate.DIAGNOSTIC_CODES - derived_stems
    assert unknown == set(), (
        "the audit ledger names diagnostic codes the gate cannot emit: "
        "{0}".format(sorted(unknown))
    )

    # NON-VACUITY, three ways. First: the scan must actually be finding codes in the
    # real file — an allowlist that swallowed everything, or a regex that matched
    # nothing, would pass the assertion above while checking nothing at all.
    found = named_codes(ledger) & gate.DIAGNOSTIC_CODES
    assert len(found) >= 5, sorted(found)
    assert "PYTEST_FAILED" in found, sorted(found)

    # Second: a code that does not exist must be REJECTED. Constructed here rather
    # than written into the ledger, so the ledger holds no fictional code even as
    # an illustration.
    fictional = "PYTEST_NODE_MISSSING"          # note the deliberate typo
    assert fictional not in gate.DIAGNOSTIC_CODES
    assert named_codes("a row naming `{0}`".format(fictional)) - gate.DIAGNOSTIC_CODES \
        == {fictional}

    # Third, and the case an inline-only scan silently missed: the same typo inside
    # a FENCED block, where the ledger quotes real gate output and the tokens carry
    # no backticks. This is the arm that regressed, so it gets its own witness.
    fenced = "```\nseed 3  {0} 1 required node id(s)\n```".format(fictional)
    assert named_codes(fenced) - gate.DIAGNOSTIC_CODES == {fictional}

    # Fourth: a MALFORMED spelling — a real code with a lowercase slip. The earlier
    # uppercase-only pattern could not represent this at all, so the bad token
    # disappeared and the test passed. Both forms must now surface it.
    malformed = "PYTEST_NODE_MISSINg"
    assert malformed not in gate.DIAGNOSTIC_CODES
    assert malformed.upper() in gate.DIAGNOSTIC_CODES, "the witness must be a near-miss"
    assert named_codes("`{0}`".format(malformed)) - gate.DIAGNOSTIC_CODES == {malformed}
    assert named_codes("```\n{0} x\n```".format(malformed)) - gate.DIAGNOSTIC_CODES \
        == {malformed}

    # ...and the fenced scan must reach the ledger's REAL quoted output, not just
    # synthetic text. Asserted against fence-derived tokens ALONE — an earlier
    # revision checked the merged set, so it stayed green even if the fence scan
    # returned nothing, because the codes it named also appear in inline spans.
    real_fenced = fenced_codes(ledger) & gate.DIAGNOSTIC_CODES
    assert len(real_fenced) >= 5, (
        "the fenced scan found {0} diagnostic code(s) in the ledger's quoted output; "
        "the 'Observed diagnostics' block should supply several".format(
            sorted(real_fenced)
        )
    )

    # The inline arm needs the same treatment, and for the same reason: it was
    # silently dropping regions because fence backticks put its span pairing out of
    # phase. Assert it against inline-derived tokens ALONE.
    real_inline = inline_codes(ledger) & gate.DIAGNOSTIC_CODES
    assert len(real_inline) >= 5, sorted(real_inline)

    # STRONGEST of the coverage checks, and the one that would have caught the
    # out-of-phase bug directly: no code written anywhere in the ledger may be
    # invisible to the scan. Substring presence is a coarse oracle, but it is
    # derived from the file rather than from the parser under test — which is the
    # whole point.
    present = {c for c in gate.DIAGNOSTIC_CODES if c in ledger}
    invisible = present - (real_inline | real_fenced)
    assert invisible == set(), (
        "these codes appear in the ledger but the scan cannot see them, so a typo "
        "in them would go unreported: {0}".format(sorted(invisible))
    )

    # ...and PER OCCURRENCE, not per distinct code. The set check above is
    # satisfied by ONE visible occurrence: the ledger repeats codes (PYTEST_FAILED
    # six times at this writing), so a region going invisible while another
    # occurrence stays visible — and a typo inside the hidden region — would both
    # pass it. "Covered" means a region a scan actually PARSES: a fence BODY (the
    # same capture fenced_codes() reads — the opener/info line is NOT parsed, so
    # it is not covered), or an inline span located on the fence-blanked text
    # (same-length whitespace, so source positions survive) exactly as
    # inline_codes() excises.
    def _covered_spans(text):
        fences = [m.span() for m in _re.finditer(r"```.*?```", text, _re.S)]
        spans = [m.span(1) for m in _re.finditer(r"```.*?\n(.*?)```", text, _re.S)]
        blanked = _re.sub(
            r"```.*?```", lambda m: " " * len(m.group(0)), text, flags=_re.S
        )
        # An inline span may OPEN before an excised fence and CLOSE after it. The
        # scan feeds _judge() only the surviving characters, so the blanked fence
        # inside the span — including its unparsed opener line — is NOT covered:
        # keep the gaps, or an opener token would be masked as if parsed.
        for a, b in (m.span() for m in _re.finditer(r"(`+)(.+?)\1", blanked, _re.S)):
            cur = a
            for fa, fb in fences:
                if fb <= cur or fa >= b:
                    continue
                if fa > cur:
                    spans.append((cur, fa))
                cur = max(cur, fb)
            if cur < b:
                spans.append((cur, b))
        return spans

    def _uncovered_text(text):
        chars = list(text)
        for a, b in _covered_spans(text):
            chars[a:b] = " " * (b - a)
        return "".join(chars)

    def _hidden_occurrences(text, codes):
        spans = _covered_spans(text)
        return [
            (code, m.start())
            for code in sorted(codes)
            for m in _re.finditer(r"\b" + _re.escape(code) + r"\b", text)
            if not any(a <= m.start() and m.end() <= b for a, b in spans)
        ]

    assert _hidden_occurrences(ledger, present) == [], (
        "these (code, offset) occurrences are outside every parsed region, so a "
        "typo there would go unreported"
    )

    # A typo in a hidden region is not in `present`, so the exact-code walk above
    # cannot search for it. Judge every candidate token in the UNCOVERED text
    # instead: no diagnostic-like token — exact, near-miss, or merely shaped like
    # a code — may sit outside the parsed regions at all.
    # Ledger stems are subtracted here for the same reason as above, and by the
    # same derived authority: a markdown LINK TARGET (`](ISSUE_171_AUDIT_LEDGER.md)`)
    # is outside every backtick span by construction, so a ledger that links to a
    # sibling ledger would otherwise fail a check about diagnostic codes.
    bare = _judge(
        _re.findall(r"\b({0})\b".format(_CANDIDATE), _uncovered_text(ledger))
    ) - derived_stems
    assert bare == set(), (
        "diagnostic-like tokens sit outside every parsed region, where the scans "
        "cannot judge them: {0}".format(sorted(bare))
    )

    # Non-vacuity witnesses, one per false-negative path this closed:
    # 1. a DUPLICATED code, one occurrence in a span and one bare — the
    #    distinct-code sets see the first and stay green; the occurrence walk
    #    must report exactly the second;
    witness = "`PYTEST_FAILED` in a span, then bare PYTEST_FAILED in prose."
    assert named_codes(witness) & gate.DIAGNOSTIC_CODES == {"PYTEST_FAILED"}
    assert _hidden_occurrences(witness, {"PYTEST_FAILED"}) == [
        ("PYTEST_FAILED", witness.rindex("PYTEST_FAILED"))
    ]
    # 2. a MALFORMED bare token: not a member of `present`, invisible to
    #    named_codes(), so only the uncovered-candidate judge can report it;
    assert "PYTEST_FAILD" not in gate.DIAGNOSTIC_CODES
    assert named_codes("bare PYTEST_FAILD in prose") == set()
    assert _judge(
        _re.findall(
            r"\b({0})\b".format(_CANDIDATE),
            _uncovered_text("bare PYTEST_FAILD in prose"),
        )
    ) == {"PYTEST_FAILD"}
    # 3. a code on a fence OPENER line, which fenced_codes() never parses: the
    #    fence-body span must not cover it.
    opener = "```PYTEST_FAILED\nbody text\n```"
    assert fenced_codes(opener) == set()
    assert _hidden_occurrences(opener, {"PYTEST_FAILED"}) == [("PYTEST_FAILED", 3)]
    # 4. an inline span that OPENS before a fence and CLOSES after it: no scan
    #    parses the opener token inside it, so the straddling span must not mask
    #    the malformed opener from the uncovered-candidate judge.
    straddle = "`a ```PYTEST_FAILD\nx\n``` b`"
    assert named_codes(straddle) == set()
    assert _judge(
        _re.findall(r"\b({0})\b".format(_CANDIDATE), _uncovered_text(straddle))
    ) == {"PYTEST_FAILD"}

    # EVERY instantiated audit ledger gets the core scan, not just #152's — the
    # ledger template promises this enforcement to future slices, and a promise
    # scoped to one filename is a hand-enumeration of the very kind the ledger
    # discipline forbids. The floors and witnesses above stay 152-specific (a
    # young ledger may legitimately name few codes); the CLOSED checks — no
    # unknown code, no hidden occurrence, no bare diagnostic-like token — bind
    # everywhere. Each file's own stem is allowed as a token by derivation (the
    # ledgers cite their own filenames AND each other's), never by hand-listing.
    # `all_ledgers`/`derived_stems` are established once, above, and used by both
    # scans — deriving the same set twice is the duplication this rule forbids.
    for path in all_ledgers:
        text = path.read_text(encoding="utf-8")
        unknown_here = named_codes(text) - gate.DIAGNOSTIC_CODES - derived_stems
        assert unknown_here == set(), (
            "{0} names diagnostic codes the gate cannot emit: {1}".format(
                path.name, sorted(unknown_here)
            )
        )
        present_here = {c for c in gate.DIAGNOSTIC_CODES if c in text}
        assert _hidden_occurrences(text, present_here) == [], path.name
        bare_here = _judge(
            _re.findall(r"\b({0})\b".format(_CANDIDATE), _uncovered_text(text))
        ) - derived_stems
        assert bare_here == set(), (
            "{0}: diagnostic-like tokens outside every parsed region: {1}".format(
                path.name, sorted(bare_here)
            )
        )


def _split_out_gate_step(workflow):
    """Split a workflow into (the named Wave-gate step, everything else).

    The step ends at the first NONBLANK DEDENT — any line indented at or below the
    step's own `- name:` line — not merely at the next sibling `- ` item. A later
    job whose `steps:` sequence is indented more deeply than this one dedents at
    its own job key, a line that never starts with `- `, so a `- `-only terminator
    runs straight through it and swallows that job into the slice. That is not
    hypothetical: `test_the_step_slice_ends_at_a_dedented_second_job` constructs
    the exact workflow it swallowed.
    """
    lines = workflow.splitlines()
    heads = [i for i, ln in enumerate(lines) if ln.strip().startswith("- name: Wave gate")]
    assert len(heads) == 1, heads
    top = heads[0]
    indent = len(lines[top]) - len(lines[top].lstrip())
    tail = len(lines)
    for j in range(top + 1, len(lines)):
        ln = lines[j]
        if ln.strip() and (len(ln) - len(ln.lstrip())) <= indent:
            tail = j
            break
    return "\n".join(lines[top:tail]), "\n".join(lines[:top] + lines[tail:])


def test_the_step_slice_ends_at_a_dedented_second_job():
    """The slice boundary's NON-VACUITY witness, committed rather than performed.

    The real workflow has ONE job whose Wave-gate step is last, so the old
    `- `-only terminator and the correct dedent terminator both run to end-of-file
    and agree — reverting the fix leaves the real-file assertions green. The
    property therefore needs a case where they DISAGREE, and this is it: a second
    job whose `steps:` sequence is indented more deeply than the first job's.

    Under the old predicate the second job's gate invocation landed INSIDE `step`
    and `outside` came back empty, so a workflow whose two routes no longer share
    one step passed. Here it must land outside.
    """
    smuggled = (
        "jobs:\n"
        "  non-kb-python311:\n"
        "    steps:\n"
        "      - uses: actions/checkout@v7\n"
        "      - name: Wave gate (manifests, collection floor, required nodes, full suite)\n"
        "        run: |\n"
        "          case \"${GITHUB_EVENT_NAME-}:${GITHUB_REF-}\" in\n"
        "            push:refs/heads/dev)\n"
        "              exec python scripts/wave_gate.py ci --github-event \"$GITHUB_EVENT_PATH\"\n"
        "              ;;\n"
        "          esac\n"
        "  smuggled-second-job:\n"
        "        runs-on: ubuntu-24.04\n"
        "        steps:\n"
        "          - name: Scratch arm hidden in a deeper job\n"
        "            run: |\n"
        "              exec python scripts/wave_gate.py ci --base \"$base\"\n"
    )
    step, outside = _split_out_gate_step(smuggled)

    # The smuggled invocation is NOT part of the named step...
    assert "--base" not in step, step
    assert step.count("scripts/wave_gate.py") == 1, step
    # ...and it IS visible outside it, which is what the real test's
    # `"wave_gate.py" not in outside` assertion then rejects.
    assert "scripts/wave_gate.py ci --base" in outside, outside

    # And the ordinary single-job shape still slices to end-of-file.
    plain = (
        "jobs:\n"
        "  only:\n"
        "    steps:\n"
        "      - name: Wave gate (manifests, collection floor, required nodes, full suite)\n"
        "        run: |\n"
        "          exec python scripts/wave_gate.py ci --github-event \"$GITHUB_EVENT_PATH\"\n"
    )
    step, outside = _split_out_gate_step(plain)
    assert "scripts/wave_gate.py" in step
    assert "wave_gate.py" not in outside


def test_the_workflow_invokes_the_real_gate_and_isolates_push_runs():
    """The workflow is part of the contract, so pin the parts that fail open.

    * It must call `wave_gate.py ci` — not `pytest` directly, which can go green
      on a partial collection.
    * Both routes must exist and must select the baseline the way #171 decided:
      the event payload on `dev`, the exact fetched `origin/dev` commit on
      `scratch/**` — never `github.event.before` (the zero sha on branch
      creation, and only the latest increment afterwards) and never a merge-base
      (a diverged branch would preflight green).
    * ONE step, no step-level `if:`, and an unrecognized context REFUSES: two
      conditional steps could both skip and leave a green job that checked
      nothing.
    * Nothing may soften a failure.
    * No `pull_request` trigger (#171 criterion 7a) and no `concurrency` block —
      a per-SHA group would make the same candidate SHA collide between its
      scratch preflight and its `dev` push, which is exactly the sequence the
      preflight route creates.
    """
    raw = (_ROOT / ".github" / "workflows" / "tests.yml").read_text(encoding="utf-8")
    # Comments legitimately NAME the removed triggers and the softeners in order
    # to say they are absent, so scan the directives only.
    workflow = "\n".join(
        line for line in raw.splitlines() if not line.lstrip().startswith("#")
    )
    assert "branches: [dev, 'scratch/**']" in workflow
    for absent in ("pull_request", "workflow_dispatch", "concurrency",
                   "cancel-in-progress", "github.event.before", "merge-base"):
        assert absent not in workflow, absent

    # ONE step carrying BOTH arms. Asserting the strings against the whole file
    # would be VACUOUS: the scratch arm could sit in a second step while the named
    # one rejected scratch, and every count below would still hold. So isolate the
    # named step and assert INSIDE it — then assert the gate is invoked nowhere
    # else. (Sliced textually rather than parsed. PyYAML IS importable here — the
    # pinned `fastmcp` declares it unconditionally — so this is a preference, not
    # a necessity: the slice asserts the block boundary directly instead of
    # reconstructing it from parsed structure. An earlier revision of this comment
    # claimed PyYAML was absent and that importing it would break CI collection;
    # that was false and is withdrawn.)
    step, outside = _split_out_gate_step(workflow)

    # Both routes, both refusals, inside the ONE step.
    assert step.count("scripts/wave_gate.py") == 2
    assert 'ci \\\n                --github-event "$GITHUB_EVENT_PATH"' in step
    assert 'ci --base "$base"' in step
    assert "push:refs/heads/dev)" in step
    assert "push:refs/heads/scratch/*)" in step
    assert "refs/remotes/origin/dev^{commit}" in step
    # The two fail-closed arms: an unresolvable baseline, and any other context.
    assert "BASELINE_UNAVAILABLE cannot resolve refs/remotes/origin/dev" in step
    assert "BASELINE_EVENT_INVALID unsupported event/ref" in step
    assert step.count("exit 2") == 2
    # ...and NOTHING outside it invokes the gate, so the step really is the whole
    # story rather than merely one telling of it.
    assert "wave_gate.py" not in outside, outside

    assert "python-version: \"3.11\"" in workflow
    assert "requirements-dev.txt" in workflow
    # Full ancestry: the preflight resolves origin/dev and proves descent from it.
    assert "fetch-depth: 0" in workflow
    for softener in ("continue-on-error", "|| true", "if: always()", "if: "):
        assert softener not in workflow, softener
    assert "name: Python 3.11 non-KB" in workflow
    assert "GITHUB_SHA: ${{ github.sha }}" in workflow


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

    # ---- local CI arm (#171). The same binding, derived from the platform and
    # from git's ancestry graph rather than from an event payload.
    ctx_local = {"kind": "local", "sha": base, "target": None,
                 "event_head": None, "after": None}
    # Clean checkout, baseline in its ancestry, no Actions environment: accepted.
    gate.check_checkout_matches_event(str(repo), ctx_local, ci_mode=True)

    # Inside Actions the platform sha is MANDATORY and must BE the checkout.
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("GITHUB_SHA", other)          # `other` is HEAD here
    gate.check_checkout_matches_event(str(repo), ctx_local, ci_mode=True)
    for bad_sha in ("9" * 40, "not-a-sha", ""):
        monkeypatch.setenv("GITHUB_SHA", bad_sha)
        with pytest.raises(gate.GateFailure) as excinfo:
            gate.check_checkout_matches_event(str(repo), ctx_local, ci_mode=True)
        assert excinfo.value.code == "CHECKOUT_EVENT_MISMATCH", bad_sha
    monkeypatch.delenv("GITHUB_SHA")                 # absent INSIDE Actions
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.check_checkout_matches_event(str(repo), ctx_local, ci_mode=True)
    assert excinfo.value.code == "CHECKOUT_EVENT_MISMATCH"

    # Outside Actions the variable is optional — `ci --base` stays usable
    # locally — but a value that IS supplied is still validated.
    monkeypatch.delenv("GITHUB_ACTIONS")
    gate.check_checkout_matches_event(str(repo), ctx_local, ci_mode=True)
    monkeypatch.setenv("GITHUB_SHA", "9" * 40)
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.check_checkout_matches_event(str(repo), ctx_local, ci_mode=True)
    assert excinfo.value.code == "CHECKOUT_EVENT_MISMATCH"
    monkeypatch.delenv("GITHUB_SHA")

    # ---- the platform binding applies to EVERY `ci` context, not just `local`.
    # These witnesses are built so the ARM ITSELF is already satisfied — the push's
    # `after` IS the checkout, the PR's `event_head` IS the checkout — so the only
    # thing that can refuse is the binding. Asserting a wrong-sha refusal without
    # that care proves nothing: the push and PR arms raise the same code on their
    # own, so such a test passes whether or not the binding runs at all.
    # (MEASURED: with these four cases removed, moving `_bind_head_to_platform_sha`
    # back under the `local` arm leaves the suite green.)
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    for kind, ctx_ok in (
        ("push", {"kind": "push", "target": base, "after": other}),
        ("pull_request",
         {"kind": "pull_request", "target": base, "event_head": other}),
    ):
        # The arm agrees with the checkout, and so does the platform: accepted.
        monkeypatch.setenv("GITHUB_SHA", other)
        gate.check_checkout_matches_event(str(repo), ctx_ok, ci_mode=True)
        # The arm still agrees — only the PLATFORM disagrees. Without the binding
        # every one of these returns cleanly.
        for bad_sha in ("9" * 40, "not-a-sha"):
            monkeypatch.setenv("GITHUB_SHA", bad_sha)
            with pytest.raises(gate.GateFailure) as excinfo:
                gate.check_checkout_matches_event(str(repo), ctx_ok, ci_mode=True)
            assert excinfo.value.code == "CHECKOUT_EVENT_MISMATCH", (kind, bad_sha)
        monkeypatch.delenv("GITHUB_SHA")
        with pytest.raises(gate.GateFailure) as excinfo:
            gate.check_checkout_matches_event(str(repo), ctx_ok, ci_mode=True)
        assert excinfo.value.code == "CHECKOUT_EVENT_MISMATCH", kind
        # ...and the same context under `ci_mode=False` is still accepted, which is
        # what makes these four cases a witness for the WIDENING specifically.
        gate.check_checkout_matches_event(str(repo), ctx_ok, ci_mode=False)
    monkeypatch.delenv("GITHUB_ACTIONS")

    # A baseline off this history is not an integration delta.
    _run_git(repo, "checkout", "-q", "-b", "side")
    off_history = _commit(repo, "a commit that is not in the candidate's ancestry")
    _run_git(repo, "checkout", "-q", "main")
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.check_checkout_matches_event(
            str(repo), dict(ctx_local, sha=off_history), ci_mode=True
        )
    assert excinfo.value.code == "BASELINE_UNAVAILABLE"
    assert "not an ancestor" in excinfo.value.message

    # ...and the same non-ancestor baseline is accepted when this is NOT `ci`:
    # `wave`/`manifests` keep their documented explicit-baseline latitude.
    gate.check_checkout_matches_event(
        str(repo), dict(ctx_local, sha=off_history), ci_mode=False
    )


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
    # The WHOLE context, not just the sha: `execute()` now derives `is_local`
    # from `kind`, and the three event-only fields must be absent rather than
    # carrying a stale value from some other arm.
    assert gate.resolve_baseline(str(repo), base=base) == {
        "sha": base, "kind": "local", "target": None,
        "event_head": None, "after": None,
    }
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
    refused = []

    def _refusing_open(path, *args, **kwargs):
        if os.fsdecode(path).endswith("locked.bin"):
            # Non-vacuity witness: a CONDITIONAL patch stops firing silently when
            # the code under test changes how it opens files, and the assertion
            # below can still pass for an unrelated reason.
            refused.append(path)
            raise PermissionError(13, "Permission denied")
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr("builtins.open", _refusing_open)
    with pytest.raises(gate.GateFailure) as excinfo:
        gate._status(str(repo))
    assert excinfo.value.code == "WORKTREE_DIRTY"
    assert "cannot fingerprint" in excinfo.value.message
    assert refused, "the forced read failure never fired — the test would be vacuous"


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


def test_a_GateFailure_can_never_carry_a_success_status():
    """[P1] Guarding each raise site individually leaked.

    Provider-controlled code could construct `GateFailure(..., 0)` — via a
    `__len__` on a tuple subclass, say — and it reached `main()`, which returned
    0. Reproduced end to end with `main_status=0`. The invariant now lives in the
    constructor, so every present and future path is closed at once.
    """
    assert gate.GateFailure("X", "m", 0).status == 1
    assert gate.GateFailure("X", "m", -1).status == 1
    assert gate.GateFailure("X", "m", 99).status == 1
    assert gate.GateFailure("X", "m", 1).status == 1
    assert gate.GateFailure("X", "m", 2).status == 2

    class _Sneaky(_StubProvider):
        def cases(self):
            raise gate.GateFailure("PLAN_FINGERPRINT_MISMATCH", "green please", 0)

    with pytest.raises(gate.GateFailure) as excinfo:
        gate.run_plan_fingerprint_checks(True, _Sneaky())
    assert excinfo.value.status == 1


def test_the_phase_boundary_preserves_real_gate_diagnostics():
    """[P2] Removing the `GateFailure` re-raise replaced legitimate diagnostics.

    `wave --require-plan-fingerprint` with no provider must still say
    `PLAN_FINGERPRINT_PENDING`, and a real mismatch must keep its detail —
    otherwise the CLI's stable diagnostic contract is broken by the very handler
    added to harden it. Safe to re-raise now because `GateFailure` guarantees its
    own status is 1 or 2.
    """
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.run_fingerprint_phase(True)
    assert excinfo.value.code == "PLAN_FINGERPRINT_PENDING"

    with pytest.raises(gate.GateFailure) as excinfo:
        gate.run_fingerprint_phase(True, _StubProvider(relocatable=False))
    assert excinfo.value.code == "PLAN_FINGERPRINT_MISMATCH"
    assert "not relocatable" in excinfo.value.message

    assert gate.run_fingerprint_phase(False) == "pending:#153"


def test_the_phase_boundary_never_inspects_a_hostile_exception():
    """[P1] `type(exc).__name__` can run provider code via a metaclass hook, and
    a hook raising `SystemExit(0)` would escape the last-resort handler."""
    class _Meta(type):
        @property
        def __name__(cls):
            raise SystemExit(0)

    class _Hostile(Exception, metaclass=_Meta):
        pass

    class _Provider(_StubProvider):
        def cases(self):
            raise _Hostile()

    with pytest.raises(gate.GateFailure) as excinfo:
        gate.run_fingerprint_phase(True, _Provider())
    assert excinfo.value.status == 1
    assert excinfo.value.code == "PLAN_FINGERPRINT_MISMATCH"


def test_scratch_inside_the_repository_is_refused(tmp_path, monkeypatch):
    """The structural invariant is ENFORCED, not asserted.

    `tempfile.mkdtemp()` honours `TMPDIR`, so with it pointing at the worktree
    the gate's scratch is created INSIDE the repository and removed again before
    the closing fingerprint — the before/after comparison stays identical while
    the gate really did write into the tree. Reproduced: `inside_repo=True`,
    `fingerprint_equal_after_cleanup=True`. Since the best-effort fingerprint
    leans on this invariant, it has to hold.
    """
    repo, _base = _seeded(tmp_path)
    # `tempfile.gettempdir()` MEMOIZES its answer, so setting the variable in this
    # warm process is inert on its own. A real gate run is a cold process that
    # resolves TMPDIR on first use, so clearing the cache is what reproduces the
    # production path rather than a lucky no-op.
    monkeypatch.setattr(tempfile, "tempdir", None)
    monkeypatch.setenv("TMPDIR", str(repo))
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.make_scratch_dir(str(repo))
    assert excinfo.value.code == "SCRATCH_INSIDE_REPO"
    assert excinfo.value.status == 2
    # ...and nothing was left behind inside the tree.
    assert not list(repo.glob("wave-gate-*"))

    # A scratch root outside the repository is accepted and really is outside.
    outside = tmp_path / "elsewhere"
    outside.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", None)
    monkeypatch.setenv("TMPDIR", str(outside))
    created = gate.make_scratch_dir(str(repo))
    try:
        assert not os.path.realpath(created).startswith(
            os.path.realpath(str(repo)) + os.sep
        )
    finally:
        os.rmdir(created)


def _case_insensitive(path):
    """Probe the filesystem rather than guessing from `sys.platform`."""
    probe = path / "CaseProbe"
    probe.mkdir()
    try:
        twin = path / "caseprobe"
        return twin.exists() and os.path.samefile(str(probe), str(twin))
    except OSError:
        return False
    finally:
        probe.rmdir()


def test_scratch_containment_is_decided_by_inode_not_by_spelling(tmp_path, monkeypatch):
    """A differently-SPELLED path to the same directory must still be refused.

    `os.path.realpath()` preserves the spelling it is handed, so on a
    case-insensitive filesystem `TMPDIR=/users/.../repo` against a repo reported
    as `/Users/.../repo` defeats any lexical prefix comparison while landing the
    scratch physically inside the worktree — cleanup then hides it from the
    closing fingerprint. Measured before the fix: `lexical check would refuse:
    False`, `PHYSICALLY inside the repo: True`.

    The probe is a runtime measurement, not a `sys.platform` guess: this hazard
    is a property of the filesystem, and a case-sensitive one cannot express it.
    """
    repo, _base = _seeded(tmp_path)
    if not _case_insensitive(tmp_path):
        pytest.skip(
            "case-sensitive filesystem: a same-inode/different-spelling TMPDIR "
            "cannot be constructed here without root (bind mount)"
        )
    spelled = str(repo).replace("/Users/", "/users/", 1)
    if spelled == str(repo):
        head, sep, tail = str(repo).rpartition(os.sep)
        spelled = head + sep + (tail.upper() if tail.lower() == tail else tail.lower())
    assert spelled != str(repo)
    assert os.path.samefile(spelled, str(repo)), "the probe must name the same directory"

    monkeypatch.setattr(tempfile, "tempdir", None)
    monkeypatch.setenv("TMPDIR", spelled)
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.make_scratch_dir(str(repo))
    assert excinfo.value.code == "SCRATCH_INSIDE_REPO"
    assert not list(repo.glob("wave-gate-*"))


def test_scratch_containment_that_cannot_be_proven_fails_closed(tmp_path, monkeypatch):
    """An unstattable ancestor is not evidence that the scratch is elsewhere."""
    repo, _base = _seeded(tmp_path)
    outside = tmp_path / "elsewhere"
    outside.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", None)
    monkeypatch.setenv("TMPDIR", str(outside))

    real_stat = os.stat
    blinded = []

    def blind(path, *args, **kwargs):
        if str(path) == str(repo):
            blinded.append(path)     # non-vacuity witness for a CONDITIONAL patch
            raise OSError(13, "Permission denied")
        return real_stat(path, *args, **kwargs)

    monkeypatch.setattr(os, "stat", blind)
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.make_scratch_dir(str(repo))
    assert excinfo.value.code == "SCRATCH_CONTAINMENT_UNPROVEN"
    assert excinfo.value.status == 2
    assert blinded, "the forced stat failure never fired — the test would be vacuous"


def test_the_scratch_path_returned_is_the_path_that_was_verified(tmp_path, monkeypatch):
    """Check one path and write through another and only one of them was proven.

    With `TMPDIR` a symlink, `mkdtemp()` returns a name whose parent can later be
    repointed. Returning the RESOLVED path means the directory the gate writes
    into is the same object the containment check cleared — so repointing the
    symlink afterwards cannot redirect the gate's writes into the worktree.
    """
    repo, _base = _seeded(tmp_path)
    outside = tmp_path / "real_outside"
    outside.mkdir()
    link = tmp_path / "link"
    link.symlink_to(outside, target_is_directory=True)

    monkeypatch.setattr(tempfile, "tempdir", None)
    monkeypatch.setenv("TMPDIR", str(link))
    scratch = gate.make_scratch_dir(str(repo))
    resolved = os.fspath(scratch)
    try:
        # The returned path carries no symlink component to retarget.
        assert resolved == os.path.realpath(resolved)
        assert not resolved.startswith(str(link) + os.sep)

        # Repoint the symlink at the repository, exactly as the finding describes,
        # and recreate the basename there.
        basename = os.path.basename(resolved)
        link.unlink()
        link.symlink_to(repo, target_is_directory=True)
        (repo / basename).mkdir()

        # A write through the RETURNED path still lands outside the repository.
        with scratch.open_for_write("collected.txt") as handle:
            handle.write("x\n")
        assert (outside / basename / "collected.txt").exists()
        assert not (repo / basename / "collected.txt").exists()
    finally:
        shutil.rmtree(resolved, ignore_errors=True)


def test_a_scratch_retargeted_mid_run_is_refused_not_followed(tmp_path, monkeypatch):
    """The sibling-process scenario: rename the parent, symlink it at the repo.

    This needs no execution inside the gate's process tree — any process running
    as the same user can do it while the gate runs. Returning the resolved path
    closes the original `TMPDIR`-symlink case but NOT this one, because a name is
    not a directory. The gate holds a descriptor on the directory it verified, so
    every conversion of the scratch back to a string re-proves the binding.
    """
    repo, _base = _seeded(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", None)
    monkeypatch.setenv("TMPDIR", str(outside))

    scratch = gate.make_scratch_dir(str(repo))
    basename = os.path.basename(os.fspath(scratch))

    # The sibling process acts: the verified parent is renamed away and replaced
    # by a symlink to the repository, with the same basename recreated inside.
    outside.rename(tmp_path / "outside-moved")
    os.symlink(str(repo), str(outside), target_is_directory=True)
    (repo / basename).mkdir()

    # The name now resolves INSIDE the repository...
    assert os.path.realpath(os.path.join(str(outside), basename)) == os.path.realpath(
        str(repo / basename)
    )
    # ...so every use of the scratch refuses rather than following it.
    with pytest.raises(gate.GateFailure) as excinfo:
        os.fspath(scratch)
    assert excinfo.value.code == "SCRATCH_RETARGETED"
    with pytest.raises(gate.GateFailure):
        os.path.join(scratch, "collected.txt")

    # A write the gate performs itself goes through the descriptor and lands in
    # the real directory, never in the repository.
    with scratch.open_for_write("collected.txt") as handle:
        handle.write("x\n")
    assert (tmp_path / "outside-moved" / basename / "collected.txt").exists()
    assert not (repo / basename / "collected.txt").exists()

    # Cleanup removes NOTHING through the changed name — deleting through it
    # would remove a directory inside the repository, and the write must survive
    # for the closing fingerprint to see it.
    assert scratch.dispose() is False
    assert (repo / basename).exists()


def test_a_scratch_whose_binding_holds_is_released_normally(tmp_path, monkeypatch):
    repo, _base = _seeded(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", None)
    monkeypatch.setenv("TMPDIR", str(outside))
    scratch = gate.make_scratch_dir(str(repo))
    with scratch.open_for_write("collected.txt") as handle:
        handle.write("x\n")
    resolved = os.fspath(scratch)
    assert scratch.dispose() is True
    assert not os.path.exists(resolved)


def test_a_retarget_only_failure_still_reaches_the_closing_fingerprint(tmp_path, monkeypatch):
    """Retargeting must not short-circuit the check labelled UNCONDITIONAL.

    Raising `SCRATCH_RETARGETED` immediately would skip `check_worktree_unchanged`
    on the ONE path where a repository mutation is most plausible — the gate
    would report the retargeting and lose the `WORKTREE_DIRTY` evidence that says
    what it actually cost. Retargeting is therefore recorded as a pending failure
    and the closing fingerprint runs first.

    This is also why `dispose()` deletes nothing on a broken binding: the write
    has to survive for the fingerprint to have something to see.
    """
    repo, _base = _seeded(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", None)
    monkeypatch.setenv("TMPDIR", str(outside))

    scratch = gate.make_scratch_dir(str(repo))
    basename = os.path.basename(os.fspath(scratch))
    before = gate._status(str(repo))

    # A sibling retargets the parent at the repository, and the gate's write
    # lands in the worktree.
    outside.rename(tmp_path / "outside-moved")
    os.symlink(str(repo), str(outside), target_is_directory=True)
    (repo / basename).mkdir()
    (repo / basename / "leftover.txt").write_text("the gate wrote here\n")

    # Disposal refuses, and crucially leaves the evidence in place...
    assert scratch.dispose() is False
    assert (repo / basename / "leftover.txt").exists()

    # ...so the closing fingerprint can still see the tree is dirty.
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.check_worktree_unchanged(before, gate._status(str(repo)))
    assert excinfo.value.code == "WORKTREE_DIRTY"


def test_a_scratch_moved_into_the_repo_is_not_emptied(tmp_path, monkeypatch):
    """The held directory itself moves into the worktree — nothing is deleted.

    No descriptor anchoring can stop a same-user process moving the verified
    directory (inode intact) into the repository. What must not happen is the
    gate then cheerfully emptying it from its new in-repo location and exiting
    0. `dispose()` re-checks CONTAINMENT of the held descriptor, so the files
    survive for the closing fingerprint and the gate goes red.
    """
    repo, _base = _seeded(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", None)
    monkeypatch.setenv("TMPDIR", str(outside))

    scratch = gate.make_scratch_dir(str(repo))
    with scratch.open_for_write("collected.txt") as handle:
        handle.write("generated by the gate\n")

    # The sibling moves the scratch directory itself into the repository.
    moved = repo / "swallowed-scratch"
    os.rename(os.fspath(scratch), str(moved))

    assert scratch.dispose() is False
    # The generated file was NOT deleted from its new in-repo location.
    assert (moved / "collected.txt").read_text() == "generated by the gate\n"


def test_a_failing_rmdir_is_a_broken_binding_not_a_shrug(tmp_path, monkeypatch):
    """`rmdir` failing means the name stopped denoting our directory.

    Swallowing it and returning True would leave an empty directory that git
    does not track, so the closing fingerprint would match and the gate would
    exit 0 over a retargeting it had already detected.
    """
    repo, _base = _seeded(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", None)
    monkeypatch.setenv("TMPDIR", str(outside))
    scratch = gate.make_scratch_dir(str(repo))
    basename = os.path.basename(os.fspath(scratch))

    real_rmdir = os.rmdir
    seen = []

    def refuse(path, *args, **kwargs):
        # The removal is descriptor-relative, so match the BASENAME plus a
        # dir_fd — matching a full path here would silently never fire and the
        # test would pass while exercising nothing.
        if str(path) == basename and kwargs.get("dir_fd") is not None:
            seen.append(path)
            raise OSError(39, "Directory not empty")
        return real_rmdir(path, *args, **kwargs)

    monkeypatch.setattr(os, "rmdir", refuse)
    assert scratch.dispose() is False
    assert seen, "the forced failure never fired — the test would be vacuous"


def test_disposal_uses_the_scratchs_current_parent_not_a_cached_one(tmp_path, monkeypatch):
    """A parent handle captured at construction goes stale; `..` never does.

    The scenario: a sibling renames the original parent away, recreates its old
    pathname, and moves the held scratch back under the recreation. Identity and
    containment both still hold — the path names our directory and it is still
    outside the repo — but a parent descriptor captured at construction now names
    the RENAMED-AWAY directory. With a decoy of the same basename sitting there,
    `rmdir` would remove the decoy, leave the real scratch behind, and report
    success.
    """
    repo, _base = _seeded(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", None)
    monkeypatch.setenv("TMPDIR", str(outside))

    scratch = gate.make_scratch_dir(str(repo))
    basename = os.path.basename(os.fspath(scratch))
    with scratch.open_for_write("collected.txt") as handle:
        handle.write("x\n")

    # Rename the parent away, recreate its pathname, move the scratch back under
    # it, and leave a same-named decoy in the renamed-away directory.
    moved_parent = tmp_path / "outside-renamed"
    outside.rename(moved_parent)
    outside.mkdir()
    os.rename(str(moved_parent / basename), str(outside / basename))
    (moved_parent / basename).mkdir()          # the decoy

    assert scratch.dispose() is True
    # The REAL scratch is gone...
    assert not (outside / basename).exists()
    # ...and the unrelated decoy was left alone.
    assert (moved_parent / basename).is_dir()


def test_disposal_removes_the_entry_that_is_the_scratch_not_its_old_name(
    tmp_path, monkeypatch
):
    """A remembered basename is just another stale name.

    The scratch is moved to `Q/new-name`, its original path becomes a symlink to
    it, and an unrelated empty `Q/<original-name>` sits beside it. Both identity
    checks pass — `os.stat` follows the symlink — and `..` correctly yields `Q`,
    but the remembered basename denotes the unrelated directory there. Removing
    by remembered name deletes a directory the gate does not own, leaves the real
    scratch behind, and reports success.
    """
    repo, _base = _seeded(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", None)
    monkeypatch.setenv("TMPDIR", str(outside))

    scratch = gate.make_scratch_dir(str(repo))
    original = os.fspath(scratch)
    basename = os.path.basename(original)

    elsewhere = tmp_path / "Q"
    elsewhere.mkdir()
    os.rename(original, str(elsewhere / "new-name"))
    os.symlink(str(elsewhere / "new-name"), original, target_is_directory=True)
    unrelated = elsewhere / basename
    unrelated.mkdir()

    assert scratch._binding_holds(), "identity holds through the symlink"
    assert scratch.dispose() is True
    assert not (elsewhere / "new-name").exists(), "the real scratch must be gone"
    assert unrelated.is_dir(), "the unrelated directory must survive"


def test_disposal_reports_failure_when_the_removal_hit_the_wrong_directory(
    tmp_path, monkeypatch
):
    """The race that no pre-check can close is caught AFTER the fact.

    A sibling moves the emptied scratch aside and installs an unrelated empty
    directory at the scanned name between the scan and the `rmdir`. POSIX has no
    remove-by-descriptor, so no guard before the call can exclude this. The gate
    instead proves the outcome — the held directory is still linked in the
    parent, so the removal hit something else — and fails closed.
    """
    repo, _base = _seeded(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", None)
    monkeypatch.setenv("TMPDIR", str(outside))

    scratch = gate.make_scratch_dir(str(repo))
    original = os.fspath(scratch)
    basename = os.path.basename(original)

    real_rmdir = os.rmdir
    fired = []

    def race(name, *args, **kwargs):
        # Fires exactly once, in the window the finding describes.
        if not fired and kwargs.get("dir_fd") is not None and name == basename:
            fired.append(name)
            os.rename(original, str(outside / "moved-aside"))
            os.mkdir(original)                     # an unrelated empty directory
        return real_rmdir(name, *args, **kwargs)

    monkeypatch.setattr(os, "rmdir", race)
    assert scratch.dispose() is False, "a removal that hit the wrong directory must fail closed"
    assert fired, "the race never fired — the test would be vacuous"
    # The real scratch survived, which is exactly why the run must go red.
    assert (outside / "moved-aside").is_dir()


def test_an_unreadable_parent_probe_is_not_proof_of_removal(tmp_path, monkeypatch):
    """`..` failing counts only when the errno MATCHES the calibrated one.

    The first version of this test asserted `False` and passed — but passed at
    the FIRST branch, because the still-linked scratch was found by
    `_entry_naming` and the `..` code was never reached. Measured:
    `entry_naming finds it first -> wave-gate-0bo5omvy`. So the listing is
    stubbed out here, which is what forces the branch under test to run.
    """
    repo, _base = _seeded(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", None)
    monkeypatch.setenv("TMPDIR", str(outside))
    scratch = gate.make_scratch_dir(str(repo))
    assert scratch._dotdot is not None, "the platform probe must have an answer"

    held = os.fstat(scratch.fd)
    parent = os.open("..", os.O_RDONLY | gate._O_DIRECTORY, dir_fd=scratch.fd)
    real_open = os.open
    reached = []

    def blind(path, *args, **kwargs):
        if path == ".." and kwargs.get("dir_fd") == scratch.fd:
            reached.append(path)
            raise OSError(errno.EACCES, "Permission denied")
        return real_open(path, *args, **kwargs)

    try:
        # Not listed in the parent, so the '..' branch is the one that decides.
        monkeypatch.setattr(gate, "_entry_naming", lambda *a, **k: None)
        monkeypatch.setattr(os, "open", blind)

        # `..` survives removal here, so ANY error is unproven.
        assert (
            gate._removal_proved(parent, scratch.fd, held, gate._DOTDOT_SURVIVES)
            is False
        )
        # Probe could not run: unproven.
        assert gate._removal_proved(parent, scratch.fd, held, None) is False
        # Calibrated to a DIFFERENT errno: an unrelated EACCES is not the signal.
        assert gate._removal_proved(parent, scratch.fd, held, errno.ENOENT) is False
        # Calibrated to exactly this errno: that IS the platform's unlink signal.
        assert gate._removal_proved(parent, scratch.fd, held, errno.EACCES) is True
        assert reached, "the '..' branch never ran — the test would be vacuous"
    finally:
        monkeypatch.undo()
        os.close(parent)
        scratch.dispose()


def test_a_survives_calibration_is_never_matched_as_an_errno(tmp_path, monkeypatch):
    """`bool` is a subclass of `int`, and `True == errno.EPERM`.

    Measured: `isinstance(True, int) -> True`, `True == errno.EPERM -> True`.
    So a survives-calibration compared with `isinstance(..., int)` would accept
    an unrelated EPERM from the parent lookup as the calibrated unlink signal and
    report a removal that never happened. The calibration is a tag, not a bool.
    """
    repo, _base = _seeded(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", None)
    monkeypatch.setenv("TMPDIR", str(outside))
    scratch = gate.make_scratch_dir(str(repo))

    held = os.fstat(scratch.fd)
    parent = os.open("..", os.O_RDONLY | gate._O_DIRECTORY, dir_fd=scratch.fd)
    real_open = os.open
    reached = []

    def eperm(path, *args, **kwargs):
        if path == ".." and kwargs.get("dir_fd") == scratch.fd:
            reached.append(path)
            raise OSError(errno.EPERM, "Operation not permitted")
        return real_open(path, *args, **kwargs)

    try:
        monkeypatch.setattr(gate, "_entry_naming", lambda *a, **k: None)
        monkeypatch.setattr(os, "open", eperm)
        assert (
            gate._removal_proved(parent, scratch.fd, held, gate._DOTDOT_SURVIVES)
            is False
        ), "EPERM must not satisfy a survives-calibration"
        # A literal True must not either, however it reaches the function.
        assert gate._removal_proved(parent, scratch.fd, held, True) is False
        assert reached, "the '..' branch never ran — the test would be vacuous"
    finally:
        monkeypatch.undo()
        os.close(parent)
        scratch.dispose()


def test_dispose_never_raises_even_when_the_filesystem_does(tmp_path, monkeypatch):
    """`dispose()` runs in a `finally`; an escape there costs the gate its evidence.

    An exception out of cleanup replaces the pending `GateFailure` with an uncoded
    traceback AND skips the closing worktree fingerprint. The guarantee is placed
    at the boundary: no path out of `dispose()` raises, and anything unexpected
    reads as "not disposed".
    """
    repo, _base = _seeded(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", None)
    monkeypatch.setenv("TMPDIR", str(outside))
    scratch = gate.make_scratch_dir(str(repo))

    def explode(*args, **kwargs):
        raise RuntimeError("the filesystem is having a day")

    monkeypatch.setattr(gate, "_entry_naming", explode)
    assert scratch.dispose() is False


def test_dispose_is_total_even_when_closing_the_descriptor_throws(
    tmp_path, monkeypatch
):
    """The guard must cover the close, not sit beside it.

    A `finally` outside `except BaseException` runs after the handler completes,
    so whatever it raises escapes anyway. Measured against the previous shape:
    `ESCAPED the 'total' boundary: RuntimeError close blew up` — and an escape
    here costs the gate its pending failure and its closing fingerprint.
    """
    repo, _base = _seeded(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", None)
    monkeypatch.setenv("TMPDIR", str(outside))
    scratch = gate.make_scratch_dir(str(repo))
    resolved = os.fspath(scratch)

    real_close = os.close
    fired = []

    def explode(fd):
        if fd == scratch.fd:
            fired.append(fd)
            real_close(fd)
            raise RuntimeError("close blew up")
        return real_close(fd)

    monkeypatch.setattr(os, "close", explode)
    assert scratch.dispose() is False
    assert fired, "the raising close never ran — the test would be vacuous"
    monkeypatch.undo()
    shutil.rmtree(resolved, ignore_errors=True)


def test_a_directory_swapped_onto_the_candidate_name_is_refused(tmp_path, monkeypatch):
    """Containment cannot tell somebody else's directory from ours.

    `mkdtemp()` returns a NAME; a same-user process can replace the directory at
    that name before `os.open()`. An attacker's directory OUTSIDE the repository
    passes the containment check perfectly well, so without an identity+shape
    check the gate adopts their files as scratch and `dispose()` recursively
    deletes them. Reproduced against the unfixed code with a `precious.txt`
    inside.
    """
    repo, _base = _seeded(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", None)
    monkeypatch.setenv("TMPDIR", str(outside))

    real_mkdtemp = tempfile.mkdtemp
    swapped = []

    def swapping(*args, **kwargs):
        made = real_mkdtemp(*args, **kwargs)
        os.rmdir(made)
        os.makedirs(made)
        (pathlib.Path(made) / "precious.txt").write_text("real data\n")
        swapped.append(made)
        return made

    monkeypatch.setattr(tempfile, "mkdtemp", swapping)
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.make_scratch_dir(str(repo))
    assert excinfo.value.code == "SCRATCH_NOT_OURS"
    assert swapped, "the swap never happened — the test would be vacuous"
    # Nothing of theirs was deleted.
    assert (pathlib.Path(swapped[0]) / "precious.txt").exists()


def test_a_rejected_scratch_is_discarded_through_the_descriptor(tmp_path, monkeypatch):
    """The failure path must not `rmtree` the candidate PATHNAME.

    On that path the gate has just decided the candidate is untrustworthy, and
    the pathname is the thing it distrusts. Reproduced against the unfixed code:
    moving a tracked directory onto the candidate name during the failing check
    made cleanup delete the tracked subtree.
    """
    repo, _base = _seeded(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", None)
    monkeypatch.setenv("TMPDIR", str(outside))

    tracked = repo / "tracked_subtree"
    tracked.mkdir()
    (tracked / "keep.py").write_text("keep me\n")

    captured = {}
    real_refuse = gate._refuse_scratch_inside_repo

    def hijack(fd, repo_path):
        # Move the tracked subtree onto the candidate name, then fail.
        entries = [p for p in os.listdir(str(outside)) if p.startswith("wave-gate-")]
        assert entries, "no candidate to hijack — the test would be vacuous"
        captured["moved"] = outside / entries[0]
        os.rmdir(str(captured["moved"]))
        os.rename(str(tracked), str(captured["moved"]))
        raise gate._contract("SCRATCH_INSIDE_REPO", "forced for the test")

    monkeypatch.setattr(gate, "_refuse_scratch_inside_repo", hijack)
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.make_scratch_dir(str(repo))
    assert excinfo.value.code == "SCRATCH_INSIDE_REPO"
    monkeypatch.setattr(gate, "_refuse_scratch_inside_repo", real_refuse)
    # The tracked subtree survived the failure path.
    assert (captured["moved"] / "keep.py").read_text() == "keep me\n"


def test_a_usage_error_emits_the_coded_line_first(capsys):
    """The stable code must be the FIRST token on stderr, not trail argparse.

    `argparse.error()` writes its `usage:` block before raising, so merely
    catching `SystemExit` leaves a machine consumer reading usage text first —
    the documented first-token contract says otherwise.
    """
    # Bare `ci` — since #171 `--base` is a VALID selector, so driving this with
    # `ci --base HEAD` would run the whole gate against the real repository
    # instead of exercising the usage boundary.
    status = gate.main(["ci"])
    assert status == 2
    err = capsys.readouterr().err
    assert err.split()[0] == "GATE_USAGE_INVALID", err
    assert "usage:" not in err.splitlines()[0]


def test_help_is_not_a_usage_failure():
    """`--help` is a success, and it no longer travels as `SystemExit(0)`.

    The outermost boundary treats every escaping exception as a failure, so help
    signals through `_HelpRequested` instead — otherwise it would be
    indistinguishable from the exact fault that boundary exists to catch.
    """
    assert gate.main(["--help"]) == 0


def test_a_symlinked_golden_ancestor_keeps_the_validation_status(tmp_path):
    """The caller picks the failure class; the helper must not force one.

    Golden-tree diagnostics are executed-validation failures (status 1) while
    manifest reading reports contract failures (status 2). The same code
    arriving with the wrong status miscategorises it for machine consumers.
    """
    root = tmp_path / "repo"
    (root / "real" / "golden_xml").mkdir(parents=True)
    (root / "tests" / "fixtures").mkdir(parents=True)
    os.symlink(str(root / "real" / "golden_xml"),
               str(root / "tests" / "fixtures" / "golden_xml"),
               target_is_directory=True)

    # The golden tree's own class: an executed validation failed.
    with pytest.raises(gate.GateFailure) as excinfo:
        gate._refuse_symlinked_ancestor(
            str(root), "tests/fixtures/golden_xml", "GOLDEN_FILE_UNDECLARED",
            "goldens", make=gate._invalid,
        )
    assert excinfo.value.code == "GOLDEN_FILE_UNDECLARED"
    assert excinfo.value.status == 1

    # The manifest caller's class: a contract was violated.
    with pytest.raises(gate.GateFailure) as excinfo:
        gate._refuse_symlinked_ancestor(
            str(root), "tests/fixtures/golden_xml", "MANIFEST_FORMAT_INVALID",
            "manifests",
        )
    assert excinfo.value.status == 2


def test_disposal_deletes_only_what_the_gate_created(tmp_path, monkeypatch):
    """A recursive sweep destroys whatever a sibling moved in.

    Reproduced against the sweep: an unrelated subtree containing `precious.txt`
    moved into a VALID scratch was deleted while `dispose()` returned True and
    the worktree fingerprint stayed unchanged. The scratch is a directory another
    process can write to, so cleanup removes an inventory of what the gate
    created and refuses anything else.
    """
    repo, _base = _seeded(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", None)
    monkeypatch.setenv("TMPDIR", str(outside))

    scratch = gate.make_scratch_dir(str(repo))
    resolved = os.fspath(scratch)
    with scratch.open_for_write("collected.txt") as handle:
        handle.write("ours\n")

    victim = tmp_path / "victim"
    victim.mkdir()
    (victim / "precious.txt").write_text("real data\n")
    os.rename(str(victim), os.path.join(resolved, "victim"))

    assert scratch.dispose() is False
    assert scratch.refusal_code == "SCRATCH_FOREIGN_ENTRIES"
    # Theirs survives...
    assert (pathlib.Path(resolved) / "victim" / "precious.txt").read_text() == "real data\n"
    # ...and ours was still removed.
    assert not (pathlib.Path(resolved) / "collected.txt").exists()
    shutil.rmtree(resolved, ignore_errors=True)


def test_an_unexpected_systemexit_cannot_make_the_gate_green(tmp_path, monkeypatch):
    """`main()` catches only `GateFailure`, so re-raising handed the process the
    exception's own exit semantics — and `SystemExit(0)` exits GREEN.

    Unexpected exceptions are normalized to a coded status-1 failure instead.
    """
    repo, base = _seeded(tmp_path)
    fired = []

    def explode(*args, **kwargs):
        fired.append(True)
        raise SystemExit(0)

    monkeypatch.setattr(gate, "collect_nodes", explode)
    status = gate.main(["--repo", str(repo), "wave", "--base", base])
    assert fired, "collect_nodes was never reached — the test would be vacuous"
    assert status == 1, "a SystemExit(0) from inside the gate must not exit green"


def test_a_forty_digit_integer_is_not_a_bootstrap_base(tmp_path):
    """`str(value)` first would let a JSON integer match the sha pattern."""
    header = _node_header(1, 1)
    header["bootstrap_base"] = int("1" * 40)
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.parse_manifest(_serialize(header, [_node_row(1)]), "pytest-nodes")
    assert "bootstrap_base" in excinfo.value.message

    # The correct string form still parses, so the check is type-specific and
    # not an accidental rejection of every base.
    header["bootstrap_base"] = "1" * 40
    gate.parse_manifest(_serialize(header, [_node_row(1)]), "pytest-nodes")


def test_cleanup_does_not_follow_a_replaced_owned_directory(tmp_path, monkeypatch):
    """`unlink("render-1/request-1.json")` follows a replaced `render-1`.

    A same-user process swapping an owned directory for a symlink would have the
    gate delete the TARGET's file of that name and then report success. Every
    component is opened `O_NOFOLLOW` from its parent's descriptor instead.
    """
    repo, _base = _seeded(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", None)
    monkeypatch.setenv("TMPDIR", str(outside))

    scratch = gate.make_scratch_dir(str(repo))
    resolved = os.fspath(scratch)
    os.close(scratch.mkdir_owned("render-1"))
    scratch.own("render-1/request-1.json")

    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    (elsewhere / "request-1.json").write_text("someone else's file\n")
    os.rmdir(os.path.join(resolved, "render-1"))
    os.symlink(str(elsewhere), os.path.join(resolved, "render-1"),
               target_is_directory=True)

    assert scratch.dispose() is False
    assert scratch.refusal_code == "SCRATCH_FOREIGN_ENTRIES"
    assert (elsewhere / "request-1.json").read_text() == "someone else's file\n"
    shutil.rmtree(resolved, ignore_errors=True)


def test_a_foreign_file_nested_in_an_owned_directory_is_classified(tmp_path, monkeypatch):
    """Its parent's `rmdir` fails first, so it never reaches the top-level listing.

    Without classifying it there, the code stayed `SCRATCH_RETARGETED` and a
    machine consumer got the wrong contract token for a foreign entry.
    """
    repo, _base = _seeded(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", None)
    monkeypatch.setenv("TMPDIR", str(outside))

    scratch = gate.make_scratch_dir(str(repo))
    resolved = os.fspath(scratch)
    os.close(scratch.mkdir_owned("render-1"))
    (pathlib.Path(resolved) / "render-1" / "intruder.txt").write_text("x\n")

    assert scratch.dispose() is False
    assert scratch.refusal_code == "SCRATCH_FOREIGN_ENTRIES"
    shutil.rmtree(resolved, ignore_errors=True)


def test_an_owned_child_must_still_be_named_by_its_parent(tmp_path, monkeypatch):
    """The scratch root re-proves its binding; every owned child must too.

    A sibling can rename an opened `render-*` between `os.open` and the recursive
    deletion, after which deletion proceeds through a still-valid descriptor while
    the ROOT's checks keep passing. This is the root's existing proof applied
    uniformly, not a new mechanism.
    """
    repo, _base = _seeded(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", None)
    monkeypatch.setenv("TMPDIR", str(outside))

    scratch = gate.make_scratch_dir(str(repo))
    child = scratch.mkdir_owned("render-9")
    try:
        # While bound, the proof accepts.
        gate._refuse_unbound_child(scratch.fd, "render-9", child)
        # Renamed away: the name no longer denotes the held directory.
        os.rename(os.path.join(os.fspath(scratch), "render-9"),
                  str(tmp_path / "stolen"))
        with pytest.raises(gate._ForeignEntry):
            gate._refuse_unbound_child(scratch.fd, "render-9", child)
    finally:
        os.close(child)
        scratch.dispose()


def test_content_moved_into_the_worktree_is_caught_by_the_fingerprint(tmp_path):
    """The bound on every remaining scratch-disposal race, measured.

    A NON-empty directory appearing in the worktree is `WORKTREE_DIRTY`; only an
    EMPTY one is invisible, because git does not track empty directories. That is
    why the residual tracked in #164 cannot smuggle content past the gate.
    """
    repo, _base = _seeded(tmp_path)
    before = gate._status(str(repo))

    (repo / "render-1").mkdir()
    (repo / "render-1" / "request-1.json").write_text("{}")
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.check_worktree_unchanged(before, gate._status(str(repo)))
    assert excinfo.value.code == "WORKTREE_DIRTY"

    shutil.rmtree(str(repo / "render-1"))
    (repo / "render-1").mkdir()
    # Empty: git tracks nothing, so the fingerprint cannot see it. Stated, not
    # hidden — this is exactly the bound recorded in #164.
    gate.check_worktree_unchanged(before, gate._status(str(repo)))


def test_a_foreign_file_at_an_owned_name_is_never_taken_over(tmp_path, monkeypatch):
    """`O_CREAT|O_TRUNC` does not create — it TAKES OVER.

    A file a sibling already placed at the name was truncated, recorded as ours,
    and deleted at disposal, with the worktree fingerprint none the wiser.
    Reproduced: `open_for_write ACCEPTED and truncated the foreign file`,
    `dispose -> True`. Creation must BE the proof of ownership.
    """
    repo, _base = _seeded(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", None)
    monkeypatch.setenv("TMPDIR", str(outside))

    scratch = gate.make_scratch_dir(str(repo))
    resolved = os.fspath(scratch)
    (pathlib.Path(resolved) / "collected.txt").write_text("someone else's data\n")

    with pytest.raises(FileExistsError):
        scratch.open_for_write("collected.txt")

    # Untouched, and disposal refuses rather than deleting what it never owned.
    assert (pathlib.Path(resolved) / "collected.txt").read_text() == "someone else's data\n"
    assert scratch.dispose() is False
    assert scratch.refusal_code == "SCRATCH_FOREIGN_ENTRIES"
    assert (pathlib.Path(resolved) / "collected.txt").exists()
    shutil.rmtree(resolved, ignore_errors=True)


def test_the_closing_fingerprint_cannot_exit_green_on_an_unexpected_error(
    tmp_path, monkeypatch
):
    """The LAST step was outside the normalization boundary.

    `_status()` shells out to git and hashes files; an unexpected exception there
    escaped `main()` with its own exit semantics, so a `SystemExit(0)` from the
    very last step exited GREEN after everything else had passed.
    """
    repo, base = _seeded(tmp_path)
    real_status = gate._status
    calls = []

    def explode(target):
        calls.append(target)
        if len(calls) > 1:            # the CLOSING call, not the opening one
            raise SystemExit(0)
        return real_status(target)

    # The heavy phases are stubbed so the run reaches the CLOSING fingerprint,
    # which is the step under test; each stub returns what its caller expects.
    monkeypatch.setattr(gate, "collect_nodes", lambda repo_, tmp: {"tests/t.py::a"})
    monkeypatch.setattr(gate, "check_collection", lambda manifest, collected: None)
    monkeypatch.setattr(
        gate, "run_suite", lambda repo_, manifest, collected: {"passed": 1, "skipped": 0}
    )
    monkeypatch.setattr(gate, "check_goldens", lambda repo_, goldens, tmp: 0)
    monkeypatch.setattr(gate, "_status", explode)

    status = gate.main(["--repo", str(repo), "wave", "--base", base])
    assert len(calls) >= 2, "the closing fingerprint never ran — the test would be vacuous"
    assert status != 0, "a SystemExit(0) from the closing fingerprint must not exit green"


class _HostileStr(Exception):
    """Its rendering tries to exit green — the classic route onto the exit path."""

    def __str__(self):
        raise SystemExit(0)


@pytest.mark.parametrize(
    "raised",
    [
        pytest.param(SystemExit(0), id="SystemExit-0"),
        pytest.param(KeyboardInterrupt(), id="KeyboardInterrupt"),
        pytest.param(GeneratorExit(), id="GeneratorExit"),
        pytest.param(RuntimeError("boom"), id="RuntimeError"),
        pytest.param(_HostileStr(), id="hostile-__str__"),
    ],
)
def test_no_exception_can_decide_the_gates_exit_status(tmp_path, monkeypatch, raised):
    """THE invariant: the gate decides its exit status, never an exception.

    Non-vacuity witness — `SystemExit(0)` raised from the OPENING fingerprint is a
    concrete case the invariant excludes and which previously exited GREEN
    (measured: `SystemExit ESCAPED main() with code: 0`).

    Coverage claim over the authority's full case set: every exception crossing
    `main()` falls into one of these partitions — an ordinary `Exception`, a
    `BaseException` that is NOT an `Exception` (`SystemExit`, `KeyboardInterrupt`,
    `GeneratorExit`), or one whose rendering itself misbehaves. All are covered
    here, and the boundary reads nothing about the object, so no case can be
    distinguished by the object's own code.

    Five instances of this class were found one at a time, each a statement
    further out, before a sibling sweep of all 51 `try` blocks located the last.
    """
    repo, base = _seeded(tmp_path)

    def boom(target):
        raise raised

    monkeypatch.setattr(gate, "_status", boom)
    status = gate.main(["--repo", str(repo), "manifests", "--base", base])
    assert status == 1, "an exception must never decide the gate's exit status"


def test_help_and_usage_survive_the_outermost_boundary(tmp_path, capsys):
    """The boundary treats every escape as failure, so help needs its own signal.

    `--help` must stay 0 and a usage error must stay 2; relying on `SystemExit(0)`
    for help would make it indistinguishable from the very thing the boundary
    exists to catch.
    """
    assert gate.main(["--help"]) == 0
    assert gate.main(["ci"]) == 2          # bare `ci` — see #171 note above
    assert capsys.readouterr().err.split()[0] == "GATE_USAGE_INVALID"


def test_a_throwing_diagnostic_sink_cannot_decide_the_exit_status(tmp_path, monkeypatch):
    """Reporting runs INSIDE a handler, where a raise escapes the enclosing try.

    The last-resort diagnostic — including the `GATE_DIAGNOSTIC_UNRENDERABLE`
    fallback — used an unguarded `_emit`, so a sink that raises `SystemExit(0)`
    exited GREEN from the very handler that exists to prevent it. Sibling sweep:
    every `_emit` inside an `except` suite now goes through `_report`, which
    cannot throw.

    Both paths are covered: an unexpected error, and an ordinary `GateFailure`
    whose status was already decided before rendering.
    """
    repo, _base = _seeded(tmp_path)

    def hostile_sink(text):
        raise SystemExit(0)

    monkeypatch.setattr(gate, "_emit", hostile_sink)

    # A) unexpected error inside the gate
    monkeypatch.setattr(gate, "_status", lambda target: (_ for _ in ()).throw(RuntimeError("x")))
    assert gate.main(["--repo", str(repo), "manifests", "--base", "HEAD"]) == 1

    # B) an ordinary GateFailure — status decided before rendering, and reporting
    #    cannot take it back
    monkeypatch.undo()
    monkeypatch.setattr(gate, "_emit", hostile_sink)
    assert gate.main(["--repo", str(repo), "manifests", "--base", "no-such-ref"]) == 2


def test_reporting_still_prints_a_code_when_the_sink_rejects_the_message(monkeypatch):
    """Not throwing is only half the obligation; a code must still be printed.

    An earlier revision of `_report` met the first half by swallowing every
    failure — which silently abandoned the second. A stderr configured as strict
    ASCII rejects the em-dash in `_refuse_ambiguous`'s message
    (`wave_gate.py:290`), and the gate then exited with EMPTY stderr and no
    machine-readable code at all.
    """
    printed = []

    def ascii_only(text):
        text.encode("ascii")          # a strict sink, exactly like the real case
        printed.append(text)

    monkeypatch.setattr(gate, "_emit", ascii_only)

    # A) the full text is unprintable; the ASCII code survives. The code must be
    #    one the gate actually owns — an invented token is refused by the sink.
    gate._report("BASELINE_UNAVAILABLE 'x' is ambiguous \u2014 git reports: y",
                 "BASELINE_UNAVAILABLE")
    assert printed == ["BASELINE_UNAVAILABLE"]

    # B) no fallback offered: the fixed ASCII line is the last rung.
    printed.clear()
    gate._report("SOMETHING \u2014 bad")
    assert printed and printed[0].startswith("GATE_DIAGNOSTIC_UNRENDERABLE")

    # C) every rung fails: still no exception, because reporting must never
    #    decide the exit status.
    monkeypatch.setattr(gate, "_emit", lambda t: (_ for _ in ()).throw(SystemExit(0)))
    gate._report("x", "Y")


def test_the_fallback_never_emits_an_undocumented_code(monkeypatch):
    """A shape-only `[A-Z_]+` test is not membership in the gate's code set.

    A provider-created or post-construction-mutated `GateFailure` can carry any
    matching token — `UNDECLARED_CODE` — and emitting it puts an undocumented
    code in the position the machine contract reserves for a documented one.
    """
    printed = []

    def ascii_only(text):
        text.encode("ascii")
        printed.append(text)

    monkeypatch.setattr(gate, "_emit", ascii_only)

    # An undocumented token that PASSES a shape check is refused as a fallback,
    # and the generic ASCII line is used instead.
    gate._report("UNDECLARED_CODE something \u2014 bad", "UNDECLARED_CODE")
    assert printed and printed[0].startswith("GATE_DIAGNOSTIC_UNRENDERABLE")

    # A real gate code is still reused, because it IS documented.
    printed.clear()
    gate._report("WORKTREE_DIRTY something \u2014 bad", "WORKTREE_DIRTY")
    assert printed == ["WORKTREE_DIRTY"]


class _HostileCode(str):
    """A `str` SUBCLASS whose `__hash__` runs on any membership test."""

    def __hash__(self):
        raise SystemExit(0)


def test_the_code_whitelist_cannot_run_foreign_code(monkeypatch):
    """`x in DIAGNOSTIC_CODES` executes `__hash__`/`__eq__`.

    So the lookup meant to SANITIZE a failure's code was itself a route onto the
    exit path — measured before the fix: `ESCAPED as SystemExit -> would exit 0`.
    `type(x) is str` admits only the exact builtin, whose `__hash__` and `__eq__`
    cannot be overridden. This is the same class `exit_status_for` documents four
    rounds of, reintroduced through a membership test.
    """
    printed = []
    monkeypatch.setattr(gate, "_emit", printed.append)

    # The sink survives a hostile fallback. `text=None` so the ladder actually
    # REACHES the fallback rung — with a printable primary text it would stop at
    # rung one and the hostile object would never be touched, which is a test
    # that proves nothing.
    gate._report(None, _HostileCode("WORKTREE_DIRTY"))
    assert printed and printed[0].startswith("GATE_DIAGNOSTIC_UNRENDERABLE")

    # ...and so does the whole gate, which must still report nonzero.
    printed.clear()
    monkeypatch.setattr(
        gate, "execute",
        lambda args: (_ for _ in ()).throw(
            gate.GateFailure(_HostileCode("WORKTREE_DIRTY"), "m", 1)
        ),
    )
    assert gate.main(["manifests", "--base", "HEAD"]) == 1
    assert printed and printed[0].startswith("GATE_DIAGNOSTIC_UNRENDERABLE")


def test_an_undocumented_code_never_reaches_the_first_stderr_token(monkeypatch):
    """Validating only the FALLBACK left the primary text free to carry it.

    Measured before the fix: `GateFailure("UNDECLARED_CODE", "m", 1)` printed
    `UNDECLARED_CODE m`, so `DIAGNOSTIC_CODES` was not actually authoritative.
    The primary text is now built FROM the resolved code, not from
    `failure.code`.
    """
    printed = []
    monkeypatch.setattr(gate, "_emit", printed.append)

    monkeypatch.setattr(
        gate, "execute",
        lambda args: (_ for _ in ()).throw(gate.GateFailure("UNDECLARED_CODE", "m", 1)),
    )
    assert gate.main(["manifests", "--base", "HEAD"]) == 1
    assert printed and printed[0].startswith("GATE_DIAGNOSTIC_UNRENDERABLE")
    assert not any("UNDECLARED_CODE" in line for line in printed)

    # A documented code still renders with its full message — the check must not
    # have degraded every diagnostic to the generic line.
    printed.clear()
    monkeypatch.setattr(
        gate, "execute",
        lambda args: (_ for _ in ()).throw(
            gate.GateFailure("WORKTREE_DIRTY", "details here", 1)
        ),
    )
    assert gate.main(["manifests", "--base", "HEAD"]) == 1
    assert printed == ["WORKTREE_DIRTY details here"]


def test_provider_strings_reject_str_subclasses():
    """The mutation-kind roster is checked by EQUALITY, so a subclass subverts it.

    A `str` subclass whose `__eq__` matches everything makes the roster believe
    `semantic`, `envelope`, `policy` and `revision` are all present, after which
    the phase reports `checked:1 case(s)` having exercised ONE mutation. That is
    a fail-open in the #153 seam.

    Sibling sweep: the other nine `isinstance(..., str)` checks in the gate
    validate JSON-derived values, and `json.loads` yields exactly `str`
    (measured), so a subclass cannot reach them. This site takes PROVIDER data —
    registered Python code — which is the trust boundary the seam polices.
    """

    class AnyStr(str):
        def __eq__(self, other):
            return True

        def __hash__(self):
            return hash("semantic")

    with pytest.raises(gate.GateFailure) as excinfo:
        gate._provider_strings(lambda: [AnyStr("semantic")], "mutation kinds")
    assert excinfo.value.code == "PLAN_FINGERPRINT_MISMATCH"

    # Plain strings are unaffected — the check must not reject honest providers.
    assert gate._provider_strings(
        lambda: ["semantic", "envelope"], "mutation kinds"
    ) == ["semantic", "envelope"]


def test_a_second_outcome_summary_is_refused(capsys):
    """Anything printed AFTER pytest's summary must not become the outcome.

    Reproduced against the previous parser: a genuine
    `9740 passed, 33 skipped ... in 700.00s` followed by an `atexit` line reading
    `9773 passed in 0.01s` parsed as 9773 passed and ZERO skipped — clearing the
    floor and hiding skips above the cap in one step. Two summaries are not a tie
    to break by position; they mean the stream is not something an outcome can be
    read from.
    """
    real = "==== 9740 passed, 33 skipped, 20 warnings in 700.00s (0:11:40) ===="
    assert gate._parse_suite_summary(real) == {
        "passed": 9740, "failed": 0, "skipped": 33, "errors": 0
    }

    with pytest.raises(gate.GateFailure) as excinfo:
        gate._parse_suite_summary(real + "\nchatter\n==== 9773 passed in 0.01s ====")
    assert excinfo.value.code == "PYTEST_SUMMARY_AMBIGUOUS"
    assert excinfo.value.status == 1


def test_a_summary_evicted_from_the_tail_is_still_counted(tmp_path, monkeypatch, capsys):
    """The one-summary rule must bind on the STREAM, not on the retained tail.

    `run_suite` keeps a bounded 400-line ring for error context. Applying the rule
    to that ring enforced it against a DERIVED view: an `atexit` handler emitting
    400+ lines after pytest's genuine summary evicts it, leaving the fabricated
    one alone in the buffer — exactly one summary, zero skips, and a mass-skipped
    suite passes. Measured on the tail-only view:
    `{'passed': 9773, 'failed': 0, 'skipped': 0, 'errors': 0}`.

    This drives `run_suite` itself. A test that only called `_parse_suite_summary`
    would have passed against the broken version, because the parser was never
    the defect.
    """
    lines = (
        ["==== 9740 passed, 33 skipped in 700.00s ===="]
        + ["noise line %d" % i for i in range(450)]
        + ["==== 9773 passed in 0.01s ===="]
    )

    class _FakeProc:
        stdout = iter("%s\n" % line for line in lines)

        def wait(self):
            return 0

    monkeypatch.setattr(gate.subprocess, "Popen", lambda *a, **k: _FakeProc())

    manifest = gate.parse_manifest(_default_nodes(), "pytest-nodes")
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.run_suite(str(tmp_path), manifest, {"tests/test_x.py::test_1"})
    assert excinfo.value.code == "PYTEST_SUMMARY_AMBIGUOUS"


def test_two_outcomes_on_one_physical_line_are_refused():
    """Counting LINES is not counting outcomes.

    `100 passed, 0 skipped in 0.01s 100 skipped in 0.01s` is a single line, so
    the one-summary rule saw one candidate; the first clause of each kind then
    won, reporting 100 passed and 0 skipped for a fully skipped run. The grammar
    is now anchored with exactly one duration clause.
    """
    with pytest.raises(gate.GateFailure) as excinfo:
        gate._parse_suite_summary(
            "==== 100 passed, 0 skipped in 0.01s 100 skipped in 0.01s ===="
        )
    assert excinfo.value.code == "PYTEST_SUMMARY_UNPARSEABLE"

    # A repeated clause inside a well-formed line is ambiguous, not first-wins.
    with pytest.raises(gate.GateFailure) as excinfo:
        gate._parse_suite_summary("==== 1 passed, 2 passed in 0.01s ====")
    assert excinfo.value.code == "PYTEST_SUMMARY_AMBIGUOUS"

    # The real thing still parses — a stricter grammar must not reject it.
    assert gate._parse_suite_summary(
        "==== 9757 passed, 18 skipped, 20 warnings in 776.50s (0:12:56) ===="
    ) == {"passed": 9757, "failed": 0, "skipped": 18, "errors": 0}


def test_an_outcome_the_gate_cannot_account_for_is_refused():
    """An outcome vocabulary open by omission let `deselected` through.

    A hook can retain a required node at COLLECTION and deselect it at
    EXECUTION; `deselected` was parsed as nothing, so the missing pass hid inside
    the skip-cap headroom and a required test avoided execution while the gate
    went green.
    """
    for line in (
        "== 229 passed, 1 deselected in 1.0s ==",
        "== 10 passed, 1 xpassed in 1.0s ==",
        "== 10 passed, 1 xfailed in 1.0s ==",
    ):
        with pytest.raises(gate.GateFailure) as excinfo:
            gate._parse_suite_summary(line)
        assert excinfo.value.code == "PYTEST_OUTCOME_UNACCOUNTED", line

    # `failed`/`error` remain parseable so the SPECIFIC diagnostic still wins.
    assert gate._parse_suite_summary("== 10 passed, 2 failed in 1.0s ==")["failed"] == 2


def test_every_collected_test_must_be_accounted_for(tmp_path, monkeypatch):
    """The accounting identity: passed + skipped == collected.

    Without it a collected-but-never-executed test simply vanishes from the
    arithmetic. Driven through `run_suite`, because that is where collection and
    outcome meet.
    """
    lines = ["==== 229 passed, 1 skipped in 1.00s ===="]

    class _FakeProc:
        stdout = iter("%s\n" % line for line in lines)

        def wait(self):
            return 0

    monkeypatch.setattr(gate.subprocess, "Popen", lambda *a, **k: _FakeProc())
    manifest = gate.parse_manifest(_default_nodes(), "pytest-nodes")

    # 230 accounted, 230 collected -> fine.
    collected = {"tests/test_x.py::t%d" % i for i in range(230)}
    gate.run_suite(str(tmp_path), manifest, collected)

    # 231 collected, still only 230 accounted -> the missing one is refused.
    lines[:] = ["==== 229 passed, 1 skipped in 1.00s ===="]
    _FakeProc.stdout = iter("%s\n" % line for line in lines)
    collected.add("tests/test_x.py::vanished")
    with pytest.raises(gate.GateFailure) as excinfo:
        gate.run_suite(str(tmp_path), manifest, collected)
    assert excinfo.value.code == "PYTEST_OUTCOME_UNACCOUNTED"


def test_the_render_envelope_must_verify_itself():
    """The child declares `len` and `sha256`; reading only `b64` ignored both.

    Reproduced: `{"len": 0, "sha256": "not-the-payload", "b64": "PHgvPgo=!!!"}`
    decoded to `b"<x/>\\n"` and was accepted, because `b64decode` without
    `validate=True` silently drops non-alphabet characters. Two such envelopes
    could then agree with each other AND with the expected bytes while violating
    the child protocol outright.
    """
    import base64 as _b64
    import hashlib as _hashlib

    payload = b"<x/>\n"
    good = {
        "id": "golden-000001",
        "len": len(payload),
        "sha256": _hashlib.sha256(payload).hexdigest(),
        "b64": _b64.b64encode(payload).decode("ascii"),
    }
    # The honest envelope still decodes — the check must not reject the protocol
    # the child actually speaks.
    assert gate._envelope_payload(good, "golden-000001") == payload

    for name, envelope in (
        ("non-canonical base64", dict(good, b64="PHgvPgo=!!!", len=0,
                                      sha256="not-the-payload")),
        ("unknown field", dict(good, extra=1)),
        ("declared length wrong", dict(good, len=99)),
        ("declared digest wrong", dict(good, sha256="0" * 64)),
    ):
        with pytest.raises(gate.GateFailure) as excinfo:
            gate._envelope_id(envelope, "{}")
            gate._envelope_payload(envelope, "golden-000001")
        assert excinfo.value.code == "GOLDEN_RENDER_FAILED", name


def test_error_spellings_are_canonicalized_before_duplicate_detection():
    """`1 error` and `0 errors` are ONE outcome, not two.

    Keying the duplicate check on the literal word let a summary carry both
    spellings: `1 passed, 1 error, 0 errors` passed as two distinct outcomes and
    the plural-first lookup read the ZERO, so a reported error vanished and the
    run was accepted. Reproduced: `{'passed': 1, 'errors': 0}`.
    """
    with pytest.raises(gate.GateFailure) as excinfo:
        gate._parse_suite_summary("== 1 passed, 1 error, 0 errors in 1.0s ==")
    assert excinfo.value.code == "PYTEST_SUMMARY_AMBIGUOUS"

    # Both spellings, alone, are recorded — canonicalization must not lose either.
    assert gate._parse_suite_summary("== 1 passed, 1 error in 1.0s ==")["errors"] == 1
    assert gate._parse_suite_summary("== 1 passed, 2 errors in 1.0s ==")["errors"] == 2

    # And the real line is unaffected.
    assert gate._parse_suite_summary(
        "9761 passed, 18 skipped, 20 warnings in 776.00s (0:12:55)"
    ) == {"passed": 9761, "failed": 0, "skipped": 18, "errors": 0}


def test_execution_is_reconciled_by_identity_not_by_count():
    """`passed + skipped == collected` is necessary but NOT sufficient.

    A `pytest_collection_modifyitems` hook that replaces node B with a second
    copy of node A keeps the count identity exactly true while B — which fails —
    never executes. junit's `(classname, name)` records are compared as a
    MULTISET against the same projection of the collected node ids, so a missing
    node and a doubled one are both caught.
    """
    assert gate._junit_projection("tests/test_x.py::test_foo") == (
        "tests.test_x", "test_foo"
    )
    assert gate._junit_projection("tests/patterns/test_x.py::TestC::test_bar[p1]") == (
        "tests.patterns.test_x.TestC", "test_bar[p1]"
    )
    # A REAL node id from this suite whose parameter contains `::` — an IPv6
    # literal. A naive `split("::")` shreds it, and the hand-written ids above
    # never showed it; the 9,782-test run did, with
    # `PYTEST_EXECUTION_UNRECONCILED` on the first honest execution.
    assert gate._junit_projection(
        "tests/test_loopback_redirect_patch.py::test_loopback_port_flexibility"
        "[http://[::1]:9999/callback-http://[::1]/callback-True]"
    ) == (
        "tests.test_loopback_redirect_patch",
        "test_loopback_port_flexibility"
        "[http://[::1]:9999/callback-http://[::1]/callback-True]",
    )

    collected = {"tests/t.py::test_a", "tests/t.py::test_b"}
    expected = sorted(gate._junit_projection(n) for n in collected)
    # A executed twice, B never — the count identity would still hold.
    executed = sorted([("tests.t", "test_a"), ("tests.t", "test_a")])
    assert len(executed) == len(expected), "the count identity is satisfied"
    assert executed != expected, "identity reconciliation must still refuse it"


def test_untrusted_json_rejects_duplicate_members():
    """`json.loads` keeps the LAST value for a repeated key.

    So an event can carry two conflicting `before`/`after` pairs, and a render
    envelope two conflicting `sha256` members, with the checks downstream seeing
    only one of them — the document is ambiguous and the gate silently picks a
    side.
    """
    for text in (
        '{"before":"a","before":"b"}',
        '{"id":"g","id":"h","len":1,"sha256":"x","b64":"y"}',
    ):
        with pytest.raises(ValueError):
            gate._strict_json_loads(text)

    # Well-formed JSON is unaffected.
    assert gate._strict_json_loads('{"a":1,"b":2}') == {"a": 1, "b": 2}


def test_the_junit_report_must_be_the_file_the_gate_created(tmp_path, monkeypatch):
    """The child opens the report by NAME, so ownership needs a proof.

    Claiming the name alone let a sibling replace the report between the run and
    the parse — forging the very execution evidence the check exists to
    establish — and a pre-planted symlink would have been opened with `w` by
    pytest, truncating an arbitrary file.
    """
    repo, _base = _seeded(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", None)
    monkeypatch.setenv("TMPDIR", str(outside))
    scratch = gate.make_scratch_dir(str(repo))
    try:
        scratch.open_for_write("junit.xml").close()
        fd = os.open("junit.xml", os.O_RDONLY | gate._O_NOFOLLOW, dir_fd=scratch.fd)
        try:
            # Unmolested: the held descriptor is the named file.
            gate._refuse_unowned_report(scratch, fd)

            # Replaced during the run: same name, different inode.
            os.unlink("junit.xml", dir_fd=scratch.fd)
            scratch.open_for_write("junit.xml").close()
            with pytest.raises(gate.GateFailure) as excinfo:
                gate._refuse_unowned_report(scratch, fd)
            assert excinfo.value.code == "PYTEST_EXECUTION_UNRECONCILED"
        finally:
            os.close(fd)
    finally:
        shutil.rmtree(os.fspath(scratch), ignore_errors=True)


def test_a_non_injective_junit_projection_is_refused():
    """If two node ids share one junit identity, this is not identity at all.

    `tests/a.py::B::test_x` and `tests/a/B.py::test_x` both project to
    `('tests.a.B', 'test_x')`, so omitting one and running the other twice leaves
    the multisets equal. Measured on this suite: 0 collisions among 9,782 active
    node ids, so the assertion constrains nothing real.
    """
    assert gate._junit_projection("tests/a.py::B::test_x") == gate._junit_projection(
        "tests/a/B.py::test_x"
    )
    collided = {"tests/a.py::B::test_x", "tests/a/B.py::test_x"}
    projections = [gate._junit_projection(n) for n in collided]
    assert len(set(projections)) != len(projections), (
        "the collision must be detectable by the injectivity check"
    )


def test_the_report_anchor_survives_a_hard_link_swap(tmp_path, monkeypatch):
    """The `O_EXCL` descriptor IS the anchor; closing and reopening loses it.

    Reproduced against the create-close-reopen shape: a sibling atomically
    replaces `junit.xml` with a hard link to another same-filesystem file in the
    gap, the reopened descriptor and the later pathname stat then identify the
    SAME FOREIGN inode, the ownership check passes, and the child truncates
    somebody else's file with `w`.
    """
    repo, _base = _seeded(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", None)
    monkeypatch.setenv("TMPDIR", str(outside))
    scratch = gate.make_scratch_dir(str(repo))
    resolved = os.fspath(scratch)
    victim = tmp_path / "precious.txt"
    victim.write_text("someone else's data\n")

    fd = scratch.create_owned("junit.xml")
    try:
        # Unmolested, the anchor verifies.
        gate._refuse_unowned_report(scratch, fd)

        # A sibling hard-links a foreign file over the name.
        os.link(str(victim), os.path.join(resolved, "swapped"))
        os.rename(os.path.join(resolved, "swapped"),
                  os.path.join(resolved, "junit.xml"))
        with pytest.raises(gate.GateFailure) as excinfo:
            gate._refuse_unowned_report(scratch, fd)
        assert excinfo.value.code == "PYTEST_EXECUTION_UNRECONCILED"
        assert victim.read_text() == "someone else's data\n"
    finally:
        os.close(fd)
        shutil.rmtree(resolved, ignore_errors=True)


def test_non_standard_json_constants_are_refused(tmp_path):
    """Python's decoder accepts `NaN`/`Infinity`; JSON does not.

    A push event with valid `before`/`after` SHAs plus `"commits": NaN` was
    accepted and could complete green — the gate would have treated a document
    that is not JSON as an authoritative event. Reproduced:
    `ACCEPTED non-standard JSON: {'commits': nan}`.
    """
    for text in ('{"a": NaN}', '{"a": Infinity}', '{"a": -Infinity}'):
        with pytest.raises(ValueError):
            gate._strict_json_loads(text)

    # Ordinary JSON is unaffected.
    assert gate._strict_json_loads('{"a": 1, "b": "x"}') == {"a": 1, "b": "x"}

    # End to end: the event arm reports the documented code.
    repo, _base = _seeded(tmp_path)
    event = tmp_path / "push.json"
    event.write_text(
        '{"before":"%s","after":"%s","ref":"refs/heads/dev","commits": NaN}'
        % ("a" * 40, "b" * 40)
    )
    proc = subprocess.run(
        [sys.executable, str(_ROOT / "scripts" / "wave_gate.py"),
         "--repo", str(repo), "manifests", "--github-event", str(event)],
        capture_output=True, text=True,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1",
             "GITHUB_EVENT_NAME": "push"},
    )
    assert proc.returncode == 2, proc.stderr
    assert proc.stderr.split()[0] == "BASELINE_EVENT_INVALID", proc.stderr


def test_a_failure_whose_diagnostic_explodes_still_exits_nonzero(capsys):
    """The exit decision precedes rendering, so no dunder can reach it.

    Four rounds each found a different special method by which a hostile
    provider could influence the exit path through the DIAGNOSTIC — `__name__`
    via a metaclass, `__repr__` in a formatter, `__eq__` on an int subclass,
    `__hash__`/`__str__` on a str subclass. Patching them individually cannot
    terminate; deciding before rendering does.
    """
    class _Explosive(str):
        def __str__(self):
            raise SystemExit(0)

        def __format__(self, spec):
            raise SystemExit(0)

    failure = gate.GateFailure(_Explosive("X"), _Explosive("m"), 1)

    original = gate.execute

    def _boom(_args):
        raise failure

    try:
        gate.execute = _boom
        assert gate.main(["manifests", "--base", "HEAD"]) == 1
    finally:
        gate.execute = original

    # The stderr contract holds even here: the first token is a documented code,
    # because a failure nobody can classify is a failure nobody acts on.
    err = capsys.readouterr().err
    assert err.split()[0] == "GATE_DIAGNOSTIC_UNRENDERABLE", err


def test_the_exit_status_is_recomputed_not_trusted():
    """A constructor invariant only holds at construction.

    `GateFailure` is mutable and subclassable, so `.status` can be reassigned
    afterwards. Enforcing the rule where the value is CONSUMED closes that
    regardless — and covers ordinary bugs, not only adversarial ones.
    """
    failure = gate.GateFailure("X", "m", 2)
    failure.status = 0                       # mutated after construction
    assert gate.exit_status_for(failure) == 1

    class _Sub(gate.GateFailure):
        def __init__(self):
            self.code, self.message, self.status = "X", "m", 0

    assert gate.exit_status_for(_Sub()) == 1

    class _Liar(int):
        def __eq__(self, other):
            return True

        def __hash__(self):
            return 0

    failure.status = _Liar(0)
    assert gate.exit_status_for(failure) == 1

    # A genuine contract failure still reports 2.
    assert gate.exit_status_for(gate.GateFailure("X", "m", 2)) == 2


def test_a_lying_int_subclass_cannot_become_a_success_status():
    """[P1] An int subclass whose `__eq__` reports 1 while its value is 0 passed
    an `in (1, 2)` test and was then read as 0 by `sys.exit()`."""
    class _Liar(int):
        def __eq__(self, other):
            return other in (1, 2)

        def __hash__(self):
            return 0

    failure = gate.GateFailure("X", "m", _Liar(0))
    assert type(failure.status) is int
    assert failure.status == 1
    assert int(failure.status) == 1        # what sys.exit() actually reads


def test_a_provider_output_whose_dunders_run_code_cannot_exit_green():
    """[P1] Provider outputs are exercised AFTER the guarded call.

    A tuple subclass whose `__len__` raises reached the orchestration boundary.
    Outputs are now validated as EXACT built-ins, so an override never runs.
    """
    class _Hostile(tuple):
        def __len__(self):
            raise gate.GateFailure("PLAN_FINGERPRINT_MISMATCH", "green", 0)

    class _Provider(_StubProvider):
        def fingerprint(self, case, *, account, environment, mutation=None):
            return _Hostile(("sha256:" + "0" * 64, b"x"))

    with pytest.raises(gate.GateFailure) as excinfo:
        gate.run_plan_fingerprint_checks(True, _Provider())
    assert excinfo.value.status == 1

    # A str subclass for the digest is likewise refused before it is compared.
    class _SneakyStr(str):
        pass

    class _StrProvider(_StubProvider):
        def fingerprint(self, case, *, account, environment, mutation=None):
            return (_SneakyStr("sha256:" + "0" * 64), b"x")

    with pytest.raises(gate.GateFailure) as excinfo:
        gate.run_plan_fingerprint_checks(True, _StrProvider())
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


def test_git_stderr_is_read_under_a_pinned_locale(tmp_path, monkeypatch):
    """[P1] The access-failure match is English; the locale must be pinned.

    Under a non-English `LC_MESSAGES`, git and libc localise both
    `could not open directory` and `Permission denied` while still exiting 0 with
    the subtree omitted — so the match would miss it and the fingerprint would
    compare equal across a real mutation.
    """
    assert gate._c_locale_env()["LC_ALL"] == "C"
    assert gate._c_locale_env()["LC_MESSAGES"] == "C"

    # And the env really reaches the subprocesses whose stderr is interpreted.
    seen = []
    real_run = gate.subprocess.run

    def _spy(argv, **kwargs):
        if argv[:1] == ["git"] and any(
            a in ("status", "diff", "rev-parse") for a in argv
        ):
            seen.append((argv, (kwargs.get("env") or {}).get("LC_ALL")))
        return real_run(argv, **kwargs)

    monkeypatch.setattr(gate.subprocess, "run", _spy)
    repo, _base = _seeded(tmp_path)
    gate._status(str(repo))
    interpreted = [(argv, loc) for argv, loc in seen
                   if "status" in argv or "diff" in argv]
    assert interpreted, "no interpreted git call observed"
    assert all(loc == "C" for _argv, loc in interpreted), interpreted


def test_a_nonzero_git_exit_still_refuses():
    with pytest.raises(gate.GateFailure):
        gate._refuse_unreadable(_Proc(stderr=b"boom", returncode=128), "git diff")


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

    # #171: `ci --base` gets the SAME rule — a preflight validates the committed
    # candidate, never the runner's edits.
    _expect(_gate(repo, "ci", "--base", base), 2, "CHECKOUT_EVENT_MISMATCH")

    # ...and the SIBLING WITNESS that the rule is SCOPED rather than a blanket
    # tightening: `manifests`/`wave --base` keep their documented dirty-tree
    # support, because there the operator chose the baseline and the dirt is the
    # subject. (`manifests` end-to-end plus the shared seam with `ci_mode=False`
    # — running `wave` here would execute the whole suite for no extra evidence.)
    status, stderr = _manifests(repo, base)
    assert status == 0, stderr
    gate.check_checkout_matches_event(
        str(repo),
        {"kind": "local", "sha": base, "target": None,
         "event_head": None, "after": None},
        ci_mode=False,
    )


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


def test_audit_ledger_attestations_have_durable_matching_evidence():
    """Every attested review round has durable evidence a machine can re-verify.

    #152's terminal loop wrote six attestations that the collector never earned —
    free prose, indistinguishable from a real one by reading — and they survived
    six rounds because nothing checked the checker. This is the in-tree half of
    the fix: every run row in every evidence archive must be backed by the
    collector's own artifacts, hash-verified, with the sidecar rules applied PER
    COLLECTOR TYPE (commit-review sidecars must never be assumed for gate runs —
    the two schemas share nothing), every index claim BOUND to the collector's
    sidecar rather than merely shaped like one, and every run the ledger CITES
    present in the archive — a fabricated ledger row must have nowhere to hide.
    The operator-side hook is the live-claim guard; this test keeps the durable
    record honest after the fact.
    """
    import re as _re

    indexes = sorted(_ROOT.glob("docs/architecture/evidence/*/index.jsonl"))
    assert indexes, "no evidence archives found — this check would be vacuous"

    # The exact, reasoned allowlist of rows that are legitimately not `completed`.
    # No wildcard and no generic "legacy" skip: a new non-completed row must be
    # added here with its reason, or it fails.
    expected_not_completed = {
        # failed run, replaced by cdx-review.Kkf8n6 over the same scope
        "commit-reviews/cdx-review.kXfU2v": "failed",
        # refused start — never an evaluation; carries only start.json+refusal.json
        "architect-reviews/cdx-gate-review.TnpZpj": "refused",
    }
    # Collector-specific sidecar allowlists: an archived run dir may hold nothing
    # else — a smuggled extra file is as suspect as a missing one.
    commit_review_names = {
        "baseline", "cwd", "dirty", "scope", "start-head", "last-reviewed-sha",
        "t0", "teardown", "start.json", "review.json", "phase",
    }
    gate_names = {"start.json", "attestation.json", "review.md", "refusal.json"}

    def _sha256(path):
        digest = hashlib.sha256()
        digest.update(path.read_bytes())
        return digest.hexdigest()

    # The verifier must have teeth: a flipped hex digit is a detected mismatch.
    probe = _ROOT / "docs/architecture/evidence/issue-152/index.jsonl"
    good = _sha256(probe)
    flipped = ("0" if good[0] != "0" else "1") + good[1:]
    assert good != flipped and _sha256(probe) != flipped

    _hex40 = _re.compile(r"^[0-9a-f]{40}$")
    _shaish = _re.compile(r"^[0-9a-f]{7,40}$")
    _ancestor_cache = {}

    def _is_ancestor(sha):
        if sha not in _ancestor_cache:
            _ancestor_cache[sha] = subprocess.run(
                ["git", "merge-base", "--is-ancestor", sha, "HEAD"],
                cwd=_ROOT, capture_output=True,
            ).returncode == 0
        return _ancestor_cache[sha]

    def _require_commit(value, where):
        # The strict form: a value that MUST be a full commit — no None, no
        # `auto`. Used for the header's source_tip, which is the archive's
        # provenance anchor and may never be anything weaker.
        assert value and _hex40.match(str(value)), (
            "{0}: required a full 40-character commit, got {1!r}".format(
                where, value
            )
        )
        assert _is_ancestor(value), "{0}: {1} is not an ancestor of HEAD".format(
            where, value
        )

    def _assert_commit(value, where):
        # The per-run form: hex-shaped values are commit assertions; the
        # collector also writes the literal `auto` for auto-scope rounds, which
        # asserts nothing.
        if value is None or value == "auto":
            return
        assert _shaish.match(value), "{0}: not a commit or 'auto': {1!r}".format(
            where, value
        )
        _require_commit(value, where)

    def _regular(path, where):
        # CLAUDE.md's own completion rule: the collector writes REGULAR files. A
        # symlink would let one sidecar impersonate another (last-reviewed-sha ->
        # start-head reads equal by construction).
        assert path.is_file() and not path.is_symlink(), (
            "{0}: {1} must be a regular non-symlink file".format(where, path.name)
        )
        return path.read_text().strip()

    total_152_rows = 0
    seen_durable = set()
    seen_source = set()
    seen_threads = set()
    for index in indexes:
        base = index.parent
        rows = [json.loads(line) for line in index.read_text().splitlines() if line]
        header, runs = rows[0], rows[1:]
        assert header["schema_version"] == 1, header
        _require_commit(header.get("source_tip"), str(index))

        # The owning ledger must point at this archive — an archive nothing
        # references is not part of any audit record.
        ledger_path = base.parent.parent / "ISSUE_{0}_AUDIT_LEDGER.md".format(
            header["issue"]
        )
        assert ledger_path.is_file(), ledger_path
        ledger_text = ledger_path.read_text()
        assert "evidence/issue-{0}".format(header["issue"]) in ledger_text

        # SHA256SUMS covers exactly the on-disk archive (minus itself), and every
        # hash re-verifies. Full verification, not a sample: the archive exists
        # to be checked.
        sums_path = base / "SHA256SUMS"
        sums = {}
        for line in sums_path.read_text().splitlines():
            digest, rel = line.split("  ", 1)
            sums[rel] = digest
        on_disk = {
            str(p.relative_to(base))
            for p in base.rglob("*")
            if p.is_file() and p.name != "SHA256SUMS"
        }

        # ...and the archive must be reproducible from GIT, not merely present in this
        # worktree. The working-tree comparison alone is green for a file that exists
        # locally but is not tracked — which is exactly how `.gitignore`'s `*.log`
        # silently excluded two raw logs that `SHA256SUMS` listed (#171 row AR3-1):
        # local runs passed, and a clean CI checkout would have had fewer files than the
        # checksums claimed. An archive nobody else can reconstruct is not durable
        # evidence, so compare against the index too.
        prefix = str(base.relative_to(_ROOT))
        listed = subprocess.run(
            ["git", "ls-files", "--", prefix],
            cwd=str(_ROOT), capture_output=True, text=True, check=True,
        ).stdout.split()
        tracked = {p[len(prefix) + 1:] for p in listed if p.startswith(prefix + "/")}
        tracked.discard("SHA256SUMS")
        assert set(sums) == tracked, (
            "{0}: SHA256SUMS and the GIT INDEX disagree — listed-but-untracked "
            "{1}, tracked-but-unlisted {2}. A file that exists only in this worktree "
            "is not archived evidence.".format(
                base.name, sorted(set(sums) - tracked), sorted(tracked - set(sums))
            )
        )
        assert set(sums) == on_disk, sorted(set(sums) ^ on_disk)
        for rel, digest in sums.items():
            assert _sha256(base / rel) == digest, "hash mismatch: {0}".format(rel)

        archived_run_names = set()
        for row in runs:
            where = "{0}:{1}".format(index, row.get("durable_dir"))
            durable = row["durable_dir"]
            source = row.get("source_run_dir") or ""
            assert durable not in seen_durable, "duplicate durable dir: " + durable
            seen_durable.add(durable)
            # One collected run is one row: the same SOURCE run must not be
            # counted twice under ANY second identity — not another destination
            # name, and not another collector label either (a collector field is
            # a claim, so it must never widen the dedup key). The name prefix
            # must also agree with the claimed collector, or one run's sidecars
            # could be judged under the other schema.
            source_norm = os.path.normpath(source)
            assert source_norm not in seen_source, (
                where + ": source run indexed twice"
            )
            seen_source.add(source_norm)
            # The authored source path is a CLAIM; the collector-emitted thread
            # id is the identity. A run dir copied under a new name carries its
            # start.json along, so duplicating a round to inflate the count is
            # caught here even when every authored string was renamed.
            run_dir = base / durable
            assert run_dir.is_dir(), where
            start_meta = json.loads((run_dir / "start.json").read_text())
            thread_id = start_meta.get("threadId")
            assert thread_id, where + ": start.json carries no threadId"
            assert thread_id not in seen_threads, (
                where + ": collector thread {0} already indexed — one collected "
                "run is one row".format(thread_id)
            )
            seen_threads.add(thread_id)
            expected_prefix = (
                "cdx-gate-review." if row["collector"] == "gate-attest"
                else "cdx-review."
            )
            assert os.path.basename(source_norm).startswith(expected_prefix), (
                where + ": source run name does not match its claimed collector"
            )
            assert os.path.basename(source_norm) == run_dir.name, (
                where + ": durable dir does not carry its source run's name"
            )
            archived_run_names.add(run_dir.name)

            # The row's file inventory must equal the on-disk run dir EXACTLY —
            # an empty or partial map would make the per-run contract vacuous —
            # and every file must be a collector-legal sidecar for this type.
            actual_files = {
                str(p.relative_to(base))
                for p in run_dir.rglob("*")
                if p.is_file()
            }
            assert set(row["files"]) == actual_files, (
                where + ": row files != archived files: {0}".format(
                    sorted(set(row["files"]) ^ actual_files)
                )
            )
            for rel, digest in row["files"].items():
                assert sums.get(rel) == digest, (
                    "row/manifest hash disagreement for {0}".format(rel)
                )
            _assert_commit(row.get("baseline"), where)
            _assert_commit(row.get("reviewed_sha"), where)

            if row["collector"] == "commit-review-collect":
                # Paths relative to the run dir, never basenames: `extra/scope`
                # must not ride in on the name of a legal root-level sidecar.
                rels = {os.path.relpath(f, durable) for f in row["files"]}
                assert rels <= commit_review_names, (
                    where + ": non-allowlisted sidecar: {0}".format(
                        sorted(rels - commit_review_names)
                    )
                )
                if row["status"] == "completed":
                    start_head = _regular(run_dir / "start-head", where)
                    reviewed = _regular(run_dir / "last-reviewed-sha", where)
                    assert _hex40.match(start_head), where
                    assert start_head == reviewed, (
                        where + ": a completed round reviews exactly its start-head"
                    )
                    assert _is_ancestor(start_head), where
                    teardown = _regular(run_dir / "teardown", where)
                    assert teardown == "confirmed stopped", where
                    # BIND the index row to the collector's sidecars: a row
                    # claiming a newer sha than its own archived run recorded
                    # is exactly the coverage-inflation forgery this exists for.
                    # The binding sidecars are REQUIRED, never optional — a
                    # deleted sidecar must fail, not silently skip the binding.
                    assert row.get("reviewed_sha") == reviewed, (
                        where + ": row reviewed_sha != collector sidecar"
                    )
                    for field in ("baseline", "dirty", "scope"):
                        assert row.get(field) == _regular(run_dir / field, where), (
                            where + ": row {0} != collector sidecar".format(field)
                        )
                else:
                    assert expected_not_completed.get(durable) == row["status"], (
                        where + ": non-completed row absent from the reasoned "
                        "allowlist"
                    )
                    assert (run_dir / "phase").is_file(), where
            elif row["collector"] == "gate-attest":
                # Root-level names from the gate allowlist, or files directly
                # under prompts/ — nothing deeper, nothing else.
                rels = {os.path.relpath(f, durable) for f in row["files"]}
                illegal = {
                    r for r in rels
                    if r not in gate_names
                    and not (r.startswith("prompts" + os.sep)
                             and os.sep not in r[len("prompts") + 1:])
                }
                assert illegal == set(), (
                    where + ": non-allowlisted gate artifact: {0}".format(
                        sorted(illegal)
                    )
                )
                if row["status"] == "completed":
                    att = json.loads((run_dir / "attestation.json").read_text())
                    assert att["teardown"] == "confirmed", where
                    assert att["turn"]["status"] == "completed", where
                    assert att.get("parsedVerdict"), where
                    # BIND attestation to THIS run's identity: its artifact must
                    # BE this run's review.md (normalized exact path — a `..`
                    # traversal that merely starts with the source prefix must
                    # not resolve into another run), its thread must match the
                    # archived start.json with both identifiers PRESENT (a
                    # missing pair comparing None == None establishes nothing),
                    # and the row may not soften the collector's verdict.
                    att_path = os.path.normpath(str(att["artifact"]["path"]))
                    assert att_path == os.path.join(source_norm, "review.md"), (
                        where + ": attested artifact path is not this run's "
                        "review.md: {0}".format(att_path)
                    )
                    start = json.loads((run_dir / "start.json").read_text())
                    att_thread = att.get("start", {}).get("threadId")
                    start_thread = start.get("threadId")
                    assert att_thread and start_thread, (
                        where + ": thread identity missing from attestation or "
                        "start.json"
                    )
                    assert att_thread == start_thread, (
                        where + ": attestation thread != archived start.json"
                    )
                    assert row.get("verdict") == att["parsedVerdict"], (
                        where + ": row verdict != attested parsedVerdict"
                    )
                    assert _sha256(run_dir / "review.md") == att["artifact"]["sha256"], (
                        where + ": review.md does not match its attestation"
                    )
                    prompt_hashes = {
                        _sha256(p) for p in (run_dir / "prompts").iterdir()
                    }
                    assert att["prompt"]["actualSha256"] in prompt_hashes, (
                        where + ": attested prompt hash not among archived prompts"
                    )
                else:
                    assert expected_not_completed.get(durable) == row["status"], (
                        where + ": non-completed row absent from the reasoned "
                        "allowlist"
                    )
                    assert (run_dir / "refusal.json").is_file(), where
            else:
                raise AssertionError(where + ": unknown collector " + row["collector"])

        # Every run the LEDGER cites must exist in the archive. Without this, a
        # fabricated ledger row citing `/tmp/cdx-review.fake` passes untouched
        # because only indexed rows are inspected — the exact hole the #152
        # fabrication drove through.
        cited = set(
            _re.findall(r"cdx(?:-gate)?-review\.[A-Za-z0-9_-]+", ledger_text)
        )
        uncited = {c for c in cited if c not in archived_run_names}
        assert uncited == set(), (
            "{0} cites review runs the archive does not hold: {1}".format(
                ledger_path.name, sorted(uncited)
            )
        )
        # ...and citations must use the FULL run-dir name. A bare suffix like
        # `7tLUAe` is invisible to the pattern above, so mutating it would
        # never be caught — enforce the convention that makes the check total:
        # an archived run's suffix may appear in its ledger only immediately
        # preceded by `review.`.
        for name in archived_run_names:
            prefix = name[: len(name) - len(name.split(".", 1)[1])]
            suffix = name.split(".", 1)[1]
            for m in _re.finditer(_re.escape(suffix), ledger_text):
                before = ledger_text[max(0, m.start() - len(prefix)):m.start()]
                assert before == prefix, (
                    "{0}: run-suffix citation `{1}` at offset {2} is not the "
                    "COMPLETE archived name {3} — a shortened form escapes the "
                    "archive check".format(
                        ledger_path.name, suffix, m.start(), name
                    )
                )

        if header["issue"] == 152:
            total_152_rows = len(runs)

    # A ledger without an archive would never enter the loop above — its
    # citations, in ANY spelling, would go unread. A citation-syntax detector
    # cannot be total (a bare token like `ABC123` is indistinguishable from
    # prose), so the requirement is unconditional: every instantiated ledger
    # owns an evidence archive from instantiation (the template mandates the
    # header-only skeleton in the Stage-1.5 baseline commit), and every claim
    # then binds inside it.
    indexed_issues = {
        json.loads(idx.read_text().splitlines()[0])["issue"] for idx in indexes
    }
    for ledger_path in sorted(
        (_ROOT / "docs" / "architecture").glob("ISSUE_*_AUDIT_LEDGER.md")
    ):
        issue = int(ledger_path.name.split("_")[1])
        assert issue in indexed_issues, (
            "{0} has no evidence archive under docs/architecture/evidence/"
            "issue-{1}/ — instantiate the skeleton (header-only index.jsonl + "
            "SHA256SUMS) with the ledger".format(ledger_path.name, issue)
        )

    # Non-vacuous #152 coverage: the slice's 87 archived rounds plus the three
    # adjustment rounds. A floor, not equality — later record-only corrections
    # may append rounds, and fewer than this means the archive lost rows.
    assert total_152_rows >= 90, total_152_rows
