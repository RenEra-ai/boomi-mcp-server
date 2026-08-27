"""The advisory image-parity workflow's shape, which is load-bearing.

The workflow proves a property the unit suite structurally cannot: that the packaged
registry is readable INSIDE a built image. On a developer machine and in the test
runner the file is on disk, so `importlib.resources` finds it whether or not the
packaging would ship it. This repository has no wheel build — it packages by copying
the tree into an image — so a `.dockerignore` pattern IS the packaging, and one added
later would break the registry in production with every unit test still green.

These tests pin the workflow's shape rather than its behaviour, because its behaviour
only exists on a runner with docker.
"""

from __future__ import annotations

from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")

_REPO = Path(__file__).resolve().parents[1]
_WORKFLOW = _REPO / ".github" / "workflows" / "connector-replay-image-parity.yml"
_TESTS_WORKFLOW = _REPO / ".github" / "workflows" / "tests.yml"


def _loaded() -> dict:
    return yaml.safe_load(_WORKFLOW.read_text())


def _job() -> dict:
    jobs = _loaded()["jobs"]
    assert len(jobs) == 1, "the workflow should carry exactly one job"
    return next(iter(jobs.values()))


def test_the_workflow_exists_and_parses():
    assert _WORKFLOW.is_file()
    assert _loaded()["name"]


def test_it_carries_no_if_condition_anywhere():
    """A job-level `if:` would report SKIPPED, and skipped reads as success.

    That is the trap this shape exists to avoid: an environment change would make
    the check pass forever without ever running. The docker-availability decision
    is made in the SHELL, where the step runs, says why it stopped, and leaves that
    sentence in the log.
    """
    job = _job()
    assert "if" not in job, "job-level `if:` — a skipped job reports as success"
    for step in job["steps"]:
        assert "if" not in step, "step-level `if:` — same failure, one level down"


def test_the_docker_skip_is_made_in_the_shell():
    body = _WORKFLOW.read_text()
    assert "command -v docker" in body
    assert "docker unavailable" in body, (
        "the skip must SAY it skipped; a silent exit 0 is indistinguishable from a pass"
    )


def test_it_is_a_separate_workflow_and_does_not_touch_the_gate():
    """`tests.yml` is the required status and its own pins forbid softeners.

    A best-effort docker step does not belong inside a gate that must fail closed.
    """
    assert _TESTS_WORKFLOW.is_file()
    gate = _TESTS_WORKFLOW.read_text()
    assert "image-parity" not in gate
    assert "connector-replay-image-parity" not in gate
    assert "docker build" not in gate


def test_it_has_its_own_timeout():
    """A docker build must not inherit a budget tuned for the unit suite."""
    assert _job()["timeout-minutes"] == 60


def test_it_runs_on_the_same_refs_the_gate_does():
    triggers = _loaded()[True]["push"]["branches"]
    assert set(triggers) == {"dev", "scratch/**"}


def test_the_check_actually_fails_on_a_bad_registry():
    """The in-image script must EXIT NONZERO on each failure it names.

    A parity check that printed a warning and exited 0 would be decoration. Each
    branch is asserted to carry an explicit non-zero exit.
    """
    body = _WORKFLOW.read_text()
    assert body.count("sys.exit(1)") >= 2, (
        "the in-image script must fail, not warn, when the registry is unusable"
    )
    assert "load_registry()" in body, "it must exercise the real loader, not just json.loads"
    assert "importlib" in body or "resources.files" in body
