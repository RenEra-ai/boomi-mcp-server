"""The derived report: tracked, in sync with the registry, and deterministic."""

from __future__ import annotations

import subprocess
from pathlib import Path

from boomi_mcp.connector_replay.registry import ReplayRegistry
from boomi_mcp.connector_replay.report import (
    REPORT_RELATIVE_PATH,
    render,
)

_REPO = Path(__file__).resolve().parents[1]
_REPORT = _REPO / REPORT_RELATIVE_PATH


def test_the_report_is_tracked_not_merely_present():
    """A working-tree-only report lets the registry and its summary drift unseen.

    `docs/*` is ignored wholesale in this repository, so this file exists only
    because of an explicit carve-out. Checking the file is present would pass even
    if the carve-out were removed; checking git tracks it is the real question.
    """
    assert _REPORT.is_file(), "the report has not been generated"
    out = subprocess.run(
        ["git", "check-ignore", str(_REPORT.relative_to(_REPO))],
        cwd=_REPO, capture_output=True, text=True,
    )
    assert out.returncode != 0, (
        "the report is git-ignored ({0}); the carve-out in .gitignore is missing, so "
        "the registry's published summary would never be reviewable".format(out.stdout.strip())
    )
    listed = subprocess.run(
        ["git", "ls-files", "--error-unmatch", str(_REPORT.relative_to(_REPO))],
        cwd=_REPO, capture_output=True, text=True,
    )
    assert listed.returncode == 0, "the report is not tracked by git"


def test_the_tracked_report_matches_what_the_registry_renders():
    """Drift between the registry and its published summary is the failure here."""
    assert _REPORT.read_text() == render(), (
        "the tracked report is stale. Regenerate it — the report is generated, and a "
        "hand-edit to it is a claim the registry does not make."
    )


def test_rendering_is_deterministic():
    assert render() == render()


def test_an_empty_registry_says_so_in_words():
    """An empty table with no explanation reads like a rendering bug."""
    text = render(ReplayRegistry((), ()))
    assert "No actions have been verified" in text
    assert "refuses a retry" in text
    assert "No connector types are mapped" in text


def test_the_report_states_the_fail_closed_consequence():
    text = render()
    assert "unverified" in text and "refuses a retry" in text


def test_no_credential_material_in_the_report():
    text = _REPORT.read_text().lower()
    for needle in ("password", "secret", "token", "credential://", "api_key", "@"):
        assert needle not in text, "the published report contains {0!r}".format(needle)
