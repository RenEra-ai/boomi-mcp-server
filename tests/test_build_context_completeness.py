"""Build-context completeness guard.

The analyze_component bug shipped because a tracked source file was silently dropped
from ``gcloud builds submit`` uploads: an unanchored ``analyze_*.py`` rule in .gitignore,
inherited by gcloud's auto-generated .gcloudignore, excluded the tracked module from the
uploaded context so the image was built without it.

The Dockerfile build-time import gate catches a dropped *imported module*, but NOT a
dropped tracked **runtime asset** (e.g. ``templates/login.html``, ``static/favicon.png``,
``deploy/kb-release.env``) — those are invisible to a `python -c "import ..."` check and a
silent drop would only surface as a runtime 500. This test closes that gap for the whole
class: it asserts that every tracked file is present in the gcloud build-context upload,
except an explicit allowlist.

Skipped when ``gcloud`` is unavailable (e.g. bare CI) so it only enforces where it can run.
"""

import shutil
import subprocess
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent

# Tracked files intentionally excluded from the Cloud Build upload context.
# .gcloudignore self-excludes (standard gcloud behavior) and is not needed in the build.
_UPLOAD_ALLOWLIST = {".gcloudignore"}

# Tracked-directory prefixes intentionally excluded from the Cloud Build upload.
# ``docs/plans/`` is gitignored by design (local planning artifacts), but the
# /auto-issue workflow force-adds the architect plan JSON there as a durable
# source-of-truth. Those planning docs are NOT runtime assets and legitimately do
# not belong in the Cloud Run image — so a drop here is expected, not the
# dropped-source-file regression this guard exists to catch.
_UPLOAD_ALLOWLIST_PREFIXES = ("docs/plans/",)


def _lines(args):
    out = subprocess.run(
        args, cwd=_ROOT, capture_output=True, text=True, check=True
    ).stdout
    return {line for line in out.splitlines() if line}


@pytest.mark.skipif(shutil.which("gcloud") is None, reason="gcloud not on PATH")
def test_no_tracked_file_dropped_from_build_context():
    """Every tracked file must reach the gcloud build-context upload (minus the allowlist).

    Guards against a future .gitignore/.gcloudignore pattern silently excluding tracked
    source OR runtime assets from ``gcloud builds submit`` — the analyze_component.py
    failure mode — for files the module-only Docker import gate cannot see.
    """
    try:
        tracked = _lines(["git", "ls-files"])
        uploaded = _lines(["gcloud", "meta", "list-files-for-upload"])
    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        pytest.skip(f"could not compute build-context file lists: {exc}")

    # Skip ONLY when gcloud genuinely produced nothing here (e.g. an unconfigured gcloud /
    # sandbox emitting empty output at exit 0). Deliberately do NOT try to heuristically
    # judge a NON-empty listing as "unreliable": every such heuristic (sentinel presence,
    # overlap count, untracked-ratio) can be fooled into SKIPPING a real regression, which
    # is the dangerous direction for a guard. A non-empty listing is asserted as-is — a
    # malformed listing would surface as a loud, investigable failure rather than silently
    # masking a dropped tracked file. Untracked extras in `uploaded` are harmless here:
    # they simply don't appear in `tracked - uploaded`.
    if not uploaded:
        pytest.skip("gcloud meta list-files-for-upload produced no output in this environment")

    # Flag a tracked file only if it EXISTS on disk yet is absent from the upload — that
    # is the real ignore-rule exclusion (ignore rules drop files from the upload, they do
    # not delete them, so the offending file is always present on disk). A tracked file
    # missing from disk is a partial/odd checkout, not a build-context ignore bug, and is
    # out of scope for this guard.
    dropped = sorted(
        f
        for f in (tracked - uploaded - _UPLOAD_ALLOWLIST)
        if (_ROOT / f).exists()
        and not f.startswith(_UPLOAD_ALLOWLIST_PREFIXES)
    )
    assert not dropped, (
        "tracked files are excluded from the gcloud build-context upload — a "
        ".gitignore/.gcloudignore pattern is dropping them, so they would be MISSING "
        f"from locally-submitted (`gcloud builds submit`) images: {dropped}"
    )
