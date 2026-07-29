"""Config regression for the Cloud Run deploy step in cloudbuild.yaml.

The production MCP server must run stateless HTTP transport with buffered JSON
responses (BOOMI_MCP_JSON_RESPONSE=true); without it, large tool results are
SSE-framed and hang clients behind Cloud Run's managed domain mapping. It must
also keep scale-to-zero (no --min-instances / minScale) while leaving CPU
available between requests so deferred docs KB warmup can complete.

Parsed as plain text (not YAML): PyYAML is not a project dependency, and the
--update-env-vars value is a single flat comma-separated string.
"""
from __future__ import annotations

from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_CLOUDBUILD = _REPO_ROOT / "cloudbuild.yaml"


def _cloudbuild_text() -> str:
    return _CLOUDBUILD.read_text(encoding="utf-8")


def _update_env_vars_segment(text: str) -> str:
    """The single --update-env-vars value (the quoted string on one line)."""
    marker = "--update-env-vars="
    assert marker in text, "cloudbuild.yaml has no --update-env-vars flag"
    return text.split(marker, 1)[1].split("\n", 1)[0]


def test_cloudbuild_sets_stateless_and_json_response():
    text = _cloudbuild_text()
    assert "BOOMI_MCP_STATELESS_HTTP=true" in text
    assert "BOOMI_MCP_JSON_RESPONSE=true" in text


def test_cloudbuild_json_response_inside_update_env_vars():
    """Both flags must live in the --update-env-vars string, not stray elsewhere."""
    segment = _update_env_vars_segment(_cloudbuild_text())
    assert "BOOMI_MCP_STATELESS_HTTP=true" in segment
    assert "BOOMI_MCP_JSON_RESPONSE=true" in segment


def test_cloudbuild_keeps_scale_to_zero():
    """No min-instances / minScale — keep scale-to-zero."""
    text = _cloudbuild_text()
    assert "--min-instances" not in text
    assert "minScale" not in text
    assert "--cpu-always-allocated" not in text


def test_cloudbuild_keeps_cpu_available_for_docs_warmup():
    """Deferred KB warmup runs in a background thread after first MCP request."""
    text = _cloudbuild_text()
    segment = _update_env_vars_segment(text)
    assert "--no-cpu-throttling" in text
    assert "BOOMI_DOCS_WARMUP_EAGER=true" in segment


def test_cloudbuild_pins_all_four_warmup_env_values():
    """Production pins the full warmup contract: eager on, a 65s bounded wait
    (just above the measured build p95/max), a 60s expected-duration hint, and
    a 4-slot long-waiter admission cap. Must match the code defaults."""
    segment = _update_env_vars_segment(_cloudbuild_text())
    assert "BOOMI_DOCS_WARMUP_EAGER=true" in segment
    assert "BOOMI_DOCS_WARMUP_WAIT_SECONDS=65" in segment
    assert "BOOMI_DOCS_WARMUP_EXPECTED_SECONDS=60" in segment
    assert "BOOMI_DOCS_WARMUP_MAX_WAITERS=4" in segment


# ---------------------------------------------------------------------------
# Issue #145 (M12.10) — trusted build provenance for the recipe registry
# ---------------------------------------------------------------------------
#
# HONEST SCOPE: these are text assertions. CI does not build the image, so they
# prove the wiring is DECLARED, not that a built image contains the file. The
# runtime half is covered by tests/test_recipe_registry.py, which points
# ``BUILD_REVISION_PATH`` at a temp file and exercises both the valid and the
# malformed read. Stated here rather than implied, because "the Dockerfile
# mentions it" is a weaker guarantee than a reader might assume.

_DOCKERFILE = _REPO_ROOT / "Dockerfile"


def _dockerfile_text() -> str:
    return _DOCKERFILE.read_text(encoding="utf-8")


def test_cloudbuild_passes_the_commit_sha_as_the_build_revision():
    text = _cloudbuild_text()
    assert '--build-arg "BOOMI_BUILD_REVISION=$COMMIT_SHA"' in text


def test_the_dockerfile_declares_and_validates_the_build_revision_arg():
    text = _dockerfile_text()
    assert 'ARG BOOMI_BUILD_REVISION=""' in text
    # Validated on the way IN. A malformed value must fail the BUILD rather than
    # become provenance a skew comparison would then treat as authoritative.
    assert "[0-9a-f]{7,40}" in text
    assert "/app/BUILD_REVISION" in text


def test_the_build_revision_file_is_written_read_only():
    assert "chmod 0444 /app/BUILD_REVISION" in _dockerfile_text()


def test_an_absent_build_revision_is_a_no_op_not_a_build_failure():
    """A local/manual ``docker build`` with no revision must still succeed.

    ``build_info`` then falls back to its source digest, which is the honest
    answer for a build that carries no commit id.
    """
    text = _dockerfile_text()
    assert 'if [ -n "$BOOMI_BUILD_REVISION" ]; then' in text


def test_the_build_revision_path_matches_the_runtime_constant():
    """The Dockerfile and ``build_info`` must agree on ONE path.

    Two independently-maintained copies of a filename is exactly how a
    provenance file gets written where nothing reads it.
    """
    import sys

    src = str(_REPO_ROOT / "src")
    if src not in sys.path:
        sys.path.insert(0, src)
    from boomi_mcp.build_info import BUILD_REVISION_PATH

    assert BUILD_REVISION_PATH in _dockerfile_text()
