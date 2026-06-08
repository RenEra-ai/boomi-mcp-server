"""Config regression for the Cloud Run deploy step in cloudbuild.yaml.

The production MCP server must run stateless HTTP transport with buffered JSON
responses (BOOMI_MCP_JSON_RESPONSE=true); without it, large tool results are
SSE-framed and hang clients behind Cloud Run's managed domain mapping. It must
ALSO keep scale-to-zero (no --min-instances / minScale / always-allocated CPU)
for cost. This test locks both invariants into the deploy config.

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
    """No min-instances / minScale / always-allocated CPU — keep scale-to-zero."""
    text = _cloudbuild_text()
    assert "--min-instances" not in text
    assert "minScale" not in text
    assert "--no-cpu-throttling" not in text
    assert "--cpu-always-allocated" not in text
