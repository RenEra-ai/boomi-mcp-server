"""MCP-surface tests: the kb://boomi-docs/corpus resource."""
import os
import sys
from pathlib import Path

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = str(Path(_HERE).parents[1])
_SRC = os.path.join(_ROOT, "src")
for _p in (_HERE, _SRC, _ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import pytest

pytest.importorskip("chromadb")
pytest.importorskip("sentence_transformers")

from _fixture_corpus import get_fixture_corpus, run_async

os.environ["BOOMI_LOCAL"] = "true"
os.environ["BOOMI_DOCS_ENABLED"] = "true"
os.environ["BOOMI_DOCS_DB_PATH"] = get_fixture_corpus()
os.environ["BOOMI_DOCS_COLLECTION"] = "boomi_docs"

import server  # noqa: E402
from boomi_mcp.kb import warmup as warmup_mod  # noqa: E402

CORPUS_URI = "kb://boomi-docs/corpus"


def _read_resource_text(uri):
    result = run_async(server.mcp.read_resource(uri))
    # FastMCP returns a ResourceResult with one or more content parts.
    return "".join(part.content for part in result.contents)


def test_resources_list_has_exactly_one_entry():
    resources = run_async(server.mcp.list_resources())
    assert len(resources) == 1
    assert str(resources[0].uri) == CORPUS_URI


def test_no_resource_templates_registered():
    templates = run_async(server.mcp.list_resource_templates())
    assert templates == []


def test_corpus_resource_reads_as_coverage_map():
    body = _read_resource_text(CORPUS_URI)
    assert body.startswith("# Boomi Documentation Corpus")
    assert "Collection: boomi_docs" in body
    assert "Embedding model: all-MiniLM-L6-v2" in body
    assert "Corpus version: kb-test" in body
    # Totals include the supplemental companion page (4 chunks / 1 page).
    assert "44 chunks across 4 pages" in body
    assert "Integration (35)" in body
    assert "EDI (5)" in body
    assert "Companion Reference (4)" in body
    assert "Known exclusions:" in body
    assert "search_boomi_docs" in body


def test_corpus_resource_shows_provenance_breakdown():
    body = _read_resource_text(CORPUS_URI)
    # Provenance line rendered from source_type_counts, sorted desc by count.
    assert "Provenance: official (40), companion_reference (4)" in body


def test_corpus_resource_shows_companion_source_and_caveat():
    body = _read_resource_text(CORPUS_URI)
    # Supplemental source names the repo + short commit.
    assert "OfficialBoomi/boomi-integration @ 19aacdd" in body
    # Unverified / not-authoritative caveat is present.
    assert "not officially supported" in body
    assert "companion_reference results as unverified, not authoritative" in body


def test_resource_is_independent_of_warmup_state():
    """The resource renders from the cheap bootstrap manifest, not a live
    KbService, so it must return the same coverage map in EVERY warmup state —
    not-yet-built, warming, ready, and failed. State is saved/restored so this
    does not pollute the warmup shared with other modules in the same process."""
    expected = _read_resource_text(CORPUS_URI)
    assert expected.startswith("# Boomi Documentation Corpus")

    w = server._kb_warmup
    with w._lock:
        saved = (w._state, w._error_type, w._service)
    try:
        for state in (warmup_mod.IDLE, warmup_mod.WARMING,
                      warmup_mod.READY, warmup_mod.FAILED):
            with w._lock:
                w._state = state
                w._error_type = "KbStartupError" if state == warmup_mod.FAILED else None
            assert _read_resource_text(CORPUS_URI) == expected, state
    finally:
        with w._lock:
            w._state, w._error_type, w._service = saved
