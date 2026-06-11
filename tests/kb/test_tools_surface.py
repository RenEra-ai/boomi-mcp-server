"""MCP-surface tests: the exported server.search_boomi_docs / read_boomi_doc_page
wrappers and their tool annotations.

Sets KB env vars before importing `server`, mirroring the repo's existing
wrapper tests (which set BOOMI_LOCAL before import).
"""
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

# The heavy KB build is now deferred off the import path (KbWarmup). Force it
# ready before the surface tests so the first call returns a real result rather
# than a bounded warming_up. get() performs the kick() itself, so this works
# regardless of the eager flag (which is only wired through server_http).
_warmed = server._kb_warmup.get(wait_seconds=120)
assert _warmed is not None, "KB warmup did not become ready for the surface tests"

DB_PAGE = "https://help.boomi.com/docs/connectors/database"
BIG_PAGE = "https://help.boomi.com/docs/processes/build-a-process"


# --- search_boomi_docs wrapper -----------------------------------------------

def test_search_wrapper_returns_documented_shape():
    result = server.search_boomi_docs(query="database connector configuration")
    assert result["_success"] is True
    assert result["status"] in {"ok", "low_confidence"}
    assert result["hits"]
    assert {"corpus_built_at", "corpus_version", "embedding_model"} <= set(result)


def test_search_wrapper_empty_query_returns_structured_error():
    result = server.search_boomi_docs(query="")
    assert result["_success"] is False
    assert result["error"] == "empty_query"


def test_search_wrapper_honors_top_k():
    result = server.search_boomi_docs(query="database", top_k=2)
    assert result["top_k"] == 2
    assert len(result["hits"]) <= 2


def test_search_tool_annotations_are_read_only():
    tool = run_async(server.mcp.get_tool("search_boomi_docs"))
    assert tool.annotations.readOnlyHint is True
    assert tool.annotations.openWorldHint is False


# --- read_boomi_doc_page wrapper ---------------------------------------------

def test_read_page_wrapper_returns_documented_shape():
    result = server.read_boomi_doc_page(page_key=DB_PAGE)
    assert result["_success"] is True
    assert result["page_key"] == DB_PAGE
    assert result["chunk_count"] == 5
    assert result["truncated"] is False
    assert {"corpus_built_at", "corpus_version", "embedding_model"} <= set(result)


def test_read_page_wrapper_honors_max_chunks_and_truncates():
    result = server.read_boomi_doc_page(page_key=BIG_PAGE, max_chunks=2)
    assert result["chunks_returned"] == 2
    assert result["truncated"] is True
    assert result["next_chunk_index"] == 2


def test_read_page_wrapper_unknown_page_key_structured_error():
    result = server.read_boomi_doc_page(page_key="https://help.boomi.com/docs/nope")
    assert result["_success"] is False
    assert result["error"] == "no_chunks_for_page_key"


def test_read_page_tool_annotations_are_read_only():
    tool = run_async(server.mcp.get_tool("read_boomi_doc_page"))
    assert tool.annotations.readOnlyHint is True
    assert tool.annotations.openWorldHint is False


# --- invoke_boomi_api KB hint (Issue #79) -------------------------------------

def test_invoke_boomi_api_description_carries_kb_hint():
    # KB is enabled in this module, so @_kb_hint must have appended the generic
    # docs cross-reference to the registered description.
    tool = run_async(server.mcp.get_tool("invoke_boomi_api"))
    assert "use search_boomi_docs before making factual claims" in (tool.description or "")
