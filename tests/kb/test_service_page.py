"""Handler-layer tests for KbService.read_page."""
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

from boomi_mcp.kb.service import KbService
from _fixture_corpus import get_kb_service

_SERVICE = get_kb_service()

BIG_PAGE = "https://help.boomi.com/docs/processes/build-a-process"   # 30 chunks
DB_PAGE = "https://help.boomi.com/docs/connectors/database"          # 5 chunks
PROVENANCE_FIELDS = {"corpus_built_at", "corpus_version", "embedding_model"}


def test_read_page_returns_full_small_page_in_order():
    result = _SERVICE.read_page(DB_PAGE)
    assert result["_success"] is True
    assert result["page_key"] == DB_PAGE
    assert result["chunk_count"] == 5
    assert result["chunks_returned"] == 5
    assert result["truncated"] is False
    assert "next_chunk_index" not in result
    indices = [c["chunk_index"] for c in result["chunks"]]
    assert indices == [0, 1, 2, 3, 4]
    assert PROVENANCE_FIELDS <= set(result)
    assert result["title"] == "Database Connector"


def test_read_page_truncates_large_page():
    result = _SERVICE.read_page(BIG_PAGE, max_chunks=10)
    assert result["chunk_count"] == 30
    assert result["chunks_returned"] == 10
    assert result["truncated"] is True
    assert result["next_chunk_index"] == 10
    assert [c["chunk_index"] for c in result["chunks"]] == list(range(10))


def test_read_page_pagination_with_start_chunk_index():
    result = _SERVICE.read_page(BIG_PAGE, max_chunks=10, start_chunk_index=10)
    assert result["start_chunk_index"] == 10
    assert [c["chunk_index"] for c in result["chunks"]] == list(range(10, 20))
    assert result["truncated"] is True
    assert result["next_chunk_index"] == 20


def test_read_page_final_slice_not_truncated():
    result = _SERVICE.read_page(BIG_PAGE, max_chunks=15, start_chunk_index=15)
    assert [c["chunk_index"] for c in result["chunks"]] == list(range(15, 30))
    assert result["truncated"] is False
    assert "next_chunk_index" not in result


def test_read_page_start_index_past_end_returns_empty():
    result = _SERVICE.read_page(BIG_PAGE, start_chunk_index=100)
    assert result["_success"] is True
    assert result["chunks_returned"] == 0
    assert result["chunk_count"] == 30
    assert result["truncated"] is False


def test_read_page_max_chunks_clamped_to_ceiling():
    result = _SERVICE.read_page(BIG_PAGE, max_chunks=999)
    assert result["chunks_returned"] == 30  # clamped to ceiling of 30
    assert result["truncated"] is False


def test_read_page_max_chunks_clamped_to_minimum():
    result = _SERVICE.read_page(BIG_PAGE, max_chunks=0)
    assert result["chunks_returned"] == 1


def test_read_page_negative_start_index_clamped_to_zero():
    result = _SERVICE.read_page(DB_PAGE, start_chunk_index=-5)
    assert result["start_chunk_index"] == 0
    assert result["chunks_returned"] == 5


def test_read_page_unknown_page_key_structured_error():
    result = _SERVICE.read_page("https://help.boomi.com/docs/does-not-exist")
    assert result["_success"] is False
    assert result["error"] == "no_chunks_for_page_key"
    assert result["page_key"] == "https://help.boomi.com/docs/does-not-exist"


def test_read_page_empty_page_key_structured_error():
    for bad in ("", "   ", None):
        result = _SERVICE.read_page(bad)
        assert result["_success"] is False
        assert result["error"] == "empty_page_key"


# --- non-contiguous chunk_index: log a warning, do not raise -----------------

class _GappyCollection:
    """Collection stub whose page has a gap in chunk_index (0,1,2,5,6)."""

    def get(self, **kwargs):
        indices = [0, 1, 2, 5, 6]
        return {
            "ids": [f"c{i}" for i in indices],
            "documents": [f"body {i}" for i in indices],
            "metadatas": [
                {"chunk_index": i, "section_heading": f"S{i}", "source_url": "u",
                 "title": "T", "breadcrumb": "b", "category": "Integration"}
                for i in indices
            ],
        }


def test_read_page_non_contiguous_indices_does_not_raise(capsys):
    svc = KbService(
        _GappyCollection(),
        {"build_timestamp": "2026-05-01T00:00:00Z", "artifact_tag": "kb-x",
         "embedding_model": "all-MiniLM-L6-v2"},
        {"top_k_default": 5, "top_k_max": 10, "low_confidence_distance": 0.45},
    )
    result = svc.read_page("some-page-key")
    assert result["_success"] is True
    assert result["chunk_count"] == 5
    assert [c["chunk_index"] for c in result["chunks"]] == [0, 1, 2, 5, 6]
    assert "non-contiguous chunk_index" in capsys.readouterr().out
