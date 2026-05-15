"""Handler-layer tests for KbService.search and its ranking helpers."""
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

from boomi_mcp.kb.service import KbService, _diversify_by_page, _flatten_query_result
from _fixture_corpus import get_kb_service

_SERVICE = get_kb_service()


# --- _diversify_by_page (pure, deterministic) --------------------------------

def _cand(page_key, distance, idx):
    return {"page_key": page_key, "distance": distance, "chunk_id": f"{page_key}-{idx}"}


def test_diversify_spreads_across_pages_when_one_page_dominates():
    # 8 candidates from page A, 2 from page B; A would dominate without spread.
    candidates = [_cand("A", 0.10 + i * 0.05, i) for i in range(8)]
    candidates += [_cand("B", 0.12, 0), _cand("B", 0.30, 1)]
    candidates.sort(key=lambda c: c["distance"])

    hits = _diversify_by_page(candidates, top_k=4)

    assert len(hits) == 4
    # Round-robin one-per-page-per-pass => 2 from each page, not 4 from A.
    assert sum(h["page_key"] == "A" for h in hits) == 2
    assert sum(h["page_key"] == "B" for h in hits) == 2


def test_diversify_falls_back_to_same_page_when_few_pages():
    candidates = [_cand("A", 0.1 + i * 0.1, i) for i in range(5)]
    hits = _diversify_by_page(candidates, top_k=3)
    assert len(hits) == 3
    assert all(h["page_key"] == "A" for h in hits)


def test_diversify_output_is_distance_sorted():
    candidates = [
        _cand("A", 0.50, 0), _cand("A", 0.10, 1),
        _cand("B", 0.20, 0), _cand("B", 0.40, 1),
    ]
    hits = _diversify_by_page(candidates, top_k=4)
    distances = [h["distance"] for h in hits]
    assert distances == sorted(distances)


def test_diversify_empty_input():
    assert _diversify_by_page([], top_k=5) == []


# --- _flatten_query_result (pure) --------------------------------------------

def test_flatten_query_result_maps_metadata_fields():
    raw = {
        "ids": [["a_000", "b_000"]],
        "documents": [["body a", "body b"]],
        "distances": [[0.11, 0.42]],
        "metadatas": [[
            {"title": "A", "section_heading": "S1", "breadcrumb": "X > A",
             "page_key": "pk-a", "source_url": "u-a", "category": "Integration",
             "chunk_index": 0, "token_estimate": 10},
            {"title": "B", "section_heading": "S2", "breadcrumb": "X > B",
             "page_key": "pk-b", "source_url": "u-b", "category": "EDI",
             "chunk_index": 3, "token_estimate": 20},
        ]],
    }
    hits = _flatten_query_result(raw)
    assert [h["chunk_id"] for h in hits] == ["a_000", "b_000"]
    assert hits[0]["content"] == "body a"
    assert hits[0]["distance"] == 0.11
    assert hits[0]["page_key"] == "pk-a"
    assert hits[1]["category"] == "EDI"
    assert hits[1]["chunk_index"] == 3


# --- KbService.search against the real fixture corpus ------------------------

HIT_FIELDS = {
    "chunk_id", "title", "section_heading", "breadcrumb", "page_key",
    "source_url", "category", "chunk_index", "token_estimate", "distance",
    "content",
}
PROVENANCE_FIELDS = {"corpus_built_at", "corpus_version", "embedding_model"}


def test_search_returns_documented_shape_and_provenance():
    result = _SERVICE.search("database connector configuration")
    assert result["_success"] is True
    assert result["status"] in {"ok", "low_confidence"}
    assert result["query"] == "database connector configuration"
    assert result["hits"], "expected at least one hit from the fixture corpus"
    assert PROVENANCE_FIELDS <= set(result)
    assert result["corpus_version"] == "kb-test"
    for hit in result["hits"]:
        assert HIT_FIELDS <= set(hit)
        assert hit["content"]


def test_search_good_query_is_ok_status():
    result = _SERVICE.search("how do I configure a database connection")
    assert result["status"] == "ok"
    assert result["best_distance"] <= result["low_confidence_distance"]


def test_search_unrelated_query_is_low_confidence():
    result = _SERVICE.search("photosynthesis in tropical rainforest ecosystems")
    assert result["status"] == "low_confidence"
    assert result["best_distance"] > result["low_confidence_distance"]


def test_search_ok_status_has_no_warning():
    result = _SERVICE.search("how do I configure a database connection")
    assert result["status"] == "ok"
    assert result["warning"] is None


def test_search_low_confidence_includes_warning():
    result = _SERVICE.search("photosynthesis in tropical rainforest ecosystems")
    assert result["status"] == "low_confidence"
    assert isinstance(result["warning"], str) and result["warning"]
    assert "do not" in result["warning"].lower()


def test_search_empty_query_returns_structured_error():
    for bad in ("", "   ", None):
        result = _SERVICE.search(bad)
        assert result["_success"] is False
        assert result["error"] == "empty_query"


def test_search_default_top_k_is_five():
    result = _SERVICE.search("database")
    assert result["top_k"] == 5
    assert len(result["hits"]) <= 5


def test_search_top_k_clamped_to_max():
    result = _SERVICE.search("database", top_k=999)
    assert result["top_k"] == 10  # BOOMI_DOCS_TOP_K_MAX default
    assert len(result["hits"]) <= 10


def test_search_top_k_clamped_to_minimum():
    result = _SERVICE.search("database", top_k=0)
    assert result["top_k"] == 1
    assert len(result["hits"]) == 1


def test_search_respects_explicit_top_k():
    result = _SERVICE.search("database", top_k=2)
    assert result["top_k"] == 2
    assert len(result["hits"]) <= 2


def test_search_diversifies_across_pages():
    # Query targets the 5-chunk EDI page; with top_k=5 the over-fetch (15) must
    # reach past that page into others, so per-page diversification surfaces more
    # than one page in the top results even though one page is most relevant.
    result = _SERVICE.search("EDI trading partner X12 communication channels", top_k=5)
    distinct_pages = {h["page_key"] for h in result["hits"]}
    assert len(distinct_pages) >= 2


# --- no_match needs an empty collection (Chroma always returns hits otherwise) -

class _EmptyCollection:
    def query(self, query_texts, n_results):
        return {"ids": [[]], "documents": [[]], "metadatas": [[]], "distances": [[]]}


def test_search_no_match_when_collection_empty():
    svc = KbService(
        _EmptyCollection(),
        {"build_timestamp": "2026-05-01T00:00:00Z", "artifact_tag": "kb-x",
         "embedding_model": "all-MiniLM-L6-v2"},
        {"top_k_default": 5, "top_k_max": 10, "low_confidence_distance": 0.45},
    )
    result = svc.search("anything at all")
    assert result["_success"] is True
    assert result["status"] == "no_match"
    assert result["hits"] == []
    assert result["best_distance"] is None
    assert isinstance(result["warning"], str) and "knowledge base" in result["warning"]
