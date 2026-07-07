"""Contract test: the checked-in KB fixture must satisfy the producer's
chunk-validation contract (knowledge-base-builder).

Closes the fixture-vs-builder drift gap. The fixture (tests/fixtures/kb/
corpus.jsonl) is a hand-authored mirror of what the builder emits; without this
guard, the builder could tighten its contract while the KB suite stayed green
against a stale fixture and a freshly built production corpus carried an
untested shape.

Two layers:
  * Always-on (no KB deps, runs everywhere): validate the fixture against the
    vendored contract copy in _producer_contract.py.
  * Parity canary (skips unless the builder source is present — dev / QA loop):
    assert the vendored copy AND the fixture still match the LIVE builder, so a
    builder-side contract change fails here instead of shipping silently.

Deliberately NO ``pytest.importorskip`` — the always-on layer must run even when
chromadb / sentence-transformers are absent. _fixture_corpus.load_fixture_chunks
and _producer_contract are both import-safe without the KB deps.
"""
import copy
import os
import sys
from contextlib import contextmanager
from pathlib import Path

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = str(Path(_HERE).parents[1])
_SRC = os.path.join(_ROOT, "src")
for _p in (_HERE, _SRC, _ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from _fixture_corpus import load_fixture_chunks
from _producer_contract import (
    COMPANION_SOURCE_TYPE,
    OFFICIAL_SOURCE_TYPE,
    PROVENANCE_REAL_VALUE_FIELDS,
    REQUIRED_CHUNK_FIELDS,
    find_builder_dir,
    validate_chunks,
)


# --- always-on: fixture satisfies the vendored producer contract -------------

def test_fixture_chunks_satisfy_producer_contract():
    chunks = load_fixture_chunks()
    assert len(chunks) == 44
    assert len({c["id"] for c in chunks}) == len(chunks), "duplicate chunk id in fixture"
    errors = validate_chunks(chunks)
    assert errors == [], "fixture violates producer chunk contract:\n" + "\n".join(errors)


def test_guard_catches_corruption():
    """Positive control: prove the vendored validator actually bites."""
    base = load_fixture_chunks()

    official = copy.deepcopy(base)
    official[0]["latest_url"] = "x"  # official row must leave extended fields blank
    assert validate_chunks(official), "guard missed a non-blank official field"

    companion = copy.deepcopy(base)
    idx = next(i for i, c in enumerate(base) if c["source_type"] == COMPANION_SOURCE_TYPE)
    companion[idx]["upstream_repo"] = ""  # companion row must carry attribution
    assert validate_chunks(companion), "guard missed an empty companion field"

    noncontiguous = copy.deepcopy(base)
    noncontiguous[0]["chunk_index"] = 99  # breaks per-page_key contiguity
    assert validate_chunks(noncontiguous), "guard missed non-contiguous chunk_index"


# --- parity canary: vendored copy + fixture still match the LIVE builder ------

@contextmanager
def _live_builder():
    """Import the sibling builder's build_index, or skip if it is not present.

    Inserts the builder dir on sys.path for the duration and cleans up both the
    path entry and the imported builder modules on exit so the rest of the suite
    is unaffected.
    """
    builder = find_builder_dir()
    if builder is None:
        pytest.skip("knowledge-base-builder source not present (set KB_BUILDER_PATH); dev/QA-only parity")
    inserted = builder not in sys.path
    if inserted:
        sys.path.insert(0, builder)
    try:
        import build_index  # noqa: E402 — deferred; builder is optional
        yield build_index
    finally:
        if inserted and builder in sys.path:
            sys.path.remove(builder)
        sys.modules.pop("build_index", None)
        sys.modules.pop("companion", None)


def test_vendored_contract_matches_builder():
    with _live_builder() as build_index:
        assert set(REQUIRED_CHUNK_FIELDS) == set(build_index.REQUIRED_CHUNK_FIELDS)
        assert PROVENANCE_REAL_VALUE_FIELDS == build_index.PROVENANCE_REAL_VALUE_FIELDS
        assert COMPANION_SOURCE_TYPE == build_index.COMPANION_SOURCE_TYPE
        assert OFFICIAL_SOURCE_TYPE == build_index.OFFICIAL_SOURCE_TYPE


def test_fixture_passes_real_builder_validate_chunks():
    with _live_builder() as build_index:
        errors = build_index.validate_chunks(load_fixture_chunks())
        assert errors == [], "fixture fails builder's live validate_chunks:\n" + "\n".join(errors)
