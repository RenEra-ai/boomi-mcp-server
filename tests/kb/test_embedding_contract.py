"""Unit tests for the fail-closed embedding-contract resolver.

Pure stdlib — no ML imports, no importorskip. The resolver is the single
source of embedding-model identity for cheap startup validation, the deferred
heavy build, the Docker preload, and the corpus resource.
"""
import os
import sys
from pathlib import Path

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = str(Path(_HERE).parents[1])
_SRC = os.path.join(_ROOT, "src")
for _p in (_HERE, _SRC, _ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from boomi_mcp.kb.embedding_contract import (
    KB24_COMPATIBLE_REVISION,
    PINNED_MODEL_ID,
    EmbeddingContract,
    resolve_embedding_contract,
)
from boomi_mcp.kb.errors import KbStartupError


def _v2_contract(**overrides):
    contract = {
        "version": 1,
        "model_id": "all-MiniLM-L6-v2",
        "revision": "1110a243fdf4706b3f48f1d95db1a4f5529b4d41",
        "max_seq_length": 256,
        "distance_metric": "cosine",
        "normalize_embeddings": False,
        "embedding_text_version": "s5-s6-v1",
        "s7_enabled": False,
    }
    contract.update(overrides)
    return contract


def _v2_manifest(**contract_overrides):
    return {
        "schema_version": "2",
        "collection_name": "boomi_docs",
        "embedding_model": "all-MiniLM-L6-v2",
        "embedding_contract": _v2_contract(**contract_overrides),
    }


# --- legacy kb-24 mapping ------------------------------------------------------

def test_legacy_kb24_manifest_resolves_to_pinned_compatible_contract():
    manifest = {"schema_version": "1", "embedding_model": "all-MiniLM-L6-v2"}
    contract = resolve_embedding_contract(manifest)
    assert contract == EmbeddingContract(
        model_id=PINNED_MODEL_ID,
        revision=KB24_COMPATIBLE_REVISION,
        max_seq_length=256,
        distance_metric="cosine",
        normalize_embeddings=False,
        embedding_text_version="raw-v1",
        s7_enabled=False,
        source="legacy-kb24",
    )
    assert contract.revision == "1110a243fdf4706b3f48f1d95db1a4f5529b4d41"


def test_legacy_manifest_with_unknown_model_is_rejected():
    manifest = {"schema_version": "1", "embedding_model": "bge-small-en-v1.5"}
    with pytest.raises(KbStartupError, match="legacy"):
        resolve_embedding_contract(manifest)


def test_legacy_manifest_without_model_is_rejected():
    with pytest.raises(KbStartupError, match="legacy"):
        resolve_embedding_contract({"schema_version": "1"})


# --- valid schema-v2 contract --------------------------------------------------

def test_valid_v2_contract_resolves_field_for_field():
    contract = resolve_embedding_contract(_v2_manifest())
    assert contract == EmbeddingContract(
        model_id="all-MiniLM-L6-v2",
        revision="1110a243fdf4706b3f48f1d95db1a4f5529b4d41",
        max_seq_length=256,
        distance_metric="cosine",
        normalize_embeddings=False,
        embedding_text_version="s5-s6-v1",
        s7_enabled=False,
        source="contract",
    )


def test_valid_b567_contract_resolves_s7_enabled():
    manifest = _v2_manifest(
        embedding_text_version="s5-s6-s7-v1", s7_enabled=True
    )
    contract = resolve_embedding_contract(manifest)
    assert contract.embedding_text_version == "s5-s6-s7-v1"
    assert contract.s7_enabled is True
    assert contract.source == "contract"


# --- malformed contracts: fail closed, never legacy-fall-back ------------------

@pytest.mark.parametrize("missing_field", [
    "version", "model_id", "revision", "max_seq_length", "distance_metric",
    "normalize_embeddings", "embedding_text_version", "s7_enabled",
])
def test_contract_missing_required_field_is_rejected(missing_field):
    manifest = _v2_manifest()
    del manifest["embedding_contract"][missing_field]
    with pytest.raises(KbStartupError, match=missing_field):
        resolve_embedding_contract(manifest)


@pytest.mark.parametrize("field,value", [
    ("version", 2),
    ("version", True),
    ("version", "1"),
    ("model_id", "other-model"),
    ("model_id", ""),
    ("revision", "abc123"),                                        # too short
    ("revision", "1110A243FDF4706B3F48F1D95DB1A4F5529B4D41"),      # uppercase
    ("revision", "zzz0a243fdf4706b3f48f1d95db1a4f5529b4d41"),      # non-hex
    ("max_seq_length", 0),
    ("max_seq_length", -1),
    ("max_seq_length", True),
    ("max_seq_length", "256"),
    ("distance_metric", "dot"),
    ("normalize_embeddings", "false"),
    ("normalize_embeddings", 0),
    ("embedding_text_version", ""),
    ("embedding_text_version", 5),
    ("s7_enabled", "yes"),
    ("s7_enabled", 1),
])
def test_contract_invalid_field_value_is_rejected(field, value):
    manifest = _v2_manifest(**{field: value})
    with pytest.raises(KbStartupError, match=field):
        resolve_embedding_contract(manifest)


def test_contract_that_is_not_a_dict_is_rejected():
    manifest = _v2_manifest()
    manifest["embedding_contract"] = []
    with pytest.raises(KbStartupError, match="embedding_contract"):
        resolve_embedding_contract(manifest)


def test_malformed_contract_never_falls_back_to_legacy():
    """A manifest whose top-level model WOULD qualify for the legacy mapping
    must still fail when it carries a malformed contract."""
    manifest = _v2_manifest(revision="short")
    assert manifest["embedding_model"] == PINNED_MODEL_ID
    with pytest.raises(KbStartupError):
        resolve_embedding_contract(manifest)


def test_top_level_model_must_equal_contract_model_id():
    manifest = _v2_manifest()
    manifest["embedding_model"] = "some-other-model"
    with pytest.raises(KbStartupError, match="embedding_model"):
        resolve_embedding_contract(manifest)
