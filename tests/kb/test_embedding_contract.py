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
    assert_collection_metric,
    assert_model_seq_length,
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


# --- loaded-model / collection assertion helpers -------------------------------

class _FakeModel:
    def __init__(self, max_seq_length):
        self.max_seq_length = max_seq_length


class _FakeCollection:
    def __init__(self, metadata=None, configuration_json=None):
        self.metadata = metadata
        if configuration_json is not None:
            self.configuration_json = configuration_json


def _legacy_contract():
    return resolve_embedding_contract(
        {"schema_version": "1", "embedding_model": "all-MiniLM-L6-v2"}
    )


def test_assert_model_seq_length_accepts_matching_model():
    assert_model_seq_length(_FakeModel(256), _legacy_contract())


def test_assert_model_seq_length_rejects_mismatch():
    with pytest.raises(KbStartupError, match="max_seq_length"):
        assert_model_seq_length(_FakeModel(512), _legacy_contract())


def test_assert_collection_metric_reads_legacy_metadata_key():
    assert_collection_metric(
        _FakeCollection(metadata={"hnsw:space": "cosine"}), _legacy_contract()
    )


def test_assert_collection_metric_falls_back_to_configuration_json():
    collection = _FakeCollection(
        metadata=None, configuration_json={"hnsw": {"space": "cosine"}}
    )
    assert_collection_metric(collection, _legacy_contract())


def test_assert_collection_metric_rejects_wrong_space():
    with pytest.raises(KbStartupError, match="distance metric"):
        assert_collection_metric(
            _FakeCollection(metadata={"hnsw:space": "l2"}), _legacy_contract()
        )


def test_assert_collection_metric_fails_closed_when_indeterminate():
    with pytest.raises(KbStartupError, match="distance metric"):
        assert_collection_metric(_FakeCollection(metadata={}), _legacy_contract())


# --- Docker preload parity ------------------------------------------------------

import json
import subprocess
import types

from boomi_mcp.kb.embedding_contract import preload_model


def _write_manifest_dir(tmp_path, manifest):
    with open(os.path.join(str(tmp_path), "manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f)
    return str(tmp_path)


def test_preload_model_loads_exactly_the_resolved_contract(tmp_path, monkeypatch):
    """Parity rule: the Docker preload must load the model identity the RUNTIME
    resolver produces for the same manifest — same function, same contract."""
    db_path = _write_manifest_dir(
        tmp_path,
        {"schema_version": "1", "collection_name": "boomi_docs",
         "embedding_model": "all-MiniLM-L6-v2"},
    )
    calls = []

    fake_module = types.ModuleType("sentence_transformers")

    def _fake_st(model_name_or_path, revision=None):
        calls.append((model_name_or_path, revision))
        return object()

    fake_module.SentenceTransformer = _fake_st
    monkeypatch.setitem(sys.modules, "sentence_transformers", fake_module)

    contract = preload_model(db_path)
    expected = resolve_embedding_contract(
        {"schema_version": "1", "embedding_model": "all-MiniLM-L6-v2"}
    )
    assert contract == expected
    assert calls == [(PINNED_MODEL_ID, KB24_COMPATIBLE_REVISION)]


def test_preload_model_fails_closed_on_malformed_manifest(tmp_path, monkeypatch):
    db_path = _write_manifest_dir(
        tmp_path,
        {"schema_version": "1", "collection_name": "boomi_docs",
         "embedding_model": "unknown-model"},
    )
    fake_module = types.ModuleType("sentence_transformers")
    fake_module.SentenceTransformer = lambda *a, **k: pytest.fail(
        "must not load a model for an unresolvable manifest"
    )
    monkeypatch.setitem(sys.modules, "sentence_transformers", fake_module)
    with pytest.raises(KbStartupError, match="legacy"):
        preload_model(db_path)


def test_preload_cli_exits_nonzero_on_bad_manifest(tmp_path):
    (tmp_path / "manifest.json").write_text("{not json", encoding="utf-8")
    result = subprocess.run(
        [sys.executable, os.path.join(_ROOT, "scripts", "preload_kb_model.py"),
         "--db-path", str(tmp_path)],
        capture_output=True, text=True,
        env={**os.environ, "PYTHONPATH": _SRC},
    )
    assert result.returncode == 1
    assert "KB model preload failed" in result.stdout + result.stderr
