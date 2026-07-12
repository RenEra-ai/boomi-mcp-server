"""Handler-layer tests for build_kb_service startup validation (spec §4.3)."""
import json
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

from boomi_mcp.kb.embedding_contract import KB24_COMPATIBLE_REVISION
from boomi_mcp.kb.errors import KbStartupError
from boomi_mcp.kb.manifest import load_manifest, validate_manifest
from boomi_mcp.kb.service import build_kb_service, validate_kb_manifest_cheap
from _fixture_corpus import build_fixture_corpus, get_fixture_corpus, load_fixture_manifest


def _point_at(monkeypatch, db_path, collection="boomi_docs"):
    monkeypatch.setenv("BOOMI_DOCS_DB_PATH", str(db_path))
    monkeypatch.setenv("BOOMI_DOCS_COLLECTION", collection)
    for key in ("BOOMI_DOCS_TOP_K_DEFAULT", "BOOMI_DOCS_TOP_K_MAX",
                "BOOMI_DOCS_LOW_CONFIDENCE_DISTANCE"):
        monkeypatch.delenv(key, raising=False)


def _write_manifest(dir_path, manifest):
    os.makedirs(dir_path, exist_ok=True)
    with open(os.path.join(dir_path, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f)


# --- happy path ---------------------------------------------------------------

def test_build_kb_service_succeeds_on_valid_corpus(monkeypatch):
    _point_at(monkeypatch, get_fixture_corpus())
    service = build_kb_service()
    assert service.corpus_version == "kb-test"
    assert service.embedding_model == "all-MiniLM-L6-v2"
    # Probe the live collection through the service.
    assert service.search("database connector")["_success"] is True


def test_fixture_manifest_round_trips_cleanly():
    # Anti-drift guard: the checked-in fixture manifest must satisfy the
    # consumer's own loader/validator (catches schema drift in the fixture).
    manifest = load_manifest(get_fixture_corpus())
    validate_manifest(manifest, "boomi_docs")


# --- failure matrix -----------------------------------------------------------

def test_missing_db_path_raises(monkeypatch, tmp_path):
    _point_at(monkeypatch, tmp_path / "does-not-exist")
    with pytest.raises(KbStartupError, match="does not exist"):
        build_kb_service()


def test_missing_manifest_raises(monkeypatch, tmp_path):
    _point_at(monkeypatch, tmp_path)  # dir exists, but no manifest.json
    with pytest.raises(KbStartupError, match="manifest not found"):
        build_kb_service()


def test_malformed_manifest_json_raises(monkeypatch, tmp_path):
    (tmp_path / "manifest.json").write_text("{not valid json", encoding="utf-8")
    _point_at(monkeypatch, tmp_path)
    with pytest.raises(KbStartupError, match="not valid JSON"):
        build_kb_service()


def test_schema_v2_with_valid_contract_passes_cheap_validation(monkeypatch, tmp_path):
    """kb-25 compatibility: a schema-v2 manifest with a well-formed
    embedding_contract must pass the cheap validation gate."""
    manifest = load_fixture_manifest()
    manifest["schema_version"] = "2"
    manifest["embedding_contract"] = {
        "version": 1,
        "model_id": "all-MiniLM-L6-v2",
        "revision": KB24_COMPATIBLE_REVISION,
        "max_seq_length": 256,
        "distance_metric": "cosine",
        "normalize_embeddings": False,
        "embedding_text_version": "s5-s6-v1",
        "s7_enabled": False,
    }
    _write_manifest(tmp_path, manifest)
    _point_at(monkeypatch, tmp_path)
    bootstrap = validate_kb_manifest_cheap()
    assert bootstrap.contract.source == "contract"
    assert bootstrap.contract.embedding_text_version == "s5-s6-v1"


def test_unsupported_schema_version_raises(monkeypatch, tmp_path):
    manifest = load_fixture_manifest()
    manifest["schema_version"] = "3"
    _write_manifest(tmp_path, manifest)
    _point_at(monkeypatch, tmp_path)
    with pytest.raises(KbStartupError, match="schema_version"):
        build_kb_service()


def test_malformed_contract_fails_cheap_validation(monkeypatch, tmp_path):
    """A present-but-broken embedding_contract fails fast at cheap validation —
    it must never fall back to the legacy mapping."""
    manifest = load_fixture_manifest()
    manifest["embedding_contract"] = {"version": 1}
    _write_manifest(tmp_path, manifest)
    _point_at(monkeypatch, tmp_path)
    with pytest.raises(KbStartupError, match="missing required field"):
        validate_kb_manifest_cheap()


def test_unknown_legacy_model_fails_cheap_validation(monkeypatch, tmp_path):
    manifest = load_fixture_manifest()
    manifest["embedding_model"] = "bge-small-en-v1.5"
    _write_manifest(tmp_path, manifest)
    _point_at(monkeypatch, tmp_path)
    with pytest.raises(KbStartupError, match="legacy"):
        validate_kb_manifest_cheap()


def test_bootstrap_resolves_legacy_contract_for_fixture_corpus(monkeypatch):
    """The kb-24-shaped fixture manifest (no contract) resolves to the pinned
    behavior-compatible revision through the legacy mapping."""
    _point_at(monkeypatch, get_fixture_corpus())
    bootstrap = validate_kb_manifest_cheap()
    assert bootstrap.contract.revision == KB24_COMPATIBLE_REVISION
    assert bootstrap.contract.source == "legacy-kb24"
    assert bootstrap.contract.max_seq_length == 256
    assert bootstrap.contract.distance_metric == "cosine"


def test_collection_name_mismatch_raises(monkeypatch, tmp_path):
    manifest = load_fixture_manifest()
    manifest["collection_name"] = "some_other_collection"
    _write_manifest(tmp_path, manifest)
    _point_at(monkeypatch, tmp_path)
    with pytest.raises(KbStartupError, match="collection_name"):
        build_kb_service()


def test_missing_embedding_model_raises(monkeypatch, tmp_path):
    manifest = load_fixture_manifest()
    manifest.pop("embedding_model", None)
    _write_manifest(tmp_path, manifest)
    _point_at(monkeypatch, tmp_path)
    with pytest.raises(KbStartupError, match="embedding_model"):
        build_kb_service()


def test_chunk_count_mismatch_raises(monkeypatch, tmp_path):
    # Needs a real corpus: the mismatch is only caught after the collection opens.
    build_fixture_corpus(str(tmp_path), manifest_overrides={"chunk_count": 999})
    _point_at(monkeypatch, tmp_path)
    with pytest.raises(KbStartupError, match="chunk count mismatch"):
        build_kb_service()
