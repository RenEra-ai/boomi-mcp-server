"""Startup tests that must hold in the DEFAULT deployment — KB disabled, and the
KB dependencies (chromadb / sentence-transformers) NOT installed.

Deliberately NO ``pytest.importorskip`` guard: these protect exactly the
no-KB-deps path, so they must run even when the KB deps are absent. Every test
here runs ``import server`` in a fresh subprocess and never imports chromadb in
the parent process. The deps-requiring startup tests live in test_startup.py.
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

import json

from _fixture_corpus import IMPORT_ONLY_SCRIPT, run_import_server

DISABLED_SCRIPT = """
import sys
import server
assert "chromadb" not in sys.modules, "chromadb imported while KB disabled"
assert "sentence_transformers" not in sys.modules, "sentence_transformers imported while disabled"
assert not hasattr(server, "search_boomi_docs"), "KB tool registered while disabled"
print("DISABLED_OK")
"""

# Deps-AGNOSTIC: with the KB enabled and a manifest that passes CHEAP validation,
# `import server` registers the tools but must NOT import the ML stack — the
# heavy build is deferred to KbWarmup. Holds whether or not chromadb is installed
# because the build never runs here (no tool call). EAGER off so nothing kicks.
DEFERRED_IMPORT_NO_ML_SCRIPT = """
import sys
import server
assert hasattr(server, "search_boomi_docs"), "KB tool not registered while enabled"
assert "chromadb" not in sys.modules, "chromadb imported at import (build not deferred)"
assert "sentence_transformers" not in sys.modules, "sentence_transformers imported at import"
print("DEFERRED_IMPORT_NO_ML_OK")
"""

# DETERMINISTIC missing-deps degradation: a meta_path finder raises
# ModuleNotFoundError for the ML modules BEFORE `import server`, so the heavy
# build hits the simulated ImportError regardless of what is installed. The tool
# must return the SANITIZED kb_unavailable (generic message + coarse error_type,
# no raw ImportError / requirements text).
MISSING_DEPS_SCRIPT = """
import sys
import importlib.abc

class _BlockML(importlib.abc.MetaPathFinder):
    BLOCKED = ("chromadb", "sentence_transformers")
    def find_spec(self, name, path, target=None):
        if name.split(".")[0] in self.BLOCKED:
            raise ModuleNotFoundError("simulated missing dependency: " + name)
        return None

sys.meta_path.insert(0, _BlockML())

import server
svc = server._kb_warmup.get(wait_seconds=30)
assert svc is None, "warmup should fail when ML deps are missing"
resp = server._kb_warmup.not_ready_response()
assert resp["error"] == "kb_unavailable", resp
assert resp["error_type"] == "KbStartupError", resp
blob = repr(resp)
assert "requirements-kb" not in blob, blob
assert "chromadb" not in blob, blob
print("MISSING_DEPS_KB_UNAVAILABLE_OK")
"""


def _write_cheap_valid_manifest(dir_path):
    """Write a minimal manifest.json that passes validate_kb_manifest_cheap
    (schema_version + collection_name + embedding_model) WITHOUT a Chroma corpus,
    so the cheap import-time validation succeeds with no ML deps."""
    manifest = {
        "schema_version": "1",
        "collection_name": "boomi_docs",
        "embedding_model": "all-MiniLM-L6-v2",
        "chunk_count": 1,
        "build_timestamp": "2026-01-01T00:00:00Z",
    }
    with open(os.path.join(dir_path, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f)


def test_disabled_boots_clean_without_ml_imports():
    result = run_import_server(DISABLED_SCRIPT, {}, unset=("BOOMI_DOCS_ENABLED",))
    assert result.returncode == 0, result.stdout + result.stderr
    assert "DISABLED_OK" in result.stdout


def test_enabled_without_corpus_fails_fast():
    result = run_import_server(
        IMPORT_ONLY_SCRIPT,
        {"BOOMI_DOCS_ENABLED": "true",
         "BOOMI_DOCS_DB_PATH": "/tmp/kb-corpus-does-not-exist"},
    )
    assert result.returncode == 1
    assert "Boomi Docs KB startup failed" in (result.stdout + result.stderr)


def test_corrupt_manifest_exits_with_clear_error(tmp_path):
    (tmp_path / "manifest.json").write_text("{ corrupt json", encoding="utf-8")
    result = run_import_server(
        IMPORT_ONLY_SCRIPT,
        {"BOOMI_DOCS_ENABLED": "true", "BOOMI_DOCS_DB_PATH": str(tmp_path)},
    )
    assert result.returncode == 1
    combined = result.stdout + result.stderr
    assert "Boomi Docs KB startup failed" in combined
    assert "not valid JSON" in combined


def test_import_defers_ml_stack_when_enabled(tmp_path):
    _write_cheap_valid_manifest(str(tmp_path))
    result = run_import_server(
        DEFERRED_IMPORT_NO_ML_SCRIPT,
        {"BOOMI_DOCS_ENABLED": "true",
         "BOOMI_DOCS_WARMUP_EAGER": "false",
         "BOOMI_DOCS_DB_PATH": str(tmp_path)},
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "DEFERRED_IMPORT_NO_ML_OK" in result.stdout


def test_missing_deps_degrade_to_sanitized_kb_unavailable(tmp_path):
    _write_cheap_valid_manifest(str(tmp_path))
    result = run_import_server(
        MISSING_DEPS_SCRIPT,
        {"BOOMI_DOCS_ENABLED": "true",
         "BOOMI_DOCS_WARMUP_EAGER": "false",
         "BOOMI_DOCS_DB_PATH": str(tmp_path)},
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "MISSING_DEPS_KB_UNAVAILABLE_OK" in result.stdout
