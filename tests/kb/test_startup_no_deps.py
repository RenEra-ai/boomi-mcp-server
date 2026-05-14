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

from _fixture_corpus import IMPORT_ONLY_SCRIPT, run_import_server

DISABLED_SCRIPT = """
import sys
import server
assert "chromadb" not in sys.modules, "chromadb imported while KB disabled"
assert "sentence_transformers" not in sys.modules, "sentence_transformers imported while disabled"
assert not hasattr(server, "search_boomi_docs"), "KB tool registered while disabled"
print("DISABLED_OK")
"""


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
