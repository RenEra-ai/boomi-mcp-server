"""Startup integration tests that require the KB dependencies and a real corpus.

The deps-free startup tests (KB-disabled path, fail-fast cases) live in
test_startup_no_deps.py so they still run when chromadb / sentence-transformers
are not installed — that is the default deployment path they protect.

These run `import server` in fresh subprocesses because `server` is
import-cached within a process.
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

from _fixture_corpus import (
    IMPORT_ONLY_SCRIPT,
    build_fixture_corpus,
    get_fixture_corpus,
    run_import_server,
)

ENABLED_SCRIPT = """
import asyncio
import server
asyncio.run(server.mcp.get_tool("search_boomi_docs"))
asyncio.run(server.mcp.get_tool("read_boomi_doc_page"))
resources = asyncio.run(server.mcp.list_resources())
assert len(resources) == 1, resources
assert str(resources[0].uri) == "kb://boomi-docs/corpus"
print("ENABLED_OK")
"""


def test_enabled_boots_with_two_tools_and_one_resource():
    result = run_import_server(
        ENABLED_SCRIPT,
        {"BOOMI_DOCS_ENABLED": "true",
         "BOOMI_DOCS_DB_PATH": get_fixture_corpus(),
         "BOOMI_DOCS_COLLECTION": "boomi_docs"},
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "ENABLED_OK" in result.stdout
    assert "Boomi Docs KB registered" in result.stdout


def test_chunk_count_mismatch_exits_with_clear_error(tmp_path):
    build_fixture_corpus(str(tmp_path), manifest_overrides={"chunk_count": 12345})
    result = run_import_server(
        IMPORT_ONLY_SCRIPT,
        {"BOOMI_DOCS_ENABLED": "true", "BOOMI_DOCS_DB_PATH": str(tmp_path)},
    )
    assert result.returncode == 1
    assert "chunk count mismatch" in (result.stdout + result.stderr)
