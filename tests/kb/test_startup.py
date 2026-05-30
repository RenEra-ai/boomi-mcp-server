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

# The heavy build is deferred, so `import server` must NOT import the ML stack;
# forcing warmup via get() then performs the build and imports it. EAGER is off
# so nothing kicks before the assertion (in-process import never kicks anyway).
DEFERRED_IMPORT_SCRIPT = """
import sys
import server
assert "chromadb" not in sys.modules, "chromadb imported at import (build not deferred)"
assert "sentence_transformers" not in sys.modules, "sentence_transformers imported at import"
svc = server._kb_warmup.get(wait_seconds=120)
assert svc is not None, "KB warmup did not become ready"
assert "chromadb" in sys.modules, "chromadb not imported after forcing warmup"
res = server.search_boomi_docs(query="database connector")
assert res["_success"] is True, res
print("DEFERRED_IMPORT_OK")
"""

# Chunk-count mismatch is a HEAVY-build check, so it no longer crashes import —
# it surfaces as a sanitized kb_unavailable from the warmed tool. The build gate
# in Workstream C catches it before prod.
CHUNK_MISMATCH_SCRIPT = """
import server
svc = server._kb_warmup.get(wait_seconds=120)
assert svc is None, "expected warmup to fail on chunk-count mismatch"
resp = server._kb_warmup.not_ready_response()
assert resp["error"] == "kb_unavailable", resp
assert resp["error_type"] == "KbStartupError", resp
blob = repr(resp)
assert "chunk count mismatch" not in blob, blob  # no raw detail leak to client
print("CHUNK_MISMATCH_KB_UNAVAILABLE_OK")
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


def test_import_defers_ml_then_warmup_builds_and_serves():
    result = run_import_server(
        DEFERRED_IMPORT_SCRIPT,
        {"BOOMI_DOCS_ENABLED": "true",
         "BOOMI_DOCS_WARMUP_EAGER": "false",
         "BOOMI_DOCS_DB_PATH": get_fixture_corpus(),
         "BOOMI_DOCS_COLLECTION": "boomi_docs"},
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "DEFERRED_IMPORT_OK" in result.stdout


def test_chunk_count_mismatch_surfaces_as_kb_unavailable(tmp_path):
    build_fixture_corpus(str(tmp_path), manifest_overrides={"chunk_count": 12345})
    result = run_import_server(
        CHUNK_MISMATCH_SCRIPT,
        {"BOOMI_DOCS_ENABLED": "true",
         "BOOMI_DOCS_WARMUP_EAGER": "false",
         "BOOMI_DOCS_DB_PATH": str(tmp_path)},
    )
    # Import no longer crashes on a heavy-build failure; the tool degrades.
    assert result.returncode == 0, result.stdout + result.stderr
    assert "CHUNK_MISMATCH_KB_UNAVAILABLE_OK" in result.stdout
