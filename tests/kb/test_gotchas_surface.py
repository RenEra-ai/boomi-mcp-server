"""MCP-surface tests for the operational gotcha KB (issue #77).

Each scenario runs `import server` in a FRESH subprocess (server is import-cached
within a process, and the registration is gated on env at import time). These
deliberately have NO `pytest.importorskip("chromadb")` guard: the whole point of
this surface is that it registers and serves WITHOUT the docs-KB ML stack.
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

from _fixture_corpus import run_import_server  # noqa: E402


# Registered + no ML imports + tool annotations + issue_ids lookup, all in one
# clean interpreter. Exercises the documented contract end to end.
REGISTERED_SCRIPT = """
import sys, asyncio
import server
from boomi_mcp.kb.operational_gotchas import (
    OPERATIONAL_GOTCHA_ENTRIES, DETECTIONS, FREQUENCIES, VERIFICATION_STATUSES,
)

assert "chromadb" not in sys.modules, "chromadb imported while only gotchas enabled"
assert "sentence_transformers" not in sys.modules, "ML stack imported for gotchas"
assert hasattr(server, "search_boomi_gotchas"), "gotcha tool not registered"
assert not hasattr(server, "search_boomi_docs"), "docs tool registered with docs disabled"

loop = asyncio.new_event_loop()
try:
    tool = loop.run_until_complete(server.mcp.get_tool("search_boomi_gotchas"))
    assert tool.annotations.readOnlyHint is True, "readOnlyHint must be True"
    assert tool.annotations.openWorldHint is False, "openWorldHint must be False (closed catalog)"

    res = tool.fn(issue_ids=["process_call_parent_redeploy"])
    assert res["_success"] is True and res["read_only"] is True
    assert res["results"][0]["id"] == "process_call_parent_redeploy"

    empty = tool.fn()
    assert empty["error"] == "empty_query"

    # Resource: full catalog with provenance + taxonomies + attribution.
    result = loop.run_until_complete(
        server.mcp.read_resource("kb://boomi-operational-gotchas/catalog")
    )
    body = "".join(part.content for part in result.contents)
    assert "BSD-2-Clause" in body
    for gid in OPERATIONAL_GOTCHA_ENTRIES:
        assert gid in body, "resource missing " + gid
    for tok in set(DETECTIONS) | set(FREQUENCIES) | set(VERIFICATION_STATUSES):
        assert tok in body, "resource missing taxonomy token " + tok

    # The gotcha resource is the ONLY resource here (docs disabled), and it is a
    # static URI — no resource templates.
    resources = loop.run_until_complete(server.mcp.list_resources())
    uris = {str(r.uri) for r in resources}
    assert "kb://boomi-operational-gotchas/catalog" in uris, uris
    templates = loop.run_until_complete(server.mcp.list_resource_templates())
    assert templates == [], "gotcha resource must not register a template"
finally:
    loop.close()

print("GOTCHAS_REGISTERED_OK")
"""

# With the flag unset (default), neither the tool nor the resource registers.
DISABLED_SCRIPT = """
import sys, asyncio
import server
assert not hasattr(server, "search_boomi_gotchas"), "gotcha tool registered while disabled"
loop = asyncio.new_event_loop()
try:
    resources = loop.run_until_complete(server.mcp.list_resources())
    uris = {str(r.uri) for r in resources}
    assert "kb://boomi-operational-gotchas/catalog" not in uris, uris
finally:
    loop.close()
print("GOTCHAS_DISABLED_OK")
"""

# Server instructions carry the gotcha routing block only when enabled.
INSTRUCTIONS_SCRIPT = """
import server
assert server.BOOMI_GOTCHAS_ENABLED is True
assert "search_boomi_gotchas" in server.SERVER_INSTRUCTIONS
assert "operational gotchas" in server.SERVER_INSTRUCTIONS
print("GOTCHAS_INSTRUCTIONS_OK")
"""


def test_gotchas_registered_without_ml_stack():
    result = run_import_server(
        REGISTERED_SCRIPT,
        {"BOOMI_GOTCHAS_ENABLED": "true", "BOOMI_DOCS_ENABLED": "false"},
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "GOTCHAS_REGISTERED_OK" in result.stdout


def test_gotchas_disabled_by_default():
    result = run_import_server(
        DISABLED_SCRIPT, {}, unset=("BOOMI_GOTCHAS_ENABLED", "BOOMI_DOCS_ENABLED")
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "GOTCHAS_DISABLED_OK" in result.stdout


def test_server_instructions_carry_gotchas_block_when_enabled():
    result = run_import_server(
        INSTRUCTIONS_SCRIPT,
        {"BOOMI_GOTCHAS_ENABLED": "true", "BOOMI_DOCS_ENABLED": "false"},
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "GOTCHAS_INSTRUCTIONS_OK" in result.stdout
