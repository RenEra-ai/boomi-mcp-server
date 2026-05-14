"""Test helper: build a real Chroma corpus from the checked-in fixture JSONL.

This mirrors knowledge-base-builder/build_index.py's ``collection.add()`` call so
the KB service consumes a fixture shaped exactly like a production artifact.
Keep the metadata field list in ``build_fixture_corpus`` in sync with
build_index.py.

Not a test module (underscore-prefixed) — pytest does not collect it.
"""
import asyncio
import json
import os
import subprocess
import sys
import tempfile

FIXTURE_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "fixtures", "kb")
CORPUS_JSONL = os.path.join(FIXTURE_DIR, "corpus.jsonl")
MANIFEST_JSON = os.path.join(FIXTURE_DIR, "manifest.json")
COLLECTION_NAME = "boomi_docs"
EMBEDDING_MODEL = "all-MiniLM-L6-v2"

# Repo root: tests/kb/_fixture_corpus.py -> tests/kb -> tests -> repo root.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Minimal subprocess script body: just import server. Used by startup tests.
IMPORT_ONLY_SCRIPT = "import server\n"

_CACHED_CORPUS_DIR = None
_CACHED_SERVICE = None


def run_async(coro):
    """Run a coroutine on a throwaway loop without touching the thread's global
    event loop.

    asyncio.run() sets the current event loop to None when it finishes, which
    poisons other test modules in the same process that still rely on the legacy
    asyncio.get_event_loop() path (e.g. tests/test_verified_storage.py). Creating
    a fresh loop and never registering it as current keeps that global state
    untouched.
    """
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def run_import_server(script, env_overrides, unset=()):
    """Run `python -c <script>` in a fresh process with controlled environment.

    Always sets BOOMI_LOCAL=true, applies env_overrides, removes `unset` keys.
    The KB startup tests use this so each scenario gets a clean interpreter —
    `server` is import-cached within a process. Importing this helper does not
    require the KB dependencies, so it is safe for the no-deps startup module.
    """
    env = os.environ.copy()
    env["BOOMI_LOCAL"] = "true"
    env.update(env_overrides)
    for key in unset:
        env.pop(key, None)
    return subprocess.run(
        [sys.executable, "-c", script],
        cwd=_REPO_ROOT, env=env, capture_output=True, text=True,
    )


def load_fixture_chunks():
    """Load the checked-in fixture chunks (producer chunk schema)."""
    chunks = []
    with open(CORPUS_JSONL, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                chunks.append(json.loads(line))
    return chunks


def load_fixture_manifest():
    with open(MANIFEST_JSON, encoding="utf-8") as f:
        return json.load(f)


def build_fixture_corpus(dest_dir, manifest_overrides=None, write_manifest=True):
    """Build a Chroma persistent corpus at ``dest_dir`` from the fixture JSONL.

    Returns ``dest_dir``. When ``write_manifest`` is true, writes manifest.json
    (optionally patched with ``manifest_overrides`` for negative startup tests).
    """
    import chromadb
    from chromadb.utils import embedding_functions

    chunks = load_fixture_chunks()

    ef = embedding_functions.SentenceTransformerEmbeddingFunction(
        model_name=EMBEDDING_MODEL
    )
    client = chromadb.PersistentClient(path=dest_dir)
    try:
        client.delete_collection(COLLECTION_NAME)
    except Exception:
        pass
    collection = client.create_collection(
        name=COLLECTION_NAME,
        embedding_function=ef,
        metadata={"hnsw:space": "cosine"},
    )
    # Mirror knowledge-base-builder/build_index.py build_index() — keep in sync.
    collection.add(
        ids=[c["id"] for c in chunks],
        documents=[c["content"] for c in chunks],
        metadatas=[{
            "title": c["title"],
            "section_heading": c["section_heading"],
            "breadcrumb": c["breadcrumb"],
            "source_url": c["source_url"],
            "page_key": c["page_key"],
            "chunk_index": c["chunk_index"],
            "category": c["category"],
            "token_estimate": c["token_estimate"],
        } for c in chunks],
    )

    if write_manifest:
        manifest = load_fixture_manifest()
        if manifest_overrides:
            manifest.update(manifest_overrides)
        with open(os.path.join(dest_dir, "manifest.json"), "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2)

    return dest_dir


def get_fixture_corpus():
    """Build the valid fixture corpus once per process; return its directory."""
    global _CACHED_CORPUS_DIR
    if _CACHED_CORPUS_DIR is None:
        _CACHED_CORPUS_DIR = tempfile.mkdtemp(prefix="kb_fixture_corpus_")
        build_fixture_corpus(_CACHED_CORPUS_DIR)
    return _CACHED_CORPUS_DIR


def get_kb_service():
    """Build a KbService bound to the valid fixture corpus, once per process.

    Built with default KB config (no BOOMI_DOCS_TOP_K_* overrides) so handler
    tests reason about known defaults.
    """
    global _CACHED_SERVICE
    if _CACHED_SERVICE is None:
        from boomi_mcp.kb.service import build_kb_service
        os.environ["BOOMI_DOCS_DB_PATH"] = get_fixture_corpus()
        os.environ["BOOMI_DOCS_COLLECTION"] = COLLECTION_NAME
        for key in ("BOOMI_DOCS_TOP_K_DEFAULT", "BOOMI_DOCS_TOP_K_MAX",
                    "BOOMI_DOCS_LOW_CONFIDENCE_DISTANCE"):
            os.environ.pop(key, None)
        _CACHED_SERVICE = build_kb_service()
    return _CACHED_SERVICE
