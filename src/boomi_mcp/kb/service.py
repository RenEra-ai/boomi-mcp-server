"""Boomi docs knowledge base retrieval service.

chromadb and sentence-transformers are imported lazily inside
``build_kb_service_heavy`` so that merely importing this module does not pull in
the ML stack. server.py only imports this module when ``BOOMI_DOCS_ENABLED`` is
true. The startup sequence is split into two halves so the heavy half can be
deferred off the import/socket-bind path (see boomi_mcp.kb.warmup):

* ``validate_kb_manifest_cheap`` — pure-stdlib config + manifest validation
  (no ML imports). Run at import so a misconfigured corpus still fails fast.
* ``build_kb_service_heavy`` — opens Chroma + loads the embedding model. This is
  the multi-second cold-start cost; it is the single entry point that touches
  the heavy dependencies.

``build_kb_service`` composes the two and keeps its original signature/behavior
(used by the Docker build-time validation gate and the startup test matrix).
"""
import os
import time
from dataclasses import dataclass

from .errors import KbQueryError, KbStartupError
from .manifest import corpus_version, load_manifest, validate_manifest

# Spec §4.1 environment variable defaults.
DEFAULT_DB_PATH = "/app/kb/boomi_knowledge_db"
DEFAULT_COLLECTION = "boomi_docs"
DEFAULT_TOP_K = 5
DEFAULT_TOP_K_MAX = 10
DEFAULT_LOW_CONFIDENCE_DISTANCE = 0.45

# Tuning constants.
OVERFETCH_CEILING = 30           # cap on Chroma n_results for diversification
READ_PAGE_DEFAULT_MAX_CHUNKS = 15
READ_PAGE_MAX_CHUNKS_CEILING = 30


def _env_int(name, default):
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError:
        print(f"[WARNING] {name}={raw!r} is not an integer; using default {default}")
        return default


def _env_float(name, default):
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError:
        print(f"[WARNING] {name}={raw!r} is not a float; using default {default}")
        return default


def _kb_config():
    """Read KB configuration from the environment with spec §4.1 defaults."""
    return {
        "db_path": os.getenv("BOOMI_DOCS_DB_PATH", DEFAULT_DB_PATH),
        "collection": os.getenv("BOOMI_DOCS_COLLECTION", DEFAULT_COLLECTION),
        "top_k_default": _env_int("BOOMI_DOCS_TOP_K_DEFAULT", DEFAULT_TOP_K),
        "top_k_max": _env_int("BOOMI_DOCS_TOP_K_MAX", DEFAULT_TOP_K_MAX),
        "low_confidence_distance": _env_float(
            "BOOMI_DOCS_LOW_CONFIDENCE_DISTANCE", DEFAULT_LOW_CONFIDENCE_DISTANCE
        ),
    }


# Per-chunk provenance keys the builder writes alongside the original 8 metadata
# fields (Companion corpus). The two label fields default to the official values
# so a pre-provenance corpus still reads cleanly; the rest default to "".
_PROVENANCE_LABEL_DEFAULTS = {
    "source_type": "official",
    "verification_status": "official",
}
_PROVENANCE_STRING_KEYS = (
    "upstream_repo",
    "upstream_commit",
    "source_path",
    "raw_url",
    "latest_url",
)


def _provenance_fields(meta):
    """Extract the seven provenance fields from a chunk's metadata (frozen
    contract). Missing keys fall back to official/"" so PRE-provenance corpora
    surface as official docs rather than blank/unverified."""
    fields = {
        key: meta.get(key, default)
        for key, default in _PROVENANCE_LABEL_DEFAULTS.items()
    }
    for key in _PROVENANCE_STRING_KEYS:
        fields[key] = meta.get(key, "")
    return fields


def _flatten_query_result(raw):
    """Flatten a Chroma ``query`` result into a list of hit dicts.

    Chroma already returns hits in ascending-distance order.
    """
    ids = (raw.get("ids") or [[]])[0]
    documents = (raw.get("documents") or [[]])[0]
    metadatas = (raw.get("metadatas") or [[]])[0]
    distances = (raw.get("distances") or [[]])[0]

    hits = []
    for i, chunk_id in enumerate(ids):
        meta = metadatas[i] or {}
        hit = {
            "chunk_id": chunk_id,
            "title": meta.get("title", ""),
            "section_heading": meta.get("section_heading", ""),
            "breadcrumb": meta.get("breadcrumb", ""),
            "page_key": meta.get("page_key", ""),
            "source_url": meta.get("source_url", ""),
            "category": meta.get("category", ""),
            "chunk_index": meta.get("chunk_index"),
            "token_estimate": meta.get("token_estimate"),
            "distance": distances[i] if i < len(distances) else None,
            "content": documents[i] if i < len(documents) else "",
        }
        hit.update(_provenance_fields(meta))
        hits.append(hit)
    return hits


def _diversify_by_page(candidates, top_k):
    """Per-page round-robin diversification (spec §5.1).

    Group candidates by page_key (each group stays in ascending-distance order
    because Chroma returned them that way), then round-robin one best-remaining
    chunk per page per pass until top_k hits are collected. Finally re-sort the
    selection by ascending distance so the output is still distance-ordered.
    """
    if not candidates:
        return []

    groups = {}
    order = []
    for hit in candidates:
        # page_key is always populated by the producer; fall back to chunk_id
        # defensively so a missing key never collapses unrelated chunks.
        page_key = hit.get("page_key") or hit.get("chunk_id")
        if page_key not in groups:
            groups[page_key] = []
            order.append(page_key)
        groups[page_key].append(hit)

    selected = []
    while len(selected) < top_k:
        progressed = False
        for page_key in order:
            group = groups[page_key]
            if group:
                selected.append(group.pop(0))
                progressed = True
                if len(selected) >= top_k:
                    break
        if not progressed:
            break

    selected.sort(key=lambda h: (h["distance"] is None, h["distance"]))
    return selected


class KbService:
    """Holds the Chroma collection + manifest and serves search / read_page.

    Constructed once at startup by ``build_kb_service`` and stored on the
    server; MCP tools close over a single instance (spec §4.4).
    """

    def __init__(self, collection, manifest, config):
        self._collection = collection
        self.manifest = manifest
        self._config = config
        self.embedding_model = manifest.get("embedding_model", "unknown")
        self.corpus_built_at = manifest.get("build_timestamp", "unknown")
        self.corpus_version = corpus_version(manifest)

    def _provenance(self):
        """Corpus freshness fields included in every KB tool response (spec §4.4)."""
        return {
            "corpus_built_at": self.corpus_built_at,
            "corpus_version": self.corpus_version,
            "embedding_model": self.embedding_model,
        }

    def search(self, query, top_k=None):
        """Semantic search over the corpus (spec §5.1).

        Returns a structured dict. Raises KbQueryError only on an underlying
        Chroma failure; expected problems (empty query) return a structured
        error instead.
        """
        trimmed = (query or "").strip()
        if not trimmed:
            return {
                "_success": False,
                "error": "empty_query",
                "message": "query must be a non-empty string",
            }

        if top_k is None:
            top_k = self._config["top_k_default"]
        try:
            top_k = int(top_k)
        except (TypeError, ValueError):
            top_k = self._config["top_k_default"]
        top_k = max(1, min(top_k, self._config["top_k_max"]))

        low_confidence_distance = self._config["low_confidence_distance"]
        n_results = min(top_k * 3, OVERFETCH_CEILING)

        try:
            raw = self._collection.query(query_texts=[trimmed], n_results=n_results)
        except Exception as e:
            raise KbQueryError(f"KB search query failed: {e}")

        hits = _diversify_by_page(_flatten_query_result(raw), top_k)
        best_distance = hits[0]["distance"] if hits else None

        if not hits:
            status = "no_match"
        elif best_distance is not None and best_distance <= low_confidence_distance:
            status = "ok"
        else:
            status = "low_confidence"

        distances = [h["distance"] for h in hits if h["distance"] is not None]
        print(
            f"[INFO] search_boomi_docs query={trimmed!r} top_k={top_k} "
            f"results={len(hits)} status={status} "
            f"distance_min={min(distances) if distances else None} "
            f"distance_max={max(distances) if distances else None}"
        )

        # low_confidence / no_match must carry an explicit warning so the model
        # does not present unsupported Boomi facts as authoritative (spec §5.1).
        warning = None
        if status == "low_confidence":
            warning = (
                "Low-confidence match: the knowledge base does not strongly "
                "cover this query. Do not present unsupported Boomi facts as "
                "authoritative; reformulate the query or tell the user the "
                "documentation does not provide enough support."
            )
        elif status == "no_match":
            warning = (
                "No match: the knowledge base returned no results for this "
                "query. Do not invent Boomi facts; tell the user the "
                "documentation does not cover this topic."
            )

        result = {
            "_success": True,
            "query": trimmed,
            "top_k": top_k,
            "status": status,
            "warning": warning,
            "best_distance": best_distance,
            "low_confidence_distance": low_confidence_distance,
            "hits": hits,
        }
        result.update(self._provenance())
        return result

    def read_page(self, page_key, max_chunks=READ_PAGE_DEFAULT_MAX_CHUNKS,
                  start_chunk_index=0):
        """Reconstruct a documentation page from its chunks (spec §5.2).

        Returns a structured dict. Raises KbQueryError only on an underlying
        Chroma failure; expected problems (empty / unknown page_key) return a
        structured error instead.
        """
        trimmed = (page_key or "").strip()
        if not trimmed:
            return {
                "_success": False,
                "error": "empty_page_key",
                "message": "page_key must be a non-empty string",
            }

        try:
            max_chunks = int(max_chunks)
        except (TypeError, ValueError):
            max_chunks = READ_PAGE_DEFAULT_MAX_CHUNKS
        max_chunks = max(1, min(max_chunks, READ_PAGE_MAX_CHUNKS_CEILING))

        try:
            start_chunk_index = int(start_chunk_index)
        except (TypeError, ValueError):
            start_chunk_index = 0
        start_chunk_index = max(0, start_chunk_index)

        try:
            raw = self._collection.get(
                where={"page_key": trimmed},
                include=["documents", "metadatas"],
            )
        except Exception as e:
            raise KbQueryError(f"KB page read failed: {e}")

        ids = raw.get("ids") or []
        documents = raw.get("documents") or []
        metadatas = raw.get("metadatas") or []

        if not ids:
            return {
                "_success": False,
                "error": "no_chunks_for_page_key",
                "page_key": trimmed,
            }

        items = []
        for i, chunk_id in enumerate(ids):
            meta = metadatas[i] or {}
            items.append({
                "chunk_id": chunk_id,
                "section_heading": meta.get("section_heading", ""),
                "chunk_index": meta.get("chunk_index", 0),
                "content": documents[i] if i < len(documents) else "",
                "_meta": meta,
            })
        items.sort(key=lambda it: it["chunk_index"])

        # Non-contiguous chunk_index is a corpus-quality smell, not a hard error:
        # log it and return what we have (spec §5.2 errors).
        actual_indices = [it["chunk_index"] for it in items]
        if actual_indices != list(range(len(items))):
            print(
                f"[WARNING] read_boomi_doc_page: non-contiguous chunk_index for "
                f"page_key={trimmed!r}: {actual_indices}"
            )

        chunk_count = len(items)
        sliced = [
            it for it in items if it["chunk_index"] >= start_chunk_index
        ][:max_chunks]
        chunks_returned = len(sliced)
        truncated = (start_chunk_index + chunks_returned) < chunk_count

        first_meta = items[0]["_meta"]
        result = {
            "_success": True,
            "page_key": trimmed,
            "source_url": first_meta.get("source_url", ""),
            "title": first_meta.get("title", ""),
            "breadcrumb": first_meta.get("breadcrumb", ""),
            "category": first_meta.get("category", ""),
            "chunk_count": chunk_count,
            "start_chunk_index": start_chunk_index,
            "chunks_returned": chunks_returned,
            "truncated": truncated,
            "chunks": [
                {
                    "chunk_id": it["chunk_id"],
                    "section_heading": it["section_heading"],
                    "chunk_index": it["chunk_index"],
                    "content": it["content"],
                    "source_type": it["_meta"].get("source_type", "official"),
                    "verification_status": it["_meta"].get(
                        "verification_status", "official"
                    ),
                }
                for it in sliced
            ],
        }
        # Page-level provenance comes from the first chunk's metadata, matching
        # the page-level source_url/title/breadcrumb above.
        result.update(_provenance_fields(first_meta))
        if truncated:
            result["next_chunk_index"] = start_chunk_index + chunks_returned
        result.update(self._provenance())

        print(
            f"[INFO] read_boomi_doc_page page_key={trimmed!r} "
            f"chunk_count={chunk_count} returned={chunks_returned} "
            f"truncated={truncated}"
        )
        return result


@dataclass(frozen=True)
class KbBootstrap:
    """Cheap, ML-free startup state produced by ``validate_kb_manifest_cheap``.

    Carries both the resolved ``config`` and the parsed ``manifest`` because the
    heavy build AND KbService need config (db_path/collection/top_k/confidence),
    not just the manifest. Held by KbWarmup and handed to ``build_kb_service_heavy``
    when the deferred build runs.
    """

    config: dict
    manifest: dict


def validate_kb_manifest_cheap():
    """Run the cheap, pure-stdlib half of the spec §4.3 startup sequence.

    Resolves config, checks the corpus path, loads + validates the manifest, and
    confirms the embedding_model field is present. Imports NO ML dependencies, so
    it is safe to run on the import/socket-bind path (server.py runs it at import
    to preserve fail-fast for cheap-detectable corpus problems). Raises
    KbStartupError on any failure. Returns a KbBootstrap for build_kb_service_heavy.
    """
    config = _kb_config()
    db_path = config["db_path"]
    collection_name = config["collection"]

    # Steps 1-2: corpus path + manifest exist and parse.
    if not os.path.isdir(db_path):
        raise KbStartupError(f"BOOMI_DOCS_DB_PATH does not exist: {db_path}")
    manifest = load_manifest(db_path)

    # Steps 3-4: manifest schema version + collection name.
    validate_manifest(manifest, collection_name)

    # Embedding-model presence is a cheap manifest check; fail fast here (moved
    # ahead of the heavy chromadb import) so a model-less manifest never defers a
    # build that is guaranteed to fail.
    if not manifest.get("embedding_model"):
        raise KbStartupError("KB manifest is missing 'embedding_model'")

    return KbBootstrap(config=config, manifest=manifest)


def build_kb_service_heavy(bootstrap):
    """Run the heavy half of startup and return a ready KbService.

    Opens the Chroma client + collection and loads the SentenceTransformer
    embedding model — the multi-second cold-start cost. chromadb and
    sentence-transformers are imported here (and nowhere else at import time),
    so a server with BOOMI_DOCS_ENABLED unset never loads the ML stack and the
    deferred warmup thread is the only place this cost is paid in production.
    Emits per-phase timing logs so the cold-start breakdown is measurable in
    Cloud Logging. Raises KbStartupError on any failure.
    """
    config = bootstrap.config
    manifest = bootstrap.manifest
    db_path = config["db_path"]
    collection_name = config["collection"]
    embedding_model = manifest.get("embedding_model")

    def _phase(name, start):
        print(f"[INFO] KB warmup phase={name} seconds={time.monotonic() - start:.2f}")

    # Heavy dependencies are imported only after the cheap checks pass.
    t = time.monotonic()
    try:
        import chromadb
        from chromadb.utils import embedding_functions
    except ImportError as e:
        raise KbStartupError(
            "KB dependencies are not installed — install requirements-kb.txt "
            f"(chromadb, sentence-transformers): {e}"
        )
    _phase("import_deps", t)

    # Step 5: open the persistent client.
    t = time.monotonic()
    try:
        client = chromadb.PersistentClient(path=db_path)
    except Exception as e:
        raise KbStartupError(f"Failed to open Chroma client at {db_path}: {e}")
    _phase("open_client", t)

    # Step 8 (before 6): the embedding function is required to re-open the
    # collection so that query_texts work — so build it before get_collection.
    t = time.monotonic()
    try:
        ef = embedding_functions.SentenceTransformerEmbeddingFunction(
            model_name=embedding_model
        )
    except Exception as e:
        raise KbStartupError(f"Failed to load embedding model {embedding_model!r}: {e}")
    _phase("load_model", t)

    # Step 6: open the collection.
    t = time.monotonic()
    try:
        collection = client.get_collection(name=collection_name, embedding_function=ef)
    except Exception as e:
        raise KbStartupError(
            f"Failed to open Chroma collection {collection_name!r}: {e}"
        )
    _phase("open_collection", t)

    # Step 7: exact count match guards against incomplete/corrupt artifacts.
    t = time.monotonic()
    expected_count = manifest.get("chunk_count")
    actual_count = collection.count()
    if actual_count != expected_count:
        raise KbStartupError(
            f"KB corpus chunk count mismatch: collection has {actual_count}, "
            f"manifest declares {expected_count}"
        )
    _phase("count", t)

    # Step 9: a probe query must return something.
    t = time.monotonic()
    try:
        probe = collection.query(query_texts=["probe"], n_results=1)
    except Exception as e:
        raise KbStartupError(f"KB probe query failed: {e}")
    if not (probe.get("ids") and probe["ids"][0]):
        raise KbStartupError("KB probe query returned no results — corpus may be empty")
    _phase("probe", t)

    service = KbService(collection, manifest, config)
    print(
        f"[INFO] Boomi Docs KB ready: collection={collection_name} "
        f"model={embedding_model} chunks={actual_count} "
        f"built={manifest.get('build_timestamp', 'unknown')}"
    )
    return service


def build_kb_service():
    """Run the full spec §4.3 startup sequence and return a ready KbService.

    Thin orchestrator: ``build_kb_service_heavy(validate_kb_manifest_cheap())``.
    Unchanged public signature/behavior — used by the Docker build-time
    validation gate and the startup test matrix. Raises KbStartupError on any
    validation failure.
    """
    return build_kb_service_heavy(validate_kb_manifest_cheap())
