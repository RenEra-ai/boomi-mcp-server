"""Load, validate, and render the Boomi docs corpus manifest.json.

Pure stdlib — safe to import without the ML stack installed.
"""
import json
import os

from .errors import KbStartupError

MANIFEST_SCHEMA_VERSION = "1"


def load_manifest(db_path):
    """Load and parse ``<db_path>/manifest.json``.

    Raises KbStartupError if the path, the file, or the JSON is missing or
    malformed (spec §4.3 steps 1-2).
    """
    if not os.path.isdir(db_path):
        raise KbStartupError(
            f"KB corpus path does not exist or is not a directory: {db_path}"
        )

    manifest_path = os.path.join(db_path, "manifest.json")
    if not os.path.isfile(manifest_path):
        raise KbStartupError(f"KB manifest not found: {manifest_path}")

    try:
        with open(manifest_path, encoding="utf-8") as f:
            manifest = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        raise KbStartupError(f"KB manifest is not valid JSON ({manifest_path}): {e}")

    if not isinstance(manifest, dict):
        raise KbStartupError(f"KB manifest must be a JSON object: {manifest_path}")

    return manifest


def validate_manifest(manifest, expected_collection):
    """Validate manifest schema version and collection name (spec §4.3 steps 3-4).

    Raises KbStartupError with a specific message on mismatch.
    """
    schema_version = manifest.get("schema_version")
    if schema_version != MANIFEST_SCHEMA_VERSION:
        raise KbStartupError(
            f"Unsupported KB manifest schema_version {schema_version!r}; "
            f"this server supports {MANIFEST_SCHEMA_VERSION!r}"
        )

    collection_name = manifest.get("collection_name")
    if collection_name != expected_collection:
        raise KbStartupError(
            f"KB manifest collection_name {collection_name!r} does not match "
            f"configured BOOMI_DOCS_COLLECTION {expected_collection!r}"
        )


def corpus_version(manifest):
    """Return the corpus version: artifact_tag when present, else builder_commit."""
    return manifest.get("artifact_tag") or manifest.get("builder_commit") or "unknown"


def render_corpus_resource(manifest):
    """Render the kb://boomi-docs/corpus coverage-map markdown body (spec §5.3)."""
    embedding_model = manifest.get("embedding_model", "unknown")
    embedding_dim = manifest.get("embedding_dim")
    model_line = (
        f"{embedding_model} ({embedding_dim} dim)" if embedding_dim else embedding_model
    )

    category_counts = manifest.get("category_counts", {})
    categories_str = ", ".join(
        f"{name} ({count:,})"
        for name, count in sorted(category_counts.items(), key=lambda kv: -kv[1])
    ) or "unknown"

    sources = ", ".join(manifest.get("source_roots", [])) or "unknown"

    lines = [
        "# Boomi Documentation Corpus",
        "",
        f"- Collection: {manifest.get('collection_name', 'unknown')}",
        f"- Embedding model: {model_line}",
        f"- Build: {manifest.get('build_timestamp', 'unknown')}",
        f"- Corpus version: {corpus_version(manifest)}",
        f"- Sources: {sources}",
        f"- Coverage: {manifest.get('chunk_count', 0):,} chunks across "
        f"{manifest.get('page_count', 0):,} pages",
        f"- Categories: {categories_str}",
        "- Known exclusions: community posts, support tickets, tenant-specific "
        "configuration, and docs published after the build timestamp.",
        f"- Builder commit: {manifest.get('builder_commit') or 'unknown'}",
        "",
        "Use `search_boomi_docs` for factual Boomi questions and "
        "`read_boomi_doc_page(page_key)` for full-page context. Treat search "
        "results as authoritative only for the corpus version and build date "
        "shown above.",
    ]
    return "\n".join(lines)
