"""Vendored copy of the producer chunk-validation contract.

MIRROR of knowledge-base-builder/build_index.py (REQUIRED_CHUNK_FIELDS,
PROVENANCE_REAL_VALUE_FIELDS, validate_chunks) and companion.py (the two
*_SOURCE_TYPE labels). The KB test fixture (tests/fixtures/kb/corpus.jsonl) is a
hand-authored mirror of what that builder emits; this file lets the MCP repo
assert the fixture satisfies the producer contract without the builder installed.

Keep in sync with the builder. ``test_producer_contract.py`` machine-enforces
equality with the live builder (``test_vendored_contract_matches_builder``)
whenever the builder source is present, so a drift here fails on any dev / QA
machine that has the sibling checkout.

Not a test module (underscore-prefixed) — pytest does not collect it.
"""
import os
from pathlib import Path

# --- vendored provenance labels (companion.py:24,30) --------------------------
COMPANION_SOURCE_TYPE = "companion_reference"
OFFICIAL_SOURCE_TYPE = "official"

# --- vendored chunk contract (build_index.py:45-49,60-62) ---------------------
# Fields every chunk must carry once chunk_docs.py has run. source_type and
# verification_status are the load-bearing provenance labels.
REQUIRED_CHUNK_FIELDS = {
    "id", "content", "title", "section_heading", "breadcrumb",
    "source_url", "page_key", "chunk_index", "category", "token_estimate",
    "source_type", "verification_status",
}

# The five extended provenance fields that companion chunks must populate with
# real values and official chunks must leave blank.
PROVENANCE_REAL_VALUE_FIELDS = (
    "upstream_repo", "upstream_commit", "source_path", "raw_url", "latest_url",
)


def validate_chunks(chunks):
    """Validate every chunk against the producer contract.

    Checks the plan's required invariants for *all* chunks, not just the first:
    required fields present; non-empty string id/page_key/content; integer
    chunk_index; and contiguous 0-based chunk_index within each page_key.
    Returns a list of human-readable error strings (empty == release-quality).
    """
    errors = []
    for i, chunk in enumerate(chunks):
        if not isinstance(chunk, dict):
            errors.append(f"chunk {i} is not a JSON object: {type(chunk).__name__}")
            continue
        missing = REQUIRED_CHUNK_FIELDS - set(chunk)
        if missing:
            errors.append(f"chunk {i} missing required field(s): {sorted(missing)}")
            continue
        for field in ("id", "page_key", "content"):
            value = chunk[field]
            if not isinstance(value, str) or not value.strip():
                errors.append(f"chunk {i} ({chunk.get('id')!r}) has empty/non-string {field!r}")
        idx = chunk["chunk_index"]
        if not isinstance(idx, int) or isinstance(idx, bool):
            errors.append(f"chunk {i} ({chunk.get('id')!r}) has non-integer chunk_index: {idx!r}")

        # Provenance contract: companion chunks must carry real attribution;
        # official chunks must leave the five extended fields blank. Only these
        # two source_types are validated, so official-only corpora and any future
        # type stay backward-compatible.
        stype = chunk.get("source_type")
        if stype == COMPANION_SOURCE_TYPE:
            for field in PROVENANCE_REAL_VALUE_FIELDS:
                value = chunk.get(field)
                if not isinstance(value, str) or not value.strip():
                    errors.append(
                        f"chunk {i} ({chunk.get('id')!r}) companion chunk has empty {field!r}"
                    )
        elif stype == OFFICIAL_SOURCE_TYPE:
            for field in PROVENANCE_REAL_VALUE_FIELDS:
                # Must be exactly the empty string (missing key -> "" is fine).
                # A present None/0 is falsey but would reach the Chroma metadata
                # write as a non-string, so reject it here instead of passing it.
                if chunk.get(field, "") != "":
                    errors.append(
                        f"chunk {i} ({chunk.get('id')!r}) official chunk must leave {field!r} blank"
                    )

    by_page = {}
    for chunk in chunks:
        if not isinstance(chunk, dict):
            continue
        page_key = chunk.get("page_key")
        idx = chunk.get("chunk_index")
        if isinstance(page_key, str) and page_key and isinstance(idx, int) and not isinstance(idx, bool):
            by_page.setdefault(page_key, []).append(idx)
    for page_key, indices in sorted(by_page.items()):
        if sorted(indices) != list(range(len(indices))):
            errors.append(
                f"page_key {page_key!r} has non-contiguous chunk_index: {sorted(indices)}"
            )

    return errors


def _resolve_builder(directory):
    """Return ``directory`` if it holds a build_index.py, else None."""
    if directory and os.path.isfile(os.path.join(directory, "build_index.py")):
        return directory
    return None


def find_builder_dir():
    """Return the knowledge-base-builder source dir if discoverable, else None.

    An explicit $KB_BUILDER_PATH is authoritative: it is resolved on its own with
    no fallback, so a bogus value force-skips the parity tests (used both to point
    at a specific checkout and to simulate the builder being absent). When unset,
    default to the sibling of the repo root (../knowledge-base-builder). Requires
    build_index.py to actually be present so a stale dir skips cleanly rather than
    raising on import.
    """
    env = os.environ.get("KB_BUILDER_PATH")
    if env:
        return _resolve_builder(env)
    # tests/kb/_producer_contract.py -> tests/kb -> tests -> repo root
    repo_root = Path(__file__).resolve().parents[2]
    return _resolve_builder(str(repo_root.parent / "knowledge-base-builder"))
