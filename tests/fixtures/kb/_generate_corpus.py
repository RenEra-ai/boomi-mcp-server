"""Regenerate tests/fixtures/kb/corpus.jsonl and manifest.json.

Run:  python tests/fixtures/kb/_generate_corpus.py

The fixture corpus is intentionally tiny and human-shaped so it is easy to
review in a diff: two normal pages (one Integration, one EDI) plus a 30-chunk
"big page" for read_boomi_doc_page truncation/pagination tests. Each row mirrors
the producer's chunk schema (chunk_docs.py) so the KB service consumes it
exactly like a production artifact.
"""
import json
import os
from collections import Counter
from datetime import datetime, timezone
from urllib.parse import urlparse

HERE = os.path.dirname(os.path.abspath(__file__))
CORPUS_PATH = os.path.join(HERE, "corpus.jsonl")
MANIFEST_PATH = os.path.join(HERE, "manifest.json")

EMBEDDING_MODEL = "all-MiniLM-L6-v2"
EMBEDDING_DIM = 384
COLLECTION_NAME = "boomi_docs"

# (page_key, title, breadcrumb, category, [(section_heading, content), ...])
PAGES = [
    (
        "https://help.boomi.com/docs/connectors/database",
        "Database Connector",
        "Integration > Connectors > Database Connector",
        "Integration",
        [
            ("Database Connector Overview",
             "The Database connector lets a Boomi process read from and write to "
             "relational databases such as PostgreSQL, MySQL, Oracle, and SQL "
             "Server. It runs on an Atom and uses a JDBC driver to open "
             "connections to the target database."),
            ("Connection Settings",
             "Configure the database connection with the host name, port, "
             "database name, user name, and password. The connection can also "
             "reference a JDBC URL directly when you need driver-specific "
             "options. Store credentials in the connection component, not the "
             "operation."),
            ("Database Operations",
             "The Database connector supports Get, Send, and Update operations. "
             "A Get operation runs a SELECT statement and returns rows as "
             "documents. A Send operation runs INSERT or stored procedures. "
             "Update runs UPDATE or DELETE statements against the target table."),
            ("Connection Pooling",
             "Enable connection pooling on the database connection to reuse "
             "open JDBC connections across process executions. Set the maximum "
             "pool size and the idle timeout so the Atom does not exhaust the "
             "database server connection limit under load."),
            ("Troubleshooting Database Connections",
             "Common database connection errors include invalid credentials, "
             "missing JDBC drivers on the Atom, and network firewall rules "
             "blocking the database port. Check the Atom container logs for the "
             "full JDBC exception stack trace when a connection test fails."),
        ],
    ),
    (
        "https://help.boomi.com/docs/edi/trading-partners",
        "EDI Trading Partners",
        "EDI > Trading Partners",
        "EDI",
        [
            ("Trading Partner Overview",
             "A trading partner component represents an external business "
             "partner that you exchange EDI documents with. It bundles the "
             "communication method, the document standard, and the "
             "partner-specific envelope settings into one reusable component."),
            ("Creating a Trading Partner",
             "Create a trading partner by choosing the standard (X12, EDIFACT, "
             "HL7, and others), then configuring the identifier qualifiers and "
             "the classification as either My Company or Trading Partner. Each "
             "partner needs at least one communication channel."),
            ("Communication Channels",
             "Trading partners exchange documents over AS2, SFTP, FTP, disk, or "
             "HTTP. AS2 is the most common channel for X12 in North America "
             "because it provides signed, encrypted, non-repudiated delivery "
             "with synchronous or asynchronous MDN receipts."),
            ("Document Standards",
             "The X12 standard is widely used in North America while EDIFACT is "
             "common internationally. Each standard defines envelopes, "
             "functional groups, and transaction sets. Boomi validates inbound "
             "documents against the standard before the process runs."),
            ("Trading Partner Troubleshooting",
             "When EDI documents fail, check the envelope identifiers, the "
             "control numbers, and the acknowledgement (997 or CONTRL) status. "
             "A mismatch between the partner's expected qualifier and the "
             "configured qualifier is the most frequent cause of rejected "
             "interchanges."),
        ],
    ),
]

# The 30-chunk "big page" — content is templated; it exists to exercise
# read_boomi_doc_page max_chunks / start_chunk_index / truncation.
BIG_PAGE_KEY = "https://help.boomi.com/docs/processes/build-a-process"
BIG_PAGE_TITLE = "Build a Process"
BIG_PAGE_BREADCRUMB = "Integration > Processes > Build a Process"
BIG_PAGE_CATEGORY = "Integration"
BIG_PAGE_CHUNK_COUNT = 30

# --- Companion (supplemental) page --------------------------------------------
# One small community-sourced page so the fixture exercises the provenance
# surfacing added for the Companion corpus. Its metadata mirrors what the builder
# writes for companion chunks (source_type/verification_status + upstream refs).
# Unlike official pages, source_url is a github.com blob permalink pinned to the
# commit, and page_key is a companion:// identity URI (not a help.boomi.com URL).
COMPANION_REPO = "OfficialBoomi/boomi-integration"
COMPANION_COMMIT = "19aacdd0aa4c9c83f5d87e9b89fb213044447f52"
COMPANION_SOURCE_PATH = "references/components/map_component.md"
COMPANION_PAGE_KEY = (
    "companion://OfficialBoomi/boomi-integration/references/components/"
    "map_component.md"
)
COMPANION_SOURCE_URL = (
    f"https://github.com/{COMPANION_REPO}/blob/{COMPANION_COMMIT}/"
    f"{COMPANION_SOURCE_PATH}"
)
COMPANION_RAW_URL = (
    f"https://raw.githubusercontent.com/{COMPANION_REPO}/{COMPANION_COMMIT}/"
    f"{COMPANION_SOURCE_PATH}"
)
COMPANION_LATEST_URL = (
    f"https://github.com/{COMPANION_REPO}/blob/main/{COMPANION_SOURCE_PATH}"
)
COMPANION_PAGE = (
    COMPANION_PAGE_KEY,
    "Map Component",
    "Companion Reference > Components > Map Component",
    "Companion Reference",
    [
        ("Map Component Overview",
         "The Map component defines how source profile fields are transformed "
         "into destination profile fields. A map connects a source profile to a "
         "destination profile and can apply functions, default values, and "
         "cross-reference lookups as documents pass through the process."),
        ("Adding Function Steps",
         "Drag a function onto the map canvas and wire source fields into its "
         "inputs and its outputs into destination fields. Chain functions to "
         "build multi-step transformations such as string concatenation, date "
         "formatting, or numeric rounding before the value reaches the "
         "destination profile."),
        ("Default Values and Caching",
         "Set a default value on a destination field so the map emits a value "
         "even when the source field is empty. Document caches let a map look up "
         "values captured earlier in the process without re-querying the source "
         "system for every document."),
        ("Map Extensions",
         "Expose a map for environment-specific overrides by enabling map "
         "extensions. An operator can then remap fields or adjust default values "
         "per environment at deployment time without editing the underlying "
         "component in the build tab."),
    ],
)


def _chunk_id(page_key, index):
    slug = page_key.rstrip("/").rsplit("/", 1)[-1].replace("-", "_").replace(".", "_")
    return f"{slug}_{index:03d}"


def _source_path(page_key):
    """Official-doc source_path stand-in: the last URL path segment."""
    return page_key.rstrip("/").rsplit("/", 1)[-1]


def _official_provenance(page_key):
    """Provenance metadata for an official chunk (mirrors build_index.py)."""
    return {
        "source_type": "official",
        "verification_status": "official",
        "upstream_repo": "",
        "upstream_commit": "",
        "source_path": _source_path(page_key),
        "raw_url": "",
        "latest_url": "",
    }


def _companion_provenance():
    """Provenance metadata for a companion chunk (mirrors build_index.py)."""
    return {
        "source_type": "companion_reference",
        "verification_status": "companion_unverified",
        "upstream_repo": COMPANION_REPO,
        "upstream_commit": COMPANION_COMMIT,
        "source_path": COMPANION_SOURCE_PATH,
        "raw_url": COMPANION_RAW_URL,
        "latest_url": COMPANION_LATEST_URL,
    }


def build_chunks():
    chunks = []
    for page_key, title, breadcrumb, category, sections in PAGES:
        for index, (heading, content) in enumerate(sections):
            chunks.append({
                "id": _chunk_id(page_key, index),
                "title": title,
                "section_heading": heading,
                "breadcrumb": breadcrumb,
                "source_url": page_key,
                "page_key": page_key,
                "category": category,
                "content": content,
                "content_html": f"<h2>{heading}</h2><p>{content}</p>",
                "token_estimate": max(1, len(content) // 4),
                "chunk_index": index,
                **_official_provenance(page_key),
            })

    for index in range(BIG_PAGE_CHUNK_COUNT):
        heading = f"Step {index + 1}"
        content = (
            f"Step {index + 1} of building a Boomi process. This section "
            f"describes shape number {index + 1}, how to connect it to the "
            f"previous shape, and which configuration fields are required "
            f"before the process can be deployed and executed on a runtime."
        )
        chunks.append({
            "id": _chunk_id(BIG_PAGE_KEY, index),
            "title": BIG_PAGE_TITLE,
            "section_heading": heading,
            "breadcrumb": BIG_PAGE_BREADCRUMB,
            "source_url": BIG_PAGE_KEY,
            "page_key": BIG_PAGE_KEY,
            "category": BIG_PAGE_CATEGORY,
            "content": content,
            "content_html": f"<h2>{heading}</h2><p>{content}</p>",
            "token_estimate": max(1, len(content) // 4),
            "chunk_index": index,
            **_official_provenance(BIG_PAGE_KEY),
        })

    # Companion (supplemental) page. source_url is the github blob permalink,
    # distinct from the companion:// page_key; provenance flags mark it as
    # community-sourced and unverified.
    page_key, title, breadcrumb, category, sections = COMPANION_PAGE
    for index, (heading, content) in enumerate(sections):
        chunks.append({
            "id": _chunk_id(page_key, index),
            "title": title,
            "section_heading": heading,
            "breadcrumb": breadcrumb,
            "source_url": COMPANION_SOURCE_URL,
            "page_key": page_key,
            "category": category,
            "content": content,
            "content_html": f"<h2>{heading}</h2><p>{content}</p>",
            "token_estimate": max(1, len(content) // 4),
            "chunk_index": index,
            **_companion_provenance(),
        })
    return chunks


def build_manifest(chunks):
    page_keys = {c["page_key"] for c in chunks}
    source_type_counts = dict(
        Counter(c.get("source_type", "official") for c in chunks)
    )
    manifest = {
        "schema_version": "1",
        "collection_name": COLLECTION_NAME,
        "embedding_model": EMBEDDING_MODEL,
        "embedding_dim": EMBEDDING_DIM,
        "build_timestamp": datetime(2026, 5, 1, tzinfo=timezone.utc).isoformat(),
        "chunk_count": len(chunks),
        "page_count": len(page_keys),
        "category_counts": dict(Counter(c["category"] for c in chunks)),
        "source_type_counts": source_type_counts,
        # Derived from chunk source_urls (mirrors build_index.build_manifest), so
        # a companion-inclusive corpus also carries the github.com blob root.
        "source_roots": sorted({
            f"{p.scheme}://{p.netloc}"
            for p in (urlparse(c["source_url"]) for c in chunks if c.get("source_url"))
            if p.scheme and p.netloc
        }) or ["https://help.boomi.com"],
        "artifact_tag": "kb-test",
        "builder_commit": "fixture0",
        "builder_version": "0.1.0",
    }

    companion_chunks = [
        c for c in chunks if c.get("source_type") == "companion_reference"
    ]
    if companion_chunks:
        # area = second breadcrumb segment ("Components", "Steps", ...); count
        # CHUNKS per area, mirroring the builder's build_companion_summary.
        area_counts = dict(Counter(
            parts[1].strip()
            for parts in (c["breadcrumb"].split(" > ") for c in companion_chunks)
            if len(parts) >= 2 and parts[1].strip()
        ))
        file_count = len({c["source_path"] for c in companion_chunks})
        manifest["companion"] = {
            "repo": COMPANION_REPO,
            "commit": COMPANION_COMMIT,
            "file_count": file_count,
            "chunk_count": len(companion_chunks),
            "area_counts": area_counts,
        }
    return manifest


def main():
    chunks = build_chunks()
    with open(CORPUS_PATH, "w", encoding="utf-8") as f:
        for chunk in chunks:
            f.write(json.dumps(chunk, ensure_ascii=False) + "\n")
    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(build_manifest(chunks), f, indent=2)
    print(f"Wrote {len(chunks)} chunks -> {CORPUS_PATH}")
    print(f"Wrote manifest -> {MANIFEST_PATH}")


if __name__ == "__main__":
    main()
