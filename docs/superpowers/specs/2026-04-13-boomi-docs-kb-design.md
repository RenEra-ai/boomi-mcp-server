# Boomi Documentation Knowledge Base — MCP Design Spec

- **Spec date**: 2026-04-13
- **Status**: Approved design, pending implementation plan
- **Supersedes**: `docs/plans/plan_boomi_docs_kb_mcp_resources.json`
- **Repos in scope**:
  - `knowledge-base-builder` (producer — scraping, chunking, indexing, release publishing)
  - `boomi-mcp-server` (consumer — MCP tools and resource surface)

## 1. Purpose

Give the MCP server a bounded, read-only retrieval layer over the Boomi
documentation corpus so Claude can answer product questions with
authoritative text during normal chat, and can pull richer context on
demand without hitting Boomi's website at runtime.

### Use case

**Occasional doc lookup during normal Boomi chat, with self-extraction
when Claude decides it needs more.** The baseline call path is a single
semantic search returning inline chunks; when a hit looks promising but
incomplete, Claude fetches the full source page by URL. Either Claude
reformulates the query or fetches more; no other retrieval path is
required in v1.

### Non-goals (v1)

- No `resources/read`-first retrieval path. `resource_link` output and a
  per-chunk resource template are explicitly dropped.
- No category filter on search. Reformulation is sufficient.
- No hybrid keyword/BM25 search, no rerank, no query expansion.
- No write actions. No subscriptions. No per-user or per-environment
  corpora.
- No answer-synthesis wrapper that hides the underlying sources.
- No corpus build automation inside `boomi-mcp-server`. The app consumes
  a pinned release artifact.

## 2. Architecture

```
                     ┌─────────────────────────────────┐
                     │ knowledge-base-builder (repo)   │
                     │  manual workflow_dispatch       │
                     │  → scrape → chunk → index       │
                     │  → validate → write manifest    │
                     │  → publish GitHub release asset │
                     │     boomi_knowledge_db.tar.gz   │
                     └───────────────┬─────────────────┘
                                     │ pinned release tag
                                     │ (kb-<run_number>)
                                     ▼
                     ┌─────────────────────────────────┐
                     │ boomi-mcp-server (repo)         │
                     │  Docker build downloads the     │
                     │  pinned release and untars to   │
                     │  /app/kb/boomi_knowledge_db/    │
                     │  ML deps installed in image     │
                     │                                 │
                     │  Runtime (BOOMI_DOCS_ENABLED):  │
                     │   - load manifest + validate    │
                     │   - open Chroma collection      │
                     │   - load embedding model        │
                     │   - register:                   │
                     │     tool search_boomi_docs      │
                     │     tool read_boomi_doc_page    │
                     │     resource kb://.../corpus    │
                     └─────────────────────────────────┘
```

### Key design choices and why

- **Tool-first, not resource-first.** Anthropic's own remote connectors
  return content inline; Claude does not reliably do a
  `resources/read` round trip after a `resource_link` in a tool
  response. Keeping retrieval on tools means the common path is a
  single round trip.
- **Two tools, not one.** `search_boomi_docs` is the entrypoint;
  `read_boomi_doc_page` is the self-extraction path. Pages are the
  unit users and Claude both reason about; chunk-level neighbors
  are a leaky abstraction.
- **Manifest is part of the release artifact.** The producer already
  runs `build_index.py` in CI; writing a small `manifest.json` there
  is cheap and keeps contract ownership in the producer.
- **Runtime feature flag.** `BOOMI_DOCS_ENABLED=false` by default. When
  off, no KB modules are imported and no tools or resources register.
  This keeps current deployments unchanged until the operator opts in.

## 3. Producer changes (`knowledge-base-builder`)

### 3.1 `chunk_docs.py`

Add two metadata fields to each emitted chunk dict (currently at
`chunk_docs.py:287-297`):

- `chunk_index` — 0-based integer counter within the source file (same
  value as the numeric tail of the current `id` slug, but stored as a
  first-class integer, not parsed from the ID string).
- `page_title` — copy of `title` (already per-page); exposed explicitly
  so the consumer never has to disambiguate it from `section_heading`.

Keep `source_url` as the page identity. Keep `id` in the current
`{slug}_{nnn}` format — it is the Chroma primary key.

### 3.2 `build_index.py`

1. Add `chunk_index` and `page_title` to the `metadatas` dicts written
   into Chroma at `build_index.py:50-57`.
2. After the index build, write a `manifest.json` beside the Chroma
   directory. Schema v1:

   ```json
   {
     "schema_version": "1",
     "collection_name": "boomi_docs",
     "embedding_model": "all-MiniLM-L6-v2",
     "embedding_dim": 384,
     "build_timestamp": "<ISO-8601 UTC>",
     "chunk_count": <int>,
     "page_count": <int, distinct source_url count>,
     "category_counts": { "<category>": <int>, ... },
     "source_roots": ["<root URL>", ...],
     "builder_commit": "<git sha, short>",
     "builder_version": "<semver or placeholder>"
   }
   ```

3. Strengthen `--verify` so that a named set of smoke queries must each
   return at least one hit with cosine distance below a threshold
   (e.g., `< 0.45`). Non-zero exit on failure. This is the release
   gate.

### 3.3 `.github/workflows/build-knowledge-base.yml`

Current workflow (`build-knowledge-base.yml`) already:

- Runs on `schedule` (weekly) and `workflow_dispatch`.
- Builds the corpus on every run.
- Creates a GitHub release **only** on `workflow_dispatch` (line 188).
- Publishes `boomi_knowledge_db.tar.gz` with tag `kb-<run_number>`.

Changes:

- Run the strengthened `build_index.py --verify` before the release
  step and abort on failure.
- Include `manifest.json` inside the tarball (it's already written into
  the output dir by `build_index.py`, so the existing `tar -czf
  boomi_knowledge_db.tar.gz boomi_knowledge_db/` picks it up).
- Add to release notes: build timestamp, chunk count, page count,
  embedding model, top 5 categories by chunk count, count of failed
  source URLs.
- Keep scheduled weekly builds as preview/health-check only (no release).

## 4. Consumer changes (`boomi-mcp-server`)

### 4.1 Environment variables

| Name | Default | Purpose |
|---|---|---|
| `BOOMI_DOCS_ENABLED` | `false` | Master switch. When false, no KB imports, no tool/resource registration. |
| `BOOMI_DOCS_DB_PATH` | `/app/kb/boomi_knowledge_db` | Local path to the Chroma persistent dir. `manifest.json` is expected at `<path>/manifest.json`. |
| `BOOMI_DOCS_COLLECTION` | `boomi_docs` | Must match `manifest.collection_name` or startup fails. |
| `BOOMI_DOCS_TOP_K_DEFAULT` | `5` | Default `top_k` for `search_boomi_docs`. |
| `BOOMI_DOCS_TOP_K_MAX` | `10` | Upper bound enforced on incoming `top_k`. |

No `BOOMI_DOCS_PREVIEW_CHARS` — chunks are returned whole (see §5.1).

### 4.2 KB service module

New module at `src/boomi_mcp/kb/` with three submodules:

- `manifest.py` — load, validate schema version, compare to env config.
- `service.py` — singleton-ish KB service: Chroma client, collection
  handle, embedding function, `search()` and `read_page()` methods.
- `errors.py` — narrow exception types (`KbStartupError`,
  `KbQueryError`).

All imports of `chromadb` and `sentence_transformers` live inside
`src/boomi_mcp/kb/` and are executed only when
`BOOMI_DOCS_ENABLED=true`. Top-level `server.py` imports from
`boomi_mcp.kb` behind the flag check.

### 4.3 Startup validation

Run in this order during `server.py` bootstrap, only if
`BOOMI_DOCS_ENABLED=true`:

1. `BOOMI_DOCS_DB_PATH` exists and is readable.
2. `<DB_PATH>/manifest.json` exists and parses as JSON.
3. `manifest.schema_version == "1"` (reject unknown versions).
4. `manifest.collection_name == BOOMI_DOCS_COLLECTION`.
5. Chroma `PersistentClient(path=DB_PATH)` opens.
6. `client.get_collection(name=BOOMI_DOCS_COLLECTION)` succeeds.
7. `collection.count() == manifest.chunk_count` (exact — guards
   against incomplete artifacts).
8. Instantiate `SentenceTransformerEmbeddingFunction(
   model_name=manifest.embedding_model)`. This downloads the model on
   first run; the Docker image warms the cache at build time.
9. Run one probe query (`collection.query(query_texts=["probe"],
   n_results=1)`). Non-empty result required.

Any failure raises `KbStartupError` with a clear message and exits the
process. Consistent with existing patterns in `server.py:55, 67, 80`.

### 4.4 Runtime model

The KB service is constructed once during startup and stored on the
FastMCP server instance. Tools close over it. No per-request client or
model loading.

## 5. Public API

### 5.1 Tool: `search_boomi_docs`

**Signature**

```python
@mcp.tool(annotations={"readOnlyHint": True, "openWorldHint": False})
def search_boomi_docs(query: str, top_k: int = 5) -> dict:
    ...
```

**Description (tool-level, Claude-facing)**

> Search the Boomi documentation knowledge base by semantic similarity
> and return the top-ranked chunks with inline content. Use this as
> the primary entrypoint when the user asks a question about Boomi —
> how a feature works, how to configure a connector, what a term
> means, what an error code implies, or when you need authoritative
> product reference material before suggesting a change or generating
> a plan. Chunks are returned in full, so the top result is usually
> enough to answer directly. If a hit looks relevant but you need the
> surrounding context on that page, follow up by calling
> `read_boomi_doc_page` with that hit's `source_url`. If results look
> off-topic, retry with reformulated query terms. Read-only.

**Input validation**

- `query`: required, non-empty after trim. On empty: return structured
  error, no raise.
- `top_k`: clamp to `[1, BOOMI_DOCS_TOP_K_MAX]`. Default from env.

**Output (structured content)**

```json
{
  "query": "<echo of trimmed query>",
  "top_k": <effective int>,
  "hits": [
    {
      "chunk_id": "<slug_nnn>",
      "title": "<page title>",
      "section_heading": "<heading>",
      "breadcrumb": "<path>",
      "source_url": "<https://help.boomi.com/...>",
      "category": "<category>",
      "chunk_index": <int>,
      "token_estimate": <int>,
      "distance": <float>,
      "content": "<full plain-text chunk body>"
    }
  ]
}
```

**Ranking and diversification**

Hits are sorted ascending by `distance` (cosine — the collection is
built with `metadata={"hnsw:space": "cosine"}` in
`knowledge-base-builder/build_index.py:177`).

To keep `top_k` slots useful for the "occasional lookup" use case, v1
diversifies across pages: a single page cannot dominate the result
list while other relevant pages exist. Algorithm:

1. Over-fetch candidates from Chroma: `n_results = top_k * 3` (capped
   at a sane ceiling, e.g., `30`).
2. Group candidates by `source_url`, each group sorted ascending by
   `distance`.
3. Round-robin across groups, taking one best-remaining chunk per
   `source_url` per pass, until `top_k` hits are collected or all
   candidates are exhausted.
4. Preserve overall distance order in the final output.

Net effect: if the over-fetched set contains 5+ distinct pages, each
of the first 5 hits comes from a different page. If fewer distinct
pages exist, the remaining slots fall back to next-best same-page
chunks so the tool still returns `top_k` hits when the corpus has
enough material. Claude can always follow up with
`read_boomi_doc_page(source_url)` to get the full page for any hit.

**Text fallback**: JSON-pretty-printed version of the same object for
clients that don't render `structuredContent`.

**Token budget**: with chunks 100–1200 tokens and `top_k` ≤ 10, the
tool result is bounded at ~12 k content tokens + ~1 k metadata, well
within Claude context and remote connector limits. No truncation in v1.

### 5.2 Tool: `read_boomi_doc_page`

**Signature**

```python
@mcp.tool(annotations={"readOnlyHint": True, "openWorldHint": False})
def read_boomi_doc_page(
    source_url: str,
    max_chunks: int = 15,
    start_chunk_index: int = 0,
) -> dict:
    ...
```

**Description**

> Return chunks of the Boomi documentation page for a given source URL
> in `chunk_index` order. Use this after `search_boomi_docs` when a
> search hit looks relevant but you need the surrounding page context
> — prerequisites, step sequences, or sections adjacent to the matched
> chunk. Most Boomi doc pages fit in a single call (default 15
> chunks); if the response has `"truncated": true`, call again with
> `start_chunk_index` set to the returned `next_chunk_index` to
> continue. Read-only.

**Input validation**

- `source_url`: required, non-empty after trim.
- `max_chunks`: clamp to `[1, 30]`. Default `15` covers virtually
  every page in the Boomi corpus (typical 3–10 chunks per page).
- `start_chunk_index`: clamp to `>= 0`. If it exceeds the page's
  `chunk_count`, return zero chunks with `truncated=false` and
  `chunk_count` intact so Claude can tell it has run off the end.

**Behavior**

1. Call `collection.get(where={"source_url": source_url},
   include=["documents", "metadatas"])` — a metadata-filtered read,
   not a vector query, so no embedding is computed.
2. Sort returned items by `metadatas[i]["chunk_index"]` ascending.
3. Slice: take items whose `chunk_index >= start_chunk_index`, up to
   `max_chunks` of them.
4. Compute `truncated = (start_chunk_index + len(slice)) < chunk_count`.
   When `truncated`, set `next_chunk_index = start_chunk_index +
   len(slice)`; otherwise omit.

**Output**

```json
{
  "source_url": "<...>",
  "title": "<page title>",
  "breadcrumb": "<path>",
  "category": "<category>",
  "chunk_count": <int, total chunks on this page>,
  "start_chunk_index": <int, echo of input>,
  "chunks_returned": <int, length of chunks array>,
  "truncated": <bool>,
  "next_chunk_index": <int, present only when truncated=true>,
  "chunks": [
    {
      "chunk_id": "<...>",
      "section_heading": "<...>",
      "chunk_index": <int>,
      "content": "<plain text>"
    }
  ]
}
```

**Errors**

- Unknown `source_url` (zero chunks): structured error `{"error":
  "no_chunks_for_source_url", "source_url": "..."}`. No raise.
- Non-contiguous `chunk_index` (gap detected in the underlying
  corpus): log warning, return the slice as-is. Do not fail the call.
  `chunk_count` reflects actual returned-group size.

**Token budget**: default cap of 15 chunks × 1200-token ceiling = ~18 k
tokens worst case, typically much lower (3–10 chunks at ~500–700
tokens average ≈ 2–7 k). Large pages stay reliable on remote MCP
connectors, and outlier pages are still fully reachable via repeated
calls with `start_chunk_index`.

### 5.3 Resource: `kb://boomi-docs/corpus`

Static, not on the retrieval path. Exists so MCP clients listing
resources see a single stable entry and operators can eyeball build
state.

**Body** (text/markdown):

```
# Boomi Documentation Corpus

- Collection: boomi_docs
- Embedding model: all-MiniLM-L6-v2 (384 dim)
- Build: 2026-04-13T06:12:44Z
- Chunks: 4,821 across 1,203 pages
- Categories: Integration (3,400), API Management (720), ...
- Builder commit: abc1234

Search via the `search_boomi_docs` tool; fetch a full page with
`read_boomi_doc_page(source_url)`.
```

Rendered from `manifest.json` at registration time. No dynamic fields.

`resources/list` returns exactly one entry (this one). No chunk
templates.

## 6. Error handling

- **Startup errors**: raise `KbStartupError`, log, `sys.exit(1)`.
  Matches existing pattern.
- **Tool-level errors**: catch, return structured `{"error": "<code>",
  "message": "<human string>"}`. Never raise out of a tool.
- **Chroma query failures**: wrap in `KbQueryError`, convert to
  structured error in the tool layer.
- **No silent fallbacks**. If search cannot run, the tool says so.

## 7. Deployment

### 7.1 Image build

- Add to `requirements.txt` (or a new `requirements-kb.txt` installed
  unconditionally in v1): `chromadb`, `sentence-transformers`.
- Add a Docker build arg `KB_RELEASE_TAG`, **optional and empty by
  default**. The corpus download and model pre-warm steps both run
  only when the arg is a non-empty string. This keeps non-KB builds
  (the current default) identical in shape to today's build.

  ```dockerfile
  ARG KB_RELEASE_TAG=""

  # Corpus: downloaded and extracted only when a release tag is provided.
  # When unset, the image ships with no /app/kb contents and runtime must
  # keep BOOMI_DOCS_ENABLED=false, or startup will fail fast.
  RUN if [ -n "$KB_RELEASE_TAG" ]; then \
        mkdir -p /app/kb && \
        curl -fsSL -o /tmp/kb.tgz \
          "https://github.com/RenEra-ai/knowledge-base-builder/releases/download/${KB_RELEASE_TAG}/boomi_knowledge_db.tar.gz" && \
        tar -xzf /tmp/kb.tgz -C /app/kb && \
        rm /tmp/kb.tgz; \
      fi

  # Pre-warm sentence-transformers model cache only when building a
  # KB-enabled image, so non-KB builds don't pay for a ~90 MB model
  # download they won't use.
  RUN if [ -n "$KB_RELEASE_TAG" ]; then \
        python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('all-MiniLM-L6-v2')"; \
      fi
  ```

- **Build contract**:
  - Non-KB build (current default): invoke `docker build` with no
    extra args. `KB_RELEASE_TAG=""`, both conditional RUN steps are
    no-ops, image is unchanged in shape (though ~1–1.5 GB larger from
    always-installed ML deps — see §7.3). Runtime must keep
    `BOOMI_DOCS_ENABLED=false`; if set to true without a corpus,
    startup fails with a clear message (§4.3 step 1).
  - KB-enabled build: `docker build --build-arg KB_RELEASE_TAG=kb-42 …`.
    Both RUN steps fire, corpus lands at `/app/kb/boomi_knowledge_db`,
    model cache is warmed. Default `BOOMI_DOCS_ENABLED` stays `false`
    at the image layer; the k8s ConfigMap (§7.2) flips it per
    environment.

### 7.2 Kubernetes manifest

Update `k8s/deployment.yaml:123-129`:

```yaml
resources:
  requests:
    cpu: 250m
    memory: 1Gi
  limits:
    cpu: 1000m
    memory: 2Gi
```

Add env entries:

```yaml
- name: BOOMI_DOCS_ENABLED
  valueFrom: { configMapKeyRef: { name: boomi-mcp-config, key: boomi_docs_enabled, optional: true } }
- name: BOOMI_DOCS_DB_PATH
  value: "/app/kb/boomi_knowledge_db"
```

Default configmap key to `"false"` until operators opt in.

### 7.3 Image size

ML deps add ~1–1.5 GB. Chroma artifact adds 50–200 MB (unknown until
first full build). Acceptable for v1. Future optimization: split into
two image variants only if pull time becomes a real problem.

## 8. Test plan

New test files live at `tests/kb/` (keeps them grouped and easy to
skip with a marker when `BOOMI_DOCS_ENABLED` is not exercised).

### 8.1 Testing layers (matches this repo's existing two-layer pattern)

The repo tests both the internal handler and the exported server
wrapper. Existing wrapper tests import `server` and call exported
functions directly (for example, `server.manage_process(...)` and
`server.execute_process(...)`), even when legacy docstrings use stale
entrypoint wording. The KB tests follow the same split:

**Handler-layer (direct-call) tests — the dominant pattern.** Call
`boomi_mcp.kb.service.KbService` methods and helpers directly with a
real tiny fixture corpus. No FastMCP involved.

- `test_service_search.py`: correct fields, respects `top_k`, clamps
  to max, handles zero-hit queries, rejects empty query with
  structured error, **per-page diversification behavior** (§5.1).
- `test_service_page.py`: correct order by `chunk_index`, unknown
  `source_url`, `max_chunks` / `start_chunk_index` behavior, the
  `truncated` / `next_chunk_index` fields, non-contiguous indices
  logged but not raised.
- `test_service_startup.py`: manifest validation matrix (valid,
  missing, bad schema version, collection mismatch, model mismatch),
  chunk-count mismatch rejected.

**MCP-surface wrapper tests — contract for the exported server layer.**
Mirror the executable pattern in `test_manage_process_bug28.py` /
`test_execute_process_bug24.py`: import `server`, patch auth / SDK /
service dependencies, and call the exported wrapper functions directly
to assert the public shape, input coercion, and error surface.

- `test_tools_surface.py`:
  - `server.search_boomi_docs(query="x")` returns the documented
    structured output shape.
  - Annotations on the tool object include
    `readOnlyHint=True, openWorldHint=False`.
  - `server.read_boomi_doc_page(source_url="...")` returns documented
    shape; `server.read_boomi_doc_page(source_url="...", max_chunks=2)`
    honors the cap and sets `truncated=true`.
  - Empty-query / unknown-URL paths return structured errors, not
    exceptions.
- `test_resource.py`:
  - `resources/list` returns exactly one entry.
  - `resources/read kb://boomi-docs/corpus` returns the documented
    metadata body.
  - No chunk template registered.

**Startup integration**

- `test_startup.py`: valid corpus boots; corrupt manifest fails with
  the expected message; `BOOMI_DOCS_ENABLED=false` boots clean and
  `chromadb` / `sentence_transformers` are not imported (assert via
  `sys.modules`).

### 8.2 Fixture corpus

Build a tiny corpus once per session (10 chunks, 3 distinct
`source_url`s, 2 categories, a deliberate 30-chunk "big page" for the
`max_chunks` truncation test). Use the real `build_index.py` flow so
the fixture is shaped exactly like production artifacts, and write a
matching `manifest.json`. Check the input JSONL into
`tests/fixtures/kb/` and let a pytest fixture build the Chroma dir to a
`tmp_path` on first use.

### 8.3 Regression

Run the existing repo test suite after KB work lands; no changes to
existing tools' behavior expected.

### 8.4 QA validation (per CLAUDE.md)

After handler-layer and MCP-surface tests pass, run the
`boomi-qa-tester` agent against a live `BOOMI_DOCS_ENABLED=true`
server to validate the tools end-to-end through the real MCP server, or
through direct exported wrapper calls when running in-process. Repeat
until the agent reports zero issues before reporting the task complete.

## 9. Observability

- Startup logs: KB enabled, collection name, model name, chunk count,
  manifest build timestamp. One summary line, plus `[ERROR]` lines on
  any validation failure before the exit.
- Query logs: `INFO` level with query string (trimmed length, not the
  full text if privacy becomes a concern — v1 logs the full trimmed
  query), `top_k`, result count, min/max distance. Behind a standard
  log level; no new telemetry sinks in v1.

## 10. Open questions deferred past v1

Not blockers for the spec; listed so we don't lose them.

- Hybrid BM25 + vector search for exact-term queries (API names,
  error codes like `PROC-0012`).
- Rerank via a cross-encoder for quality.
- Tunable diversification factor (v1 hardcodes the over-fetch
  multiplier).
- Category filter — only if reformulation proves insufficient.
- Two image variants (with / without ML deps) if image size hurts.
- Subscriptions / live updates — only if corpus cadence changes.

## 11. Acceptance criteria

- Producer writes a spec-compliant `manifest.json`, bundles it into
  the existing release tarball, and fails the release on verification
  regression.
- Non-KB Docker builds (the current default, `KB_RELEASE_TAG` unset)
  succeed without touching GitHub releases or the sentence-transformers
  cache, and run identically to today with `BOOMI_DOCS_ENABLED=false`.
- KB-enabled Docker builds (`--build-arg KB_RELEASE_TAG=kb-<n>`)
  produce an image with the corpus at `/app/kb/boomi_knowledge_db` and
  the model cache warmed.
- Consumer starts clean with `BOOMI_DOCS_ENABLED=false`; starts with
  two tools and one resource registered when `BOOMI_DOCS_ENABLED=true`
  and a valid corpus is mounted; fails fast with a clear message in
  any other combination.
- `search_boomi_docs` returns bounded ranked hits with full chunk
  content, stable metadata, and page-diversified ordering when ≥
  `top_k` distinct pages are available.
- `read_boomi_doc_page` returns chunks in `chunk_index` order, honors
  `max_chunks` and `start_chunk_index`, and emits `truncated` /
  `next_chunk_index` correctly for large pages.
- `resources/list` returns exactly one entry (`kb://boomi-docs/corpus`).
- All new retrieval interfaces are read-only, size-bounded, and
  explicitly annotated for remote-connector use.
- Kubernetes memory request/limit raised to 1 Gi / 2 Gi.
- Handler-layer tests, exported-wrapper MCP-surface tests, and QA-agent
  validation all pass.
