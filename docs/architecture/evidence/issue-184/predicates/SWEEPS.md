# Pre-implementation sweeps (issue #184)

Both sweeps ran in scratch `git worktree`s detached at the branch point
`cbab28ffc176ddb2378283cf7132eb9c37afc374`, never in the working tree, with the full non-KB suite
(`python -m pytest tests --ignore=tests/kb -p no:cacheprovider -q`, `.venv` 3.12, `PYTHONPATH=src`).
Each patch was reverted with `git checkout --` afterwards and the worktree confirmed clean.

## A5 — general document-property invalidation at stream-replacing reads

Patch in `src/boomi_mcp/compiler/process_ir/semantic_validation/lineage.py`, inside `_visit`, directly
after `on_documents = frozenset(established_here)` in the block guarded by
`_discards_document_properties(semantic)`:

    state = _State(frozenset(k for k in state.document if k in established_here), state.execution)

Suite: `12298 passed, 19 skipped, 19 warnings in 1037.97s (0:17:17)`, exit 0.

Non-vacuity witness (re-applied patch, `probe_predicates.py` against the patched worktree):

    {"form": "stale_ddp_after_cacheget", "parse": "ok", "compile": [["PROCESS_IR_SEMANTIC_LINEAGE_DDP_SCOPE_INVALID", "/body/steps/4", "semantic_lowering"]]}
    {"form": "ref_staging_skeleton", "parse": "ok", "compile": "ok+emit"}

At the branch point the same stale-read form compiles and emits (`probe_cbab28f.jsonl`).

## A4 — absent-stream refusal for map and cache-put consumers

Patch in `src/boomi_mcp/compiler/process_ir/connector_resolution.py`: the `if not bindings: return`
early exit of `validate_connector_call_semantics` removed, and in `_walk_paths`, before the
`connector_call` branch:

    if kind in ("map", "cache_put") and state.producer is None:
        _cardinality_failure(node.source_path, node.node_id)

Suite: `12298 passed, 19 skipped, 19 warnings in 1046.30s (0:17:26)`, exit 0.

Non-vacuity witness:

    {"form": "absent_stream_maps_in_legs", "parse": "ok", "compile": [["PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH", "/body/steps/0/legs/1/steps/0", "semantic_lowering"]]}
    {"form": "absent_stream_map_put", "parse": "ok", "compile": [["PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH", "/body/steps/0/legs/0/steps/0", "semantic_lowering"]]}
    {"form": "ref_staging_skeleton", "parse": "ok", "compile": "ok+emit"}

At the branch point both absent-stream forms compile and emit (`probe_cbab28f.jsonl`).

The suite logs themselves (pytest progress output, several megabytes) are not archived; the summary
lines above are quoted verbatim from them.
