# DDP survival by node kind — LIVE class evidence (account traininghlibbochkarov-JKIY2X)

Measured 2026-08-26 on the wire (bound REST GET whose Path is the DDP; unique key per
cell; per-execution relative log window). Every execution COMPLETE — no cell errored.
Keyed by NODE KIND and emitted platform shape, not by component id, so it outlives the account.

| IR node kind | step | emitted platform shape(s) | docs out | DDP reached the wire |
|---|---|---|---|---|
| (none — control) | — | `connectoraction`,`documentproperties`,`connectoraction` | 2 | **SURVIVES** |
| `message` | — | `message` | 2 | **SURVIVES** |
| `data_process` | `split_documents` | `dataprocess` | 1 | **LOST** |
| `data_process` | `custom_scripting` (emits a fresh doc w/ empty Properties) | `dataprocess` | 1→2 | **SURVIVES** |
| `cache_get` | — | `doccacheload` → `doccacheretrieve` | 1 | **LOST** |
| `document_cache_retrieve` | — | `doccacheload` → `doccacheretrieve` | 1 | **LOST** |

Invariant observed in every cell: DDP survives ⟺ 2 outbound documents ⟺ the bound call
issued a request. When the property is lost the bound call produces no document and
issues no HTTP request at all.

## Structural conclusions

1. **`cache_get` and `document_cache_retrieve` emit the SAME platform shapes** — two IR
   spellings of one runtime step. They cannot differ, and both lose the property.
2. **`data_process` gives BOTH answers depending on its step.** Survival is not a
   property of that node kind at all.
3. **Only `message` among the replacing kinds preserves the property.**

⇒ "does this discard document properties" is NOT a function of the node kind. It is a
function of (kind, step operation, and for custom scripting, the script itself).

## Caveats, recorded rather than inferred
- The custom_scripting cell used ONE script, which stored a brand-new stream with an
  empty `java.util.Properties` — and the DDP still survived. Whether a different script
  can drop it is UNMEASURED.
- `combine_documents` was not exercised.
- A `cache_get` from a never-written cache is not authorable
  (`PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING`; `empty_cache_behavior` is a
  `stopprocess`-only constant).
