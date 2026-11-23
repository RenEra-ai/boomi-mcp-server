# Issue #159 fixture provenance

Every golden this issue adds records the CAUSALLY INDEPENDENT source its bytes come from
(CLAUDE.md Stage-1 step 2, amended 2026-08-14). Nothing here is a render of the code #159
changes. This note lives here rather than beside the XML because the golden directory
admits only declared `.xml` files (`scripts/wave_gate.py::check_golden_tree`).

## `trycatch_dlq:notify_document_cache_connection_override` — the `ConnectionOverride` survivor (S2)

| Field | Value |
| --- | --- |
| Expected file | `tests/fixtures/golden_xml/issue159_cache_notify_connection_override.xml` |
| Manifest row | appended by the integrator (`golden-000089` if nothing is appended first), owner `repository`, disposition `survivor`, renderer `process-component-v1` |
| Provenance class | **recorded byte transform of frozen goldens** that predate the #159 S2 lanes' base `ed17dde` |
| Shape source | `tests/fixtures/golden_xml/issue184_cache_notify_terminal.xml` (golden-000087), sha256 `dd29439ea851a49d8280d14588eea36a8a622b8f06564ffef65bf9fb8dc5ca2c` (3520 bytes). Its own provenance is in `tests/fixtures/process_ir/issue184/PROVENANCE.md` (a byte transform of legacy-builder output frozen before #156) |
| Extension source | the `<bns:processOverrides>…</bns:processOverrides>` element of `tests/fixtures/golden_xml/issue184_cache_notify_archetype_terminal.xml` (golden-000088), sha256 `4845f0319ac55e93cdaa03b91a72d135d914594d262786b1ba2b40c52a133cab` (5826 bytes) |
| Extension cross-check | the same element in `tests/fixtures/golden_xml/try_catch_dlq_document_cache_archetype.xml` (golden-000056), sha256 `3a872dc87065c625ea044d4b6334300d1a880079f26bfba1e5679d3bcda7a5c4` (4412 bytes), is byte-identical — the two retiring goldens carry ONE extension shape, so one survivor covers both |
| Field authority | `tests/fixtures/process_overrides/database_connection_fields_process_overrides.xml` (#92): the `username`/`password` field ids, labels (`User`, `Password`) and XPaths (`DatabaseConnectionSettings/@username`, `…/@password`) transcribed from a UI-authored platform exemplar |
| Transform | cut the one `<bns:processOverrides>` element from each extension source (asserted exactly once each, and equal); assert golden-000087 contains exactly one `processOverrides` token, the empty `<bns:processOverrides/>`; replace it with the cut element. Nothing else moves: name `TryCatch Notify DLQ Golden`, folder `Golden/Fixtures`, every shape |
| Cut element | sha256 `26f81ca78c9076380afbd3816d821444a9f6fb6448208202608218ff161fcc48` (626 bytes): one `ConnectionOverride id="11111111-1111-1111-1111-111111111111"` (the DB connection both sources bind) with the two XPath-keyed fields |
| Result sha256 | `36b3e46e83472ca859cbe735ed38f1e5bbeeac6f5bfbf92e3da5823582cad41e` (4123 bytes) |
| Transform script | performed by a stdlib-only script (no `boomi_mcp` import) that asserts the three source hashes above before cutting; its body is reproduced below |
| Rendered by | `tests/_wave_gate_golden_corpus.py::_case_try_catch_notify_dlq_connection_override` → golden-000087's canonical route (`plan_authoring_request_v1` → `build_materialization_plan` → `materialize_canonical_process_xml`) with `NOTIFY_DLQ_CONNECTION_OVERRIDE_EXTENSIONS` as the unit envelope's typed `process_extensions`, connection `$ref:DBCONN` late-bound to `11111111-…` |

**Never regenerated from the compiler.** The canonical render was compared with these bytes
after they were written and matched byte-for-byte. The render is the thing under test; the
transform of frozen bytes is the oracle.

**Why keep golden-000087's name and folder.** With the shapes and envelope unchanged, the only
difference between the two files is the extension block. The negative control in
`tests/test_wave_gate_goldens.py` uses this: removing the typed binding must reproduce
golden-000087 exactly, and changing one binding must break parity with this file.

**What it replaces.** S3 deletes golden-000056 (`archetype_dlq:document_cache`) and
golden-000088 (`archetype_dlq:notify_document_cache_terminal`), which render through the
retiring archetype path. After S3 this file is the corpus's only `ConnectionOverride` oracle.
The transform's sources are the retiring files themselves, so their hashes above are the
durable record of what was cut once they are gone.

```python
import hashlib, re
G = "tests/fixtures/golden_xml/"
SRC_SHAPES = G + "issue184_cache_notify_terminal.xml"              # golden-000087
SRC_BLOCKS = (G + "try_catch_dlq_document_cache_archetype.xml",    # golden-000056
              G + "issue184_cache_notify_archetype_terminal.xml")  # golden-000088
OUT = G + "issue159_cache_notify_connection_override.xml"
EXPECT = {
    SRC_SHAPES: "dd29439ea851a49d8280d14588eea36a8a622b8f06564ffef65bf9fb8dc5ca2c",
    SRC_BLOCKS[0]: "3a872dc87065c625ea044d4b6334300d1a880079f26bfba1e5679d3bcda7a5c4",
    SRC_BLOCKS[1]: "4845f0319ac55e93cdaa03b91a72d135d914594d262786b1ba2b40c52a133cab",
}
raw = {p: open(p, "rb").read() for p in EXPECT}
for p, h in EXPECT.items():
    assert hashlib.sha256(raw[p]).hexdigest() == h, p
pat = re.compile(rb"<bns:processOverrides>.*?</bns:processOverrides>", re.S)
blocks = []
for p in SRC_BLOCKS:
    found = pat.findall(raw[p])
    assert len(found) == 1, p
    blocks.append(found[0])
assert blocks[0] == blocks[1], "the two retiring blocks differ"
block = blocks[0]
assert block.count(b"<ConnectionOverride ") == 1
slot = b"<bns:processOverrides/>"
base = raw[SRC_SHAPES]
assert base.count(slot) == 1 and base.count(b"processOverrides") == 1
out = base.replace(slot, block)
assert out.count(block) == 1
open(OUT, "wb").write(out)
```

## `recipe:*` renderer migrations — golden-000001, golden-000003, golden-000004 (S2)

The three renderers no longer route through `patterns/recipe_bridge.py`. Their case keys, golden
ids, `process-xml-v1` artifact kind and frozen expected bytes are unchanged. No expected file
was touched, and the frozen bytes are what the new renderers are compared against. Each one
renders through the public typed plan (`plan_authoring_request_v1`, offline, errors refused). The
symbol table is rebound to `placeholder_component_id` (the `id-<key>` spellings the frozen files
carry) by `placeholder_backed_symbols`. Then `compile_process_ir_v1` → `emit_process(...).process_xml`.

| Golden | Case | New route | Frozen inputs and their source |
| --- | --- | --- | --- |
| golden-000004 | `recipe:compose_stream` | pinned `boomi.compose.db_rest_fanout@2.0.0` | `FANOUT_V2_GOLDEN_RAW_INPUT` + `FANOUT_V2_GOLDEN_MATERIAL` in the corpus. Values come from the frozen spec dump `tests/fixtures/recipe_parity/compose_stream.json` (captured at `060dabad64e028d83d192e5820d8f37df64d54d3`): prefix `DEMO`; DB driver, host, database, username and credential reference; read statement and `source_a`; the one direct mapping; the `target_a` payload profile; Orders and Billing base URLs and paths; target labels. They are re-expressed in the v2 split the same way as `tests/_fanout_v2_support.py::v2_from_compose_request`. |
| golden-000003 | `recipe:compose_all_cache` | the same pinned invocation | the same inputs plus `FANOUT_V2_ALL_CACHE_LINKS`, the explicit star with both targets on `document_cache`. This is the composition `compose_all_cache.json` was captured with. |
| golden-000001 | `recipe:api_to_api_sync_0` | direct canonical `process_ir` unit | `API_TO_API_SYNC_GOLDEN_PROCESS_IR`: source → map → target → stop over the frozen `recipe_parity/api_to_api_sync_0.json` process's own `$ref` symbols. It has no labels, because the sync dialect authored none and every `userlabel` in the frozen file is empty. The supporting components and the envelope (key, name, action, dependency set) are read from that spec at render time and never retyped. |

**The one re-spelling.** Fan-out 2.0.0 admits a credential reference only in the opaque
`credential://` form. So the DB connection's `<<opaque credential reference>>` is carried as
`credential://<<opaque credential reference>>`, the rule `v2_credential_ref` applies to the #159 S1
flagship oracle. No other value is changed.

**Checked, not asserted.** `process-xml-v1` bytes carry only references and labels, so byte parity
alone would accept almost any material.
`test_wave_gate_goldens.py::test_a_migrated_recipe_golden_plans_the_frozen_spec_it_was_captured_against`
pins every supporting component the request plans, field by field, to the frozen spec, with the
re-spelling applied to the expected side. It also pins the process envelope.
`test_every_active_golden_renders_with_all_test_modules_unimportable` renders the three cases and
the `ConnectionOverride` survivor to their frozen bytes before anything else, in a child process
where every `test_*` module is unimportable. While they render, it traces execution. No function
defined in `patterns.archetypes`, `patterns.composition`, `patterns.recipe_bridge` or
`compiler.process_ir.legacy_adapters` may run, and a control call proves the tracer sees one. An
import block is not used, because it cannot pass before the S3 cutover. The production recipe
registry imports the first three to hash their source (`recipes/registry.py::RECIPE_LAYER_MODULES`),
so every public-workflow route loads them without running them.
