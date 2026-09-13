# Issue #184 fixture provenance

Every fixture and golden this slice adds records the CAUSALLY INDEPENDENT source its served field
names and byte shapes come from (CLAUDE.md Stage-1 step 2, amended 2026-08-14). Nothing here is
derived from the code #184 changes.

## `dynamic_path:both_sides` — the mapped both-sides legacy oracle (A8, `EVAL-155-02`)

| Field | Value |
| --- | --- |
| Expected file | `tests/fixtures/golden_xml/dynamic_path_both_sides.xml` |
| Manifest row | `golden-000079`, owner `#184`, disposition `transitional_oracle`, renderer `process-component-v1` |
| Provenance class | **legacy-oracle parity capture** (the legacy chain is #184's oracle for A8, not its subject) |
| Input | `tests/fixtures/process_ir/issue184/legacy_both_sides_config.json` — the attested architect design's frozen config (`.codex/plans/issue-184.md` §3), byte-identical to the capture input |
| Rendered by | `tests/_wave_gate_golden_corpus.py::_case_dynamic_path_both_sides` → `ProcessFlowBuilder.build(config, name="Dynamic Path Both Sides Golden", folder_name="Golden/Fixtures")` |
| Frozen at | the commit that adds this file — the first commit of the slice, **before the step-0 baseline and before any `src/` edit** (its sha is recorded in `docs/architecture/ISSUE_184_AUDIT_LEDGER.md`); the bytes were captured from a pristine `git archive` extraction of the branch point `cbab28ffc176ddb2378283cf7132eb9c37afc374` |
| Validator | `ProcessFlowBuilder.validate_config(config, depends_on=[], allow_rest_source=True)` returns None (measured) |
| sha256 | `633a197f3b67a0773a828acb378451d64183ca63a8c5ef60ddb095bbc053aede` (4471 bytes) |
| Determinism | two builds compared equal at capture |
| Capture record | `docs/architecture/evidence/issue-184/oracle/` — the capture script, its input, the output bytes and a manifest carrying the branch-point sha and interpreter |

**What the frozen bytes carry** (verified at capture): shape order
`start → documentproperties → connectoraction(GET) → map → documentproperties → connectoraction(PATCH) → stop`;
each connector carries exactly one dynamic `Path` property; only the PATCH carries a
`parameter-profile` attribute (`PROFILE-REQUEST`), because the source path composes from a
run-supplied dynamic process property and reads no profile.

**Why these config choices.** The SOURCE path composes from the process property `seed_id`: that is
the source-role driver #155 attested live (`cap155-e1-source-dynamic-path`), and it keeps the live
composite execution A8 (iii) requires able to address a real resource — a profile read there would
read the scheduled start document. The TARGET path reads a profile element from the MAPPED document,
so its request profile is the map's OUTPUT profile, distinct from the GET's response profile; that is
the spine the canonical spelling `set_ddp → connector_call{path_binding} → map_ref → set_ddp →
connector_call{path_binding} → stop` must reproduce, with the target-side writer after the map.

**Comparison scope for the canonical row.** A verbatim component freeze cannot match a canonical
`process-xml-v1` render: the legacy open tag carries option attributes `emit_process` never emits
(the #155 precedent, `tests/fixtures/process_ir/issue155/PROVENANCE.md`). The canonical A8 row
compares the `<shapes>…</shapes>` element; its expected bytes are
`"<process xmlns=\"\">" + legacy_shapes + "</process>"`, cut from THIS file, never from a canonical
render.

**What does NOT count as discharge** (issue #184 A8): a green compile of the hoisted-writer or
no-map form; a spelling whose target writer reads the pre-map profile; expected bytes regenerated
from the new compiler; a fixture without this provenance.

## `issue184:source_dynamic_path_dpp` — golden-000072's safe replacement survivor (amendment 2 §5)

| Field | Value |
| --- | --- |
| Expected file | `tests/fixtures/golden_xml/issue184_source_dynamic_path_dpp.xml` |
| Manifest row | `golden-000080`, owner `#184`, disposition `survivor`, renderer `process-xml-v1` (appended; `golden-000072` tombstoned in the same change, so the active floor is unchanged at 79) |
| Provenance class | **live capture of an artifact proven operable**: `cap155-e1-source-dynamic-path` executed COMPLETE with one inbound and two outbound documents (`execution-b91fb002-0a98-4e51-b9fb-ad503ea01241-2026.08.26`), and the counterparty log records the composed GET path |
| Source bytes | `docs/architecture/evidence/issue-155/captures/cap155-e1-source-dynamic-path/stored_process.xml`, the platform's STORED process, sha256 `c3355a59e6813707a02a9d75b4856d59d28d9de3dbd57adf5ee368a0f9d5f17b` |
| Transform | `docs/architecture/evidence/issue-184/oracle/freeze_source_dynamic_path_dpp.py`: slice `<shapes>`; substitute the platform ids per shape (GET `RCONN`/`ROP`, PATCH `CONN-UUID`/`OP-UUID`, each asserted to occur once); wrap as `<process xmlns="">…</process>` |
| Record | `docs/architecture/evidence/issue-184/oracle/source_dynamic_path_dpp.MANIFEST.json` |
| sha256 | `56ffde0818b2f6d3fe364a39a015f9ddd1c05c04e407802398a261a691dc502f` (2491 bytes) |
| Authored input | `tests/fixtures/process_ir/issue184/source_dynamic_path_dpp.json`, written by the same script from a literal whose field names come from the stored shapes: a static segment, then a `dpp` segment `key` with an empty default (`processpropertydefaultvalue=""`), and a path binding naming `DDP_PATH_CLIENTS` with no request profile (the stored GET carries no `parameter-profile`) |
| Symbols | `issue155_symbols()`, whose ids are the substitution targets |

**Never regenerated from the compiler.** The canonical render was compared against these bytes
before they were committed, and matched. The render is the thing under test; the capture is the
oracle.

**What it retires.** `golden-000072` read a profile element off the empty scheduled entry document,
which #184 refuses. This replacement keeps the source-role spine that the capture attests
(`start → documentproperties → connectoraction GET{Path} → connectoraction PATCH → stop`). It
composes the path from a run-supplied dynamic process property, the driver the platform actually
ran.

## Terminal cache replacements — the seven cache-successor goldens (amendment 3 §6)

**Why they retire.** Add to Cache and all-document Remove from Cache emit zero documents. Any step
wired after them is skipped, and the run still reads COMPLETE. Evidence:
`cap184-prefix-predecessors`, `cap184-passthrough-ddp-handoff` (`xr-ddp-cache-linear-1`),
`cap184-cache-put-successor` and `cap184-cache-remove-read`, all under
`docs/architecture/evidence/issue-184/captures/`.

A scan of all 80 active goldens found exactly seven rows with an outbound edge from a cache sink
(ten edges). Each is retired through the manifest's active-to-tombstone transition and replaced
below. Seven additions minus seven retirements leaves the active floor at 80.

| Retired | Replacement input case / expected file | Source of the expected bytes | sha256 (bytes) |
| --- | --- | --- | --- |
| `golden-000012` `process_flow:document_cache_remove` | `issue184:cache_remove_terminal_branch` / `issue184_cache_remove_terminal_branch.xml` | pristine branch-point render of `source → Branch[target; cache_remove → stop]`, then the recorded transform: drop the remove's single outgoing dragpoint and the final Stop `shape7` | `b4b8d4287264e02aade4c2f8bb829efb26231b44817e3e8992aea4a1dea71a12` (2014) |
| `golden-000018` `process_flow:flow_sequence_cache_load_retrieve_remove` | `issue184:cache_stage_read_remove` / `issue184_cache_stage_read_remove.xml` | pristine render of `source → Branch[cache_put; cache_get → target; cache_remove → stop]`, same transform on `shape8`/`shape9` | `2515c16100e6475ed19c9c7471eda9e09fae42f5bb81504ec2aeeb2183e71665` (2679) |
| `golden-000019` `process_flow:flow_sequence_cache_put_get` | `issue184:cache_stage_read` / `issue184_cache_stage_read.xml` | pristine render of `source → Branch[cache_put; cache_get → target]`, no transform | `e63c53802710b2e7cafa9e68f70d196b40a25da97acbd5250a0ecceb48e7a171` (2325) |
| `golden-000066` `issue154:catch_cache_put_exception` | `issue184:catch_exception_without_cache_put` / `issue184_catch_exception_without_cache_put.xml` | pristine render of 000066's input with the catch-body write removed and the message no longer claiming a stage; no transform | `b01ad9c5b95ff931677bd759e71e8d25edafb550604b5530d83bc613a47465c9` (1894) |
| `golden-000005` `trycatch_dlq:connector_scope_notify` | `trycatch_dlq:connector_scope_notify_terminal` / `issue184_cache_notify_connector_terminal.xml` | byte transform of the immutable pre-change golden: drop both cache outgoing dragpoints and the synthetic Stops `shape10`, `shape13` | `49ed42fbd2d7df0ac668d8e5d58b84bc0bbaff6a3a36343f5f3dafdf36f67658` (5276) |
| `golden-000059` `trycatch_dlq:notify_document_cache` | `trycatch_dlq:notify_document_cache_terminal` / `issue184_cache_notify_terminal.xml` | same transform, synthetic Stop `shape8` | `dd29439ea851a49d8280d14588eea36a8a622b8f06564ffef65bf9fb8dc5ca2c` (3520) |
| `golden-000060` `archetype_dlq:notify_document_cache` | `archetype_dlq:notify_document_cache_terminal` / `issue184_cache_notify_archetype_terminal.xml` | same transform, synthetic Stops `shape10`, `shape13` | `cc3b88770710800fb782c4ea73d36a24176bed67617f4f8d2dfc8aaf2829a728` (5826) |

The replacement input-case names for the three notify rows are provisional until the rows are
registered; the byte records do not depend on them.

**Provenance classes.**

* The **canonical four** are renders by the canonical compiler and `emit_process` imported from a
  detached worktree of the branch point `cbab28ffc176ddb2378283cf7132eb9c37afc374`. The script
  asserts the worktree's head, its clean state and the import origin, so the code #184 changes
  supplies no byte.
* Every cache and Exception shape in them is also checked, ids and layout aside, against a stored
  independent artifact:
  * terminal Add to Cache and the triggered retrieve against `cap184-retrieve-ddp-replacement`
    `r1_ddp_replacement.stored.xml` (executed COMPLETE; the R1 overlay row was decided on it);
  * the terminal all-document remove against the platform-authored
    `tests/fixtures/live_xml/m11/process_cache_branch_load_remove.xml` (#119 census, pre-baseline);
  * the Exception against the Exception-only control `p3c_catch_exception_control.stored.xml`
    (executed; the run reads ERROR with the caught message).
* The terminal-remove form does not exist at the branch point. So the remove renders as the LAST
  Branch leg, followed by a Stop, and the transform deletes that Stop. It is the final allocated
  shape, so nothing is renumbered.
* The **notify three** are byte transforms of the goldens they retire. Those goldens predate this
  slice. Every substitution is asserted to occur exactly once, with no remaining reference, and
  each resulting terminal Add to Cache is checked against the same executed R1 capture. Remaining
  shape ids and coordinates are unchanged, so the removed synthetic slots stay reserved.

**What the replacements do NOT claim.** `catch_exception_without_cache_put` preserves the throwing
intent; it does not claim stage-and-throw equivalence, which the platform does not provide. The
original combination stays as a refusal witness. The staged read proves the ordered-Branch
contract; it makes no claim about N×M property overlay, which stays OPEN.

**Authored inputs.** The canonical four's inputs are written by the same script under
`tests/fixtures/process_ir/issue184/`, in the NEW spelling: a Branch leg whose terminal is
`cache_remove`. The notify three keep their existing legacy/archetype configurations, so their
original routes stay exercised.

**Records.**
* `docs/architecture/evidence/issue-184/oracle/freeze_terminal_cache_canonical.py` →
  `terminal_cache_canonical.MANIFEST.json`: branch-point render hashes, transform, golden and
  fixture hashes, and the authority checks with authority hashes.
* `docs/architecture/evidence/issue-184/oracle/freeze_terminal_cache_dlq.py` →
  `terminal_cache_dlq.MANIFEST.json`: source and target hashes, removed shapes, and the
  terminal-load authority with its hash.
