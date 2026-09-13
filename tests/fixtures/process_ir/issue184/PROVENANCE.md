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
