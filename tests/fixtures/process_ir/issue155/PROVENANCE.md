# Issue #155 fixture provenance

Every fixture and golden this slice adds records the CAUSALLY INDEPENDENT source its served
field names and byte shapes come from (CLAUDE.md Stage-1 step 2, amended 2026-08-14). Nothing
here is derived from the code #155 changes.

## `dynamic_path:source_role_profile` — the SOURCE-role legacy oracle

| Field | Value |
| --- | --- |
| Expected file | `tests/fixtures/golden_xml/dynamic_path_source_role_profile.xml` |
| Manifest row | `golden-000071`, owner `#155`, disposition `transitional_oracle`, renderer `process-component-v1` |
| Provenance class | **legacy-oracle parity capture** (the legacy chain is #155's oracle, not its subject) |
| Rendered by | `tests/_wave_gate_golden_corpus.py::_case_dynamic_path_source_role_profile` → `ProcessFlowBuilder.build(dynpath_source_role_config(), name="Dynamic Path Source Role Golden", folder_name="Golden/Fixtures")` |
| Frozen at | `9860842e5932c2e091a115e4697722bd8429953c` — **before any `src/` edit of this slice** |
| sha256 | `72a37d960d360b1255f1ff840f5d39c9c7c287ea5c9d20d99a09bfef60c1e73a` (3384 bytes) |
| Determinism | two renders compared equal at freeze time |

**Why it exists.** The issue's acceptance criterion names `dynamic_path_source_ddp.xml` as the
source-side oracle. Measured: that file and `dynamic_path_target_profile.xml` are BOTH target-side
REST `PATCH` shapes — "source" there names the DDP *segment* source, not the connector role — so no
golden pinned the source ROLE, and the criterion is unsatisfiable as written (ledger `EVAL-155-01`).
The source role has its own emitted spine: `documentproperties` precedes the source
`connectoraction` and the `Path` property rides the GET (`process_flow_builder.py:931-961`).

**What the frozen bytes carry** (verified at freeze time): shape order
`start → documentproperties → connectoraction(GET) → map → connectoraction(PATCH) → stop`; the GET
carries `parameter-profile="PROFILE-UUID"` and one `<propertyvalue key="path" name="Path"
valueType="track">`; the PATCH target carries none. The profile-bearing path is used deliberately —
it is the superset shape (Path body **and** `parameter-profile`) on a source-role connector, and it
matches the config the legacy source tests already exercise.

**Placement attestation — CLOSED 2026-08-26.** The source-role PREFIX placement (a linear step
before the first connector call) was legacy-oracle parity but platform-UNATTESTED — the limitation
#154 recorded at `tests/fixtures/process_ir/issue154/PROVENANCE.md:33-49`. It is now attested by a
live execution of this exact spine on the renera account: capture scenario `cap155-e1-source-dynamic-path`,
execution `execution-b91fb002-0a98-4e51-b9fb-ad503ea01241-2026.08.26`, terminal status COMPLETE,
archived under `docs/architecture/evidence/issue-155/captures/`. The platform stores the shape,
serves it back intact (`start` → `documentproperties` → `connectoraction` GET → … ), and composes the
per-document path at runtime against an operation whose stored path is blank. Ledger row
`EVAL-155-04`.

**Public reachability, measured.** This config is NOT plannable through any composed public route at
the baseline: the legacy kind refuses a REST source, the pipeline stage refuses the gated sub-block,
and the archetype refuses a per-document token path (ledger row `QA-155-r1-01`, envelopes archived).
The corpus therefore renders it by calling the builder directly — legitimate for an ORACLE, since the
builder is the emission authority this slice is compared against, and deliberate: giving this shape a
composed public route is what #155 adds.

**Comparison scope for the canonical rows (slice A).** A verbatim `<process>` freeze is impossible:
the legacy open tag carries option attributes `emit_process` never emits. Canonical
`process-xml-v1` rows compare the `<shapes>…</shapes>` element; expected bytes are
`"<process xmlns=\"\">" + legacy_shapes + "</process>"`. The transform is recorded with each
canonical row when it lands.
