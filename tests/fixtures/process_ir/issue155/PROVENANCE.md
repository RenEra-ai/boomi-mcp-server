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

**Placement attestation.** The source-role PREFIX placement (a linear step before the first
connector call) is legacy-oracle parity but **platform-UNATTESTED** — the same limitation #154
recorded at `tests/fixtures/process_ir/issue154/PROVENANCE.md:33-49`. #155 closes it with a live
execution of this exact spine on the renera account; the capture scenario id is recorded here and in
the audit ledger when that capture lands. Until then the limitation is CARRIED, not argued away.

**Comparison scope for the canonical rows (slice A).** A verbatim `<process>` freeze is impossible:
the legacy open tag carries option attributes `emit_process` never emits. Canonical
`process-xml-v1` rows compare the `<shapes>…</shapes>` element; expected bytes are
`"<process xmlns=\"\">" + legacy_shapes + "</process>"`. The transform is recorded with each
canonical row when it lands.
