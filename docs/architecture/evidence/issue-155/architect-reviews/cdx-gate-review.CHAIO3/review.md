I’ll review only the slice-A contract against the plan, treating B–F absences as intentionally out of scope. I’ll also give an explicit accept/reject disposition for each of the four recorded departures and verify every reported point against the baseline diff.The branch tip matches the stated `e39545c` and the scoped tree is clean. I’m now tracing each slice-A contract through model → lowering → semantic validation → emission/public projection, with the plan addendum/corrections treated as controlling where they override earlier text.The main contract path is present, but I’ve found several plan-record gaps worth separating from the four disclosed departures—especially immutability, the deferred primitive lowerer, and the provenance/acceptance matrix. I’m validating those against the controlling addendum now so the verdict doesn’t mistake a later-slice item for a Slice A miss.The disclosed behavior changes around DDP strictness, platform property survival, and profile identity are defensible. The substantive misses are elsewhere: two semantic diagnostics do not follow the planned contract, the canonical lowerers are incomplete, and several required clean-room/regression proofs were never added.Slice A does not fully realize the plan. I excluded all Slice B–F work, including trusted blank-path facts, `DYNAMIC_PATH_REQUIRED`, registry/grant machinery, and apply rechecks.

### Findings

1. The database archetype was not canonically re-lowered.

   The plan requires the archetype to “emit the same canonical pair” and specifically to serialize `"element_id": str(entry["key"])`. It still emits the raw key, returns the legacy `dynamic_path` dictionary, and installs that legacy block on the target: [database_to_api_sync.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/patterns/archetypes/database_to_api_sync.py:1380), [database_to_api_sync.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/patterns/archetypes/database_to_api_sync.py:1391), [database_to_api_sync.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/patterns/archetypes/database_to_api_sync.py:1507). Unlike the disclosed primitive-helper omission, this is the publicly reachable lowerer explicitly assigned to Slice A.

2. The dynamic-path diagnostic contract differs from the plan in two ways.

   - The plan assigns `PROCESS_IR_SEMANTIC_DYNAMIC_PATH_NO_DYNAMIC_SEGMENT` when the reaching writer “is absent or contains only static sources.” An absent writer instead produces `...DDP_NOT_ESTABLISHED`; `...NO_DYNAMIC_SEGMENT` is reserved for an existing static writer: [lineage.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/semantic_validation/lineage.py:719).
   - The plan says to emit the dynamic-specific diagnostic “instead of a duplicate generic read-before-write diagnostic.” A non-defaulted, unestablished nested DDP is first reported by the generic read pass and then again at the bound connector: [lineage.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/semantic_validation/lineage.py:807), [lineage.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/semantic_validation/lineage.py:733).
   - The Slice-A contract also specifies `<bound node>/path_binding`; `_report` uses only the node path: [lineage.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/semantic_validation/lineage.py:581).

3. `ConnectorPathBindingV1` is not frozen.

   The clause is explicit: “Add frozen, extra-forbidden `ConnectorPathBindingV1`.” The class inherits `ConfigDict(extra="forbid")` without `frozen=True`, and assignment succeeds at runtime: [process_ir.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/models/process_ir.py:208), [process_ir.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/models/process_ir.py:1087).

4. The golden matrix does not provide the required independent parity proof.

   The corrected plan requires source-DDP, target-DDP, and target-profile canonical rows derived from legacy `<shapes>` bytes. The implementation substitutes source-profile for source-DDP and explicitly records the target-DDP case as “canonical-only”: [PROVENANCE.md](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/fixtures/process_ir/issue155/PROVENANCE.md:57), [PROVENANCE.md](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/fixtures/process_ir/issue155/PROVENANCE.md:72), [_wave_gate_golden_corpus.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/_wave_gate_golden_corpus.py:1837).

   Refusing the unsafe old target-DDP document is correct; weakening lineage would be wrong. But freezing the new compiler’s own output does not replace a clean-room legacy oracle. If a safe legacy render is impossible, this acceptance clause needs an explicit architect amendment.

5. The mandatory acceptance matrix is incomplete.

   - `tests/test_process_ir_error_handling.py` is unchanged from baseline and contains no `allow_duplicates` cases. The sole positive witness proves propagation and XML omission, but not connector-scope refusal, retry-zero refusal, default dump/reparse, authored/default `forbid`, or write-safety independence: [test_process_ir_error_handling.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/test_process_ir_error_handling.py:283), [_process_ir_capability_witnesses.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/_process_ir_capability_witnesses.py:1061).
   - The required bound-target, bound-connector-call, and bound-entry fan-out witnesses are absent; existing Slice-A lineage coverage starts from a linear connector call: [test_process_ir_semantic_lineage.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/test_process_ir_semantic_lineage.py:965).
   - `test_error_taxonomy.py` has generic shape checks but no non-vacuous #155 ownership/presence pin.

6. The served node projection does not advertise the new capability from the nodes that author it.

   The Slice-A clause requires `"dynamic_path"` in the node facts for `node.target` and `node.connector_call`. Target has no capability tuple, and connector-call’s tuple omits it: [process_ir_projection.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/process_ir_projection.py:309), [process_ir_projection.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/process_ir_projection.py:328). Thus the manifest publishes support, but node-centric discovery does not link authors to that support.

### Four disclosed departures

1. Narrow bound-path strictness and DPP exclusion: **acceptable**. It matches the corrected bound-only rule, preserves the legacy general-lineage contract, and the DPP exclusion is live-attested and fail-closed where appropriate.

2. Measured property-survival table: **acceptable**. It answers the actual safety question and treats script-dependent or unmeasured behavior as refusing: [lineage.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/semantic_validation/lineage.py:295).

3. Primitive `lower_path_bindings` omission: **acceptable as a scope correction and #160 deferral**. Implementing an unreachable branch would add a dark channel. This acceptance does not cover the separate, publicly reachable database-archetype omission above. The primitive remains legacy at [rest_runtime.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/patterns/primitives/rest_runtime.py:291).

4. Inert profile-identity comparison: **acceptable for Slice A only**. Token-level mismatch remains fail-closed, while actual alias identity properly belongs to Slice C’s trusted snapshot: [lineage.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/semantic_validation/lineage.py:686).

I found no other unauthorized Slice-A runtime addition. In particular, the measured survival table is an intent-preserving correction, and Unit 3’s runtime implementation itself follows the plan.

VERDICT: ISSUES FOUND
