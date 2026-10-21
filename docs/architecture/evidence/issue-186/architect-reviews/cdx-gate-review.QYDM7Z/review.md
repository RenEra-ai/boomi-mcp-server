I’ll read the ledger first, then trace the correction delta and its recorded validations. I’ll keep this review read-only and make no MCP calls.The working tree is clean at `71c046f`. The ledger records fixes for seven findings and a refutation for e1-04. I’ll assess that refutation against the required code-set equality and separately inspect the cache requirement, binding, and validation evidence changes. I’m using the local architect conformance review skill for the report discipline.The new QA records link both stored-cache captures to COMPLETE executions, addressing the provenance gap. The e1-04 ledger row still records a constructible case where the composition serves SCOPE and the twin serves BEFORE. I’m checking whether the refutation justifies that difference under the plan.The source trace shows a new over-refusal: the inherited cache-write check rejects mixed cache contents when any stored profile differs, while the direct twin treats the mixed retrieval as unproved and admits the write. The binding and malformed-ID fixes look sound. I’m also checking the remaining matrix gaps: the new inherited cache routes use No Data children, and the forwarding matrix compares lineage codes only.The review leaves two new cache correction defects, the unresolved e1-04 code difference, and incomplete e1-06/e1-07 coverage. The origin fix and operability evidence address e1-05 and e1-08. I’ve kept the recorded unsupported-placement residual separate from the new runtime findings.Reviewed files: `ISSUE_186_AUDIT_LEDGER.md`; evaluation-1 review and correction-review archives; `authoring/{process_ir_effects,connector_resolution_snapshot,workflow,contract}.py`; `semantic_validation/{lineage,contracts,context}.py`; `recipes/materialization.py`; relevant grammar/ID authorities; the four affected test modules; local QA r3/r4 reports, captures and execution records.

Reviewed `518bac3..71c046f`. No edits, tests, network requests or MCP calls were made.

| Finding | Disposition |
|---|---|
| e1-01 | **RESOLVED for the reported single-profile scenarios.** Destination requirements now propagate. Two new correction defects remain below. |
| e1-02 | **RESOLVED.** Unjudged account name bindings override candidate cache configuration with UNKNOWN. |
| e1-03 | **RESOLVED.** Stored profile IDs must pass the shared component-ID recognizer. |
| e1-04 | **NOT RESOLVED.** The recorded refutation does not establish the required code-set equality. |
| e1-05 | **RESOLVED.** Measurement now distinguishes observations by pointer, name and origin. |
| e1-06 | **NOT RESOLVED.** Same-document coverage was added, but forwarding coverage remains incomplete. |
| e1-07 | **NOT RESOLVED.** Fresh typed-apply and identity controls were added; stored-profile passthrough route cells remain absent. |
| e1-08 | **RESOLVED.** Both captures now link to COMPLETE executions whose logs show cache load, retrieve and Stop with one document. |

CDX-186-r3/r4/r5’s reported scenarios are repaired. Their validations do not cover the following neighbours.

[P2] Inherited cache writes reject mixed contents the twin admits — [lineage.py:3361](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/semantic_validation/lineage.py:3361)

Parent Branch legs append P1 and P2 documents to an unprofiled SOURCE, then call a No Data child containing `cache_get SOURCE → cache_put TARGET(P1)`.

The new call check rejects P2 among `{P1,P2}`, returning `PROCESS_IR_SEMANTIC_PROFILE_MISMATCH`. The flattened twin retrieves that mixture as `STREAM_UNKNOWN` and admits: its cache-write check rejects only a proved stream profile. Expected complete code set: empty.

A singleton caller contribution also fails when the child appends unprofiled documents before retrieving SOURCE: the call rejects P1 against TARGET(P2), while the twin retrieves `{P1,None}` and admits.

Apply the twin’s aggregate stream-proof predicate, including the child’s own reaching contributions. Comparing individual caller identities—or only checking caller-content cardinality—cannot reproduce it.

[P2] Destroying profile proof is mistaken for downstream reliance — [process_ir_effects.py:1335](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/process_ir_effects.py:1335)

Parent stages unprofiled documents in SOURCE. Its No Data child first appends P1 to SOURCE, then retrieves SOURCE into TARGET(P2), with no downstream typed consumer.

Unseeded derivation sees `{P1}` and a write mismatch. Seeding P2 creates `{P1,P2}`, makes retrieval UNKNOWN and clears that mismatch. The helper therefore promotes `(SOURCE,P2)` to an exact content requirement.

The composition rejects the parent’s `{None}` with PROFILE_MISMATCH. The flattened twin retrieves `{None,P1}` and admits.

Promotion must demonstrate that the seed proves a profile needed by a consumer. Clearing a refusal by making the stream unproved does not establish reliance.

[P2] The local-writer dropped-property difference remains — [lineage.py:3548](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/semantic_validation/lineage.py:3548)

Use the constructible variant acknowledged in ledger row e1-04: parent Branch call-leg prefix `set_ddp X → waited passthrough CHILD`; CHILD first leg writes X, while its second runs custom scripting and then a non-default Decision reading X.

The arriving X is discarded on the reader’s documents. The twin returns `{PROPERTY_READ_BEFORE_WRITE}`; the composition retains `{DDP_SCOPE_INVALID}`. The static set-DDP prefix is expressly admitted, so this requires no placement exception.

“Both sides refuse” explains the limitation but does not refute the acceptance criterion requiring exact codes. Preserve conditional observations independently of immediate diagnostics, and make called-root/caller judgment produce BEFORE here while retaining SCOPE for the no-prefix control. Restoring the filtered requirement alone would leave an extra code.

[P2] Expanded forwarding coverage still omits legacy roots and complete verdicts — [test_issue_184_child_state_transfer.py:10703](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/test_issue_184_child_state_transfer.py:10703)

The new loop’s `typed=False` arm changes symbols, but `_b186_deep` constructs the same source-less roots. A legacy No Data reader starting with `source`, reached through a legacy forwarding middle, remains unmeasured.

The comparison also discards every non-lineage code. For example, its No Data `Message → call reader` cell reduces composition `{PROCESS_CALL_PLACEMENT_UNSUPPORTED}` versus admitted twin `{}` to empty-versus-empty.

Construct the legacy source forms and compare complete verdicts. Record unsupported pairings with their actual refusal/context rather than counting filtered equality as matching evidence.

[P2] Stored-profile passthrough route witnesses remain missing — [test_issue_184_component_identity.py:3800](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/test_issue_184_component_identity.py:3800)

The added inherited-write witness uses No Data `cache_get STAGE → cache_put stored TARGET`; its forwarding middle is also No Data. Live r3/r4 repeat that channel.

The required separate input remains absent: parent produces known A documents → Data Passthrough child writes those arriving documents into stored cache B, directly and through a passthrough middle. Local UNKNOWN-symbol witnesses do not exercise stored-profile collection.

Add typed plan, typed compile and raw apply cells: matching A admitted; stored B/UNKNOWN refused at `/process_ref`; faithful twins and zero writes on refusals.

VERDICT: ISSUES FOUND
