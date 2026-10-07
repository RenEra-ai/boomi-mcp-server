# Audit ledger — issue #186 (M12.24 follow-up: entry-document and later-leg property reads across a process call, and the stored profile of a reused document cache)

Audit record for the completion workflow (`CLAUDE.md`, amended 2026-08-12 / -08-14; standing
rules in `docs/architecture/COMPLETION_WORKFLOW_RULES.md`).

Plans of record, each attested by the dispatcher-owned gate collector:

| Plan | Attestation | Message sha256 | Thread | Result |
| --- | --- | --- | --- | --- |
| `.codex/plans/issue-186.md` — the architect design plan | `.codex/plans/issue-186.attest.json` | `85753bba1ad7079cac4722310db43c816b46e850f7ef83e53eb1e92a343b89de` | `01a11755-eff7-7fd0-83e4-e80710db9346` | turn token 1, gate `architect`, `ok:true`/`stopped:true`; the brief told the session not to call MCP tools and it completed on its primary prompt |

Claude's implementation plan is `.codex/plans/issue-186.claude.md` (both plans are gitignored, kept
locally). Where a plan and this ledger disagree, the correction is recorded below with its evidence and
the plan is NOT silently re-authored.

## Stage-1 step 0 — baseline

| Field | Value |
| --- | --- |
| Issue | #186 — entry-document and later-leg property reads across a process call; the stored profile of a reused or reference-only document cache |
| Branch | `codex/issue-186` |
| Branch point and step-0 baseline | `dev` @ `46100b7866c7962b53cf62a400276ae30258ccc6` (printed by `git rev-parse HEAD` before any edit) |
| Comparison tree for the blast radius | #184's final source tree `e22b8fa46db594bf6845e97a03444ad760c2f181` (`46100b7` adds records and a README link only; `git diff e22b8fa 46100b7 -- src tests` is empty) |
| Reference suite at the branch point | full non-KB, `python -m pytest tests --ignore=tests/kb -p no:cacheprovider`, local `.venv` 3.12, run to completion before any edit: `1 failed, 14411 passed, 19 skipped in 2835.75s`. The one failure, `test_audit_ledger_attestations_have_durable_matching_evidence`, named a byte-code file the run itself wrote into the #184 evidence directory (`document_emission/__pycache__/`, untracked and ignored) because the shell did not set `PYTHONDONTWRITEBYTECODE`; the wave gate sets it for every child. Removed, the test passes; every later run here sets it |
| Development | a detached worktree of the baseline (`.claude/worktrees/w186`), so the baseline suite read an untouched tree |
| Slice kind | behaviour-affecting (new child-contract rows a call judges, a stored document-cache profile read on account-holding routes, served rule text, served revisions) |

## Loop roster (enumerated in advance, before the first correction)

| # | Logical loop | Authority | Scope |
| --- | --- | --- | --- |
| 1 | Stage-1 QA | `boomi-qa-tester`, live through the public MCP tool boundary | the three items' affected scenarios on typed plan, typed compile and raw apply, both child entry forms, and the matching controls |
| 2 | Stage-2 repo commit review | detached Codex review per `CLAUDE.md` §5b–5e | initial: `--base 46100b7866c7962b53cf62a400276ae30258ccc6`; then each fix delta |
| 3 | Architect implementation review | `/codex-issue` §6 gate (`--gate review`) | implementation vs the plan of record; capped at 3 evaluations by `docs/architecture/COMPLETION_WORKFLOW_RULES.md` |
| 4 | Composite wave gate | `scripts/wave_gate.py` | full suite, golden manifest, determinism, wave-delta review, one live scenario per changed capability class |
| 5 | Terminal correction loop | only via a recorded roster-addition checkpoint | that batch |

## Plan corrections and decisions (recorded before the first gate)

1. **The planner's ten deviations, each decided on measurement.** Adopted: no separate seeding of the
   new conditional rows when a called root is validated (a conditional row establishes nothing, and the
   child already tolerates absence alone); the unknown-profile requirement gets its own branch at the
   call, refusing only a PROVED document profile, because the generic arm would also refuse a Message's
   output that the twin admits; a stored profile's internal symbol carries the stored component id as
   its identity; the served mismatch text is extended, not replaced, so its existing served-text pin
   still holds. REJECTED: deferring the entry-requirement repair filter. The grandchild matrix measured
   ten cells where a middle that fails its child's dropped read exported it as a requirement, was then
   validated under that seed and served a read before write beside the twin's scope error (SELF-186-02);
   the architect's filter is implemented.
2. **No-account routes state the limit rather than fail closed (item 3).** The issue allows either. A
   route that reads nothing — no client — describes a reused or reference-only cache by no profile, as
   before #186, and the served rule says so; every apply holds the account and reads the stored profile
   in its pre-write pass, so nothing is written on the unchecked verdict. Fail-closed there refused
   #184's own no-account witnesses of working reuse (measured on
   `test_two_specs_binding_one_cache_are_one_cache_on_the_typed_plan_route`). On a route that READ, a
   cache with no usable reading is unknown and matches nothing.
3. **`profileType="profile.none"` with no profile is absent, everything incomplete is unknown.** The
   absent verdict is the pre-#186 verdict for every reused cache, so trusting that one spelling relaxes
   nothing; a missing attribute, a missing profile id, an unrecognised type, two configurations, the
   wrong component type, unparseable or entity-declaring bytes and a failed read are all unknown.
4. **Item 2 generalised to all execution-scoped state.** The writer sets a child exports are the
   execution part of the two in-process writer sets — process properties and document caches — so the
   caller's later-leg index also locates a child's conditional cache write (the cache sibling of
   SELF-186-01, measured: `PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING` against the twin's Branch-order code before, matching
   after).
5. **One #184 expectation flips, as the issue requires.**
   `test_plan_compile_and_apply_judge_one_binding[writerless_through_alias]` wrote P1 — a profile the
   request itself creates — into two caches bound to the stored cache; no stored cache can declare it,
   so every route now refuses it before any write. #184's `test_a_calls_later_leg_write_is_what_it_guarantees_not_what_it_may_write`
   pinned SELF-186-01's admission and now asserts the twin's code.

## Defect-class ledger (a class is a (mechanism, runtime-authority) pair, assigned at reconciliation)

| Class | Mechanism | Runtime authority | Instances (derived from rows) | Resolution |
| --- | --- | --- | --- | --- |
| `child-contract-omits-an-entry-document-read` | a called child's non-strict read of a document property off documents no retrieve handed on is exported by no contract row, so no caller judges it | the in-process different-document-copy rule (`_classify_unmet_read`) | INH-SELF-184-73 | fixed: the child exports `nonstrict_document_reads` with the relation measured against an entry seed; each call applies the in-process rule against its own writers |
| `child-contract-omits-a-leg-ordered-process-property-read` | a called child's execution-scoped read reaches its caller without what the in-process later-leg and establishing-writer rules read: the non-strict read itself (INH-SELF-184-74), a sibling's conditional writer (SELF-186-01), the leg order of a strict requirement (SELF-186-03) | the in-process execution-scope rules (`_written_in_a_later_leg` over `_leg_write_index`, `_nonstrict_read_can_fail` over `_established_anywhere`) | INH-SELF-184-74, SELF-186-01, SELF-186-03 | structural fix on the second instance (below) |
| `entry-requirement-not-measured-against-an-entry-seed` | a passthrough child's document-property requirement exported whether or not an arriving property can satisfy it | the child's own walk under an entry seed | SELF-186-02 | first instance; fixed by measurement (`_entry_repairable_reads`) |
| `unknown-profile-read-as-unstated` | an UNKNOWN cache profile recorded as the unstated consumption every call skips | the profile identity rule a cache write applies | SELF-186-04 | first instance; fixed with a requirement that matches no proved profile |
| `stored-profile-of-an-unwritten-cache-never-read` | the profile of a document cache the request does not write is never read from the account that stores it | the account's stored document-cache component | INH-SELF-184-78 | fixed: read on every account-holding route, before any write |
| `requirement-judged-against-a-stream-an-earlier-requirement-narrowed` | the call's check of its child's unknown-profile requirement read the stream identity an earlier stated requirement of the same child had just inferred | the twin's Branch, which hands every leg the documents that arrive at it | CDX-186-r1-01 | first instance; fixed |

**Structural fix for `child-contract-omits-a-leg-ordered-process-property-read`** (second instance at
SELF-186-01, so mandatory in the same batch). The invariant replaces enumeration: what a child exports
is read off THE functions the in-process rules ask — `_keys_written_anywhere` (every writer, which the
later-leg index locates) and `_keys_established_anywhere` (the establishing writers, newly factored out
of `_established_anywhere`) — restricted to execution scope, and each caller-side rule reads the
exported set where it reads its own nodes; a passed-on read carries the in-process later-leg answer,
and so does every execution-scoped requirement (`required_read_leg_orders`). Sibling sweep: the cache
scope (fixed in the same batch, decision 4); document properties (already carried by
`document_property_writes`, #184 batch 30); an unwaited child's process-property writes (by the
in-process rule they cannot fail a non-strict read, recorded below as having no faithful twin).
Non-vacuity witnesses: one source mutant per rule (`tests/test_issue_184_child_state_transfer.py`,
#186 section). Coverage: the differential over every step kind the emission authority admits, both
entry forms, both dialects, six writer placements, and the grandchild matrix.

## Recorded residuals (measured; both sides refuse, or no faithful twin exists)

- A retrieve between the entry and the read makes it a read of retrieved documents, #184's cached
  channel: the twin adds `PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE` beside
  `PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING` for the empty cache, while the composition's
  cached row is answered vacuously by a caller that stores nothing there. Both refused, identical at
  `e22b8fa`.
- A profile consumer in a passthrough child (a map, a split or combine): the child's profile proof
  serves `PROCESS_IR_SEMANTIC_PROFILE_MISMATCH` where the twin serves
  `PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH`; lineage codes match exactly. Identical at `e22b8fa`.
- A process-property read whose only writer is a child called without waiting: inlining the call
  erases its asynchronous boundary, so there is no faithful twin; the in-process rule counts no
  fire-and-forget write as one that can fail a non-strict read.
- A step before a call draws `PROCESS_IR_CAPABILITY_PROCESS_CALL_PLACEMENT_UNSUPPORTED`, which a twin
  with no call cannot share; the grandchild matrix compares lineage codes for that reason.

## Finding rows (one per raw finding; append-only; exactly one disposition each)

| ID | Source gate + run dir + attestation | Verbatim summary | Original label | Blocking class | Defect class | Derived tier (anchor inline) | SHA/delta | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| INH-SELF-184-73 | Inherited — `docs/architecture/ISSUE_184_AUDIT_LEDGER.md` row `SELF-184-73` (deferred to #186, `out-of-scope-by-design`) | "A non-strict read straight off a called child's entry documents is never judged against its callers: a No Data or Data Passthrough child reaches a Decision on DDP X with no default, read off its entry documents with no retrieve, while the caller writes X on another leg; the composition is admitted while the flattened twin is refused `PROCESS_IR_SEMANTIC_LINEAGE_DDP_SCOPE_INVALID` by the different-document-copy rule" | unlabelled (self-found) | runtime behavior | `child-contract-omits-an-entry-document-read` | Standard — anchor: no source label; runtime behavior is a blocking class, not a critical anchor | inherited at `46100b7` | fixed in the implementation batch — measured over the full differential: at `e22b8fa` every writer cell of the family diverged (admitted against the twin's scope error, both forms, both dialects, every admitted step kind); on this tree every cell matches its twin's code set except the recorded residuals above; witnesses `test_every_property_read_across_a_call_earns_its_flattened_twins_codes[ddp]`, `test_a_direct_read_off_a_childs_entry_documents_earns_its_twins_scope_error` (replacing the recorded-limit witness), `test_a_no_data_child_reads_its_own_empty_document_whatever_its_caller_handed`, `test_an_inherited_property_a_script_drops_is_a_read_before_write_and_one_never_carried_is_a_scope_error`; mutants `test_the_entry_document_row_left_unrecorded_re_admits_the_reader`, `test_a_fresh_row_judged_as_the_cache_less_row_serves_the_wrong_code` |
| INH-SELF-184-74 | Inherited — `docs/architecture/ISSUE_184_AUDIT_LEDGER.md` row `SELF-184-74` (deferred to #186, `out-of-scope-by-design`) | "A non-strict process-property read in a called child is never judged by the caller's leg order: a child reaches a Decision on a process property with no default that its caller writes on a Branch leg running AFTER the leg holding the call; the composition is admitted while the flattened twin is refused `PROCESS_IR_SEMANTIC_LINEAGE_BRANCH_ORDER_INVALID` by the execution-scope later-leg rule" | unlabelled (self-found) | runtime behavior | `child-contract-omits-a-leg-ordered-process-property-read` | Standard — anchor: no source label; runtime behavior is a blocking class, not a critical anchor | inherited at `46100b7` | fixed in the implementation batch — the child exports `nonstrict_process_reads` with its own later-leg answer; each call applies the leg-order rule with its establishing writers; witnesses `test_every_property_read_across_a_call_earns_its_flattened_twins_codes[dpp]`, `test_a_non_strict_process_property_read_earns_its_twins_branch_order_error` (replacing the recorded-limit witness), `test_a_reader_one_call_deeper_earns_its_twins_lineage_codes[dpp]`; mutant `test_the_process_property_row_left_unrecorded_re_admits_the_reader` |
| INH-SELF-184-78 | Inherited — `docs/architecture/ISSUE_184_AUDIT_LEDGER.md` row `SELF-184-78` (deferred to #186 item 3, `out-of-scope-by-design`) | "A cache write into a document cache apply reuses or names only by reference is admitted with no profile check: a component apply does not write publishes no profile fact, so the cache reads as declaring none, though the account's stored cache may declare another. Pre-existing — there was no cache-profile check at `cbab28f`" | unlabelled (self-found) | runtime behavior | `stored-profile-of-an-unwritten-cache-never-read` | Standard — anchor: no source label; runtime behavior is a blocking class, not a critical anchor | inherited at `46100b7` | fixed in the implementation batch — measured on typed plan, typed compile and raw apply for a declared-id reuse, a reference-only binding and a name reuse: at `e22b8fa` all 21 cells wrote; on this tree a different stored profile, a profile the request creates, missing attributes, unreadable bytes and a failed read are refused `PROCESS_IR_SEMANTIC_PROFILE_MISMATCH` at the write's `/cache_ref` before any write, while a matching stored profile and an unprofiled cache are admitted and write; witnesses `test_a_write_into_an_unwritten_cache_is_judged_against_its_stored_profile_on_every_route`, `test_the_stored_cache_reader_classifies_known_absent_and_unknown`, `test_an_unwritten_cache_is_read_once_per_component_whatever_its_aliases`, `test_with_no_account_an_unwritten_cache_states_no_profile_as_its_served_rule_says`; mutant `test_the_stored_profile_selection_is_load_bearing` |
| SELF-186-01 | the implementation batch's own sibling sweep (the conditional-writer cells of the differential) | "A non-strict process-property read in a caller whose only writer of it is a WAITED child writing it conditionally — behind a Decision arm, or after a step that may hand on no documents — is admitted: the caller's establishing-writer set and leg index count only what the child GUARANTEES, while the flattened twin holds the conditional writer as its own node and refuses the read (read before write, or Branch order when the writer's leg runs later). #184 pinned the leg-order case as an admission" | unlabelled (self-found) | runtime behavior | `child-contract-omits-a-leg-ordered-process-property-read` (second instance) | Standard — anchor: no source label; runtime behavior is a blocking class, not a critical anchor | `46100b7`..the batch | fixed in the implementation batch by the class's structural fix — the child exports `execution_state_writes` and `establishing_execution_state_writes`, read off the two in-process writer sets; witnesses the sibling cells of the differential, `test_a_calls_later_leg_write_is_what_it_guarantees_not_what_it_may_write` (now asserting the twin's code), `test_a_cache_a_later_leg_child_may_write_is_a_branch_order_error_as_in_its_twin`; mutants `test_a_sibling_childs_establishing_writer_left_uncounted_re_admits_the_reader`, `test_a_sibling_childs_writer_left_out_of_its_leg_serves_the_wrong_code` |
| SELF-186-02 | the implementation batch's grandchild matrix | "A Data Passthrough middle that fails its child's dropped document-property read exports the property as an entry requirement; validated under that seed, the middle serves a read before write the flattened twin, whose read is a different-document-copy scope error, never serves" | unlabelled (self-found) | runtime behavior | `entry-requirement-not-measured-against-an-entry-seed` | Standard — anchor: no source label; a served refusal code is blocking-class behaviour; refused on both sides | `46100b7`..the batch | fixed in the implementation batch — a document-property requirement is exported only where an entry seed repairs one of its reads, compared by read site; witness `test_a_reader_one_call_deeper_earns_its_twins_lineage_codes[ddp]` (ten cells moved from an extra code to an exact match); mutant `test_an_entry_requirement_no_seed_repairs_exported_anyway_adds_a_code_the_twin_lacks`; #184's `test_a_read_of_retrieved_documents_is_no_entry_requirement_by_that_rule_alone` now reverts both guards of that row |
| SELF-186-03 | the implementation batch's grandchild matrix | "An execution-scoped requirement a middle exports loses the leg order it was found in: where the middle writes the property on a later leg than its call and the caller does not establish it, the caller serves a read before write while the flattened twin serves Branch order" | unlabelled (self-found) | runtime behavior | `child-contract-omits-a-leg-ordered-process-property-read` (third instance, covered by the structural fix) | Standard — anchor: no source label; a served refusal code; refused on both sides | `46100b7`..the batch | fixed in the implementation batch — `required_read_leg_orders` carries each requirement's in-process later-leg answer and the call reports it; witness `test_a_reader_one_call_deeper_earns_its_twins_lineage_codes[dpp]`; mutants `test_a_passed_on_later_leg_order_dropped_serves_the_wrong_code`, `test_a_passed_on_row_flag_ignored_serves_the_wrong_code`; the two-reference oracle row `required_read_leg_orders` |
| SELF-186-04 | the implementation batch's consumer sweep of the unknown profile fact | "A passthrough child that stages its caller's documents into a document cache whose profile is UNKNOWN records the unstated consumption (None), which every call skips, so a caller whose documents carry a proved profile is admitted while the flattened twin's Add to Cache refuses it. Pre-existing since #184 correction batch 32 for raw-XML caches; reachable for every unreadable stored cache once item 3 lands" | unlabelled (self-found) | runtime behavior | `unknown-profile-read-as-unstated` | Standard — anchor: no source label; runtime behavior is a blocking class | `46100b7`..the batch | fixed in the implementation batch — the child records a requirement no proved profile satisfies, refused only where the documents' profile is proved, passed on otherwise, and treated as unstated by the prefix and cache-seed rules; witness `test_documents_a_caller_proves_staged_into_an_unknown_cache_are_refused_at_the_call`; mutant `test_the_unknown_requirement_spelled_as_none_re_admits_the_caller` |
| CDX-186-r1-01 | Codex commit review round r1 (loop 2, initial), run `cdx-review.zKZCQy`, `--base 46100b7866c7962b53cf62a400276ae30258ccc6`, scope `branch diff against 46100b7866c7962b53cf62a400276ae30258ccc6 (46100b7)` head=2e17b7e4f66093c4830bf7d62ad2658c8cabe533 dirty=false, collected `STATUS: completed`, teardown `confirmed stopped`; archived `docs/architecture/evidence/issue-186/commit-reviews/cdx-review.zKZCQy/`, finding 1 of 1 | "Check unknown-profile requirements against the incoming profile — `lineage.py:2817-2818`. For a passthrough caller with an unproved input profile, a passthrough child with a map requiring P1 on its first Branch leg and an unknown-profile cache write on another exports `('$ref:P1', 'urn:boomi:unknown-profile')`. Processing P1 first temporarily narrows `stream.identity`, so this check incorrectly reports `PROCESS_IR_SEMANTIC_PROFILE_MISMATCH`. Reversing the legs admits the caller, while the child and flattened twin are admitted in both orders. Evaluate UNKNOWN against the original reaching profile rather than an identity inferred from an earlier child requirement." | P2 | runtime behavior | `requirement-judged-against-a-stream-an-earlier-requirement-narrowed` — first instance | Standard — anchor: source label P2, not P0/P1/Critical/High; runtime behavior is a blocking class | `46100b7`..`2e17b7e` | fixed in correction batch 1 — reproduced before the fix on the reviewer's shape: with the map leg first the caller was refused `PROCESS_IR_SEMANTIC_PROFILE_MISMATCH` at `/body/steps/1/legs/0/terminal/process_ref` while the child and the twin were admitted, and with the legs reversed all three were admitted. The token is now judged against the documents as they arrive at the call; a stated profile still narrows the stream for the requirements after it. Sibling, one-line justified: two stated profiles on two legs (P1 then P2) are still a mismatch at the call — #184's contradiction rule, since no documents satisfy both and every caller of the twin is refused too; the token states no profile, so it is the only requirement the narrowing could wrongly fail. Witness `test_the_unknown_requirement_is_judged_against_the_arriving_documents_in_either_leg_order` (both orders, child and twin) |

## Checkpoints (written in flight at every third evaluation of each loop)

| Loop | Evaluation (window / cumulative) | SHA (+dirty) | Outcome | Rationale |
| --- | --- | --- | --- | --- |

## Deferrals

Pointer-only — reason class, placement and lineage live on the finding row and in the filed issue.

None so far.

## Gate results (chronological)

| Gate | Scope | Tree | Result |
| --- | --- | --- | --- |
| Blast radius, goldens | all 80 active goldens rendered through each arm's own registry, hash seeds 0 and 1 (`blast/goldens.py`) | `e22b8fa` vs the uncommitted implementation | 80 of 80 byte-identical across both arms and both seeds, and to the committed files |
| Blast radius, 216-cell matrix | `blast/matrix.py` (the #184 batch 21a matrix kit) on each arm | same | 216 cells, 0 moved |
| Blast radius, six corpora | 89,444 requests replayed per arm (`blast/run_arm.py`, `blast/launch.sh`, inputs hashed in `blast/inputs.sha256`), compared by `blast/compare.py` (`blast/compare.txt`) | same | no admission became a refusal and no refusal an admission; 20,567 already-refused requests moved codes, the Branch-order code added beside or in place of a missing-writer or read-before-write code where a child reads execution state on one leg and writes it on a later one (SELF-186-01's cache sibling and SELF-186-03) — the twin's code for those legs; the kit builds no twin for these requests, whose children call themselves |
| Blast radius, x29 universe | QA's `x29_hunt.py` per arm, graded by `grade_x29.py` (`blast/x29/grade.txt`) | same | 4,872 cells, 0 moved: 4,720 match and the 152 refused-both residuals unchanged, violations 0 |
| Stage-1 QA r1 (loop 1) | live, `boomi-qa-tester`, through `build_integration` typed plan, typed compile and raw apply on the `renera` account; kit `.claude/agent-memory/boomi-qa-tester/harness/186r1/` (gitignored), capture records `.claude/agent-memory/boomi-qa-tester/evidence/issue-186/` | the uncommitted implementation | clean, no findings: 26 property-read compositions each serve their twin's code at the caller's call, the 12 newly refused were admitted at the baseline, every refusal made zero writes; item 3 refuses a stored profile other than the documents' on all three routes and admits the matching and the unprofiled stored cache, leaving the reused cache at version 1; all 45 live components removed |
| Stage-2 review r1 (loop 2, initial) | `cdx-review.zKZCQy`, `--base 46100b7`, head `2e17b7e` | `2e17b7e` | one P2 finding, CDX-186-r1-01 |

## Evidence index

Collected run directories and the blast-radius kit are archived under
`docs/architecture/evidence/issue-186/` with `index.jsonl` + `SHA256SUMS`, in the batch that collects
them.

## Final-tree validation (filled at close; every roster gate current on the FINAL sha)

| Gate | Evidence (quoted output / run URL / archived round) | SHA |
| --- | --- | --- |

## Final report
