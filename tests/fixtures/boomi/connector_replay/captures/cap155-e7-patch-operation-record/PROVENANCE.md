# cap155-e7-patch-operation-record

A double-attested REST **PATCH** capture taken so that an `OperationContractRecordV1` can rest on it. Unlike every earlier capture under this issue, the operation under test is a **durable account fixture**, not a component the capture created and deleted.

## Why the operation is durable

`OperationContractRecordV1.operation_identity` binds a component id AND a version, and `project_idempotency_contracts` matches on that PAIR. A record minted from an E5/E6-style capture would cite a component the capture's own teardown soft-deleted, so it could never place a contract on this account. The PATCH operation below was therefore provisioned as a standing fixture in its own step BEFORE the capture, referenced unchanged by both executions, and left in place afterwards.

| role | component | id | version before | version after |
|---|---|---|---|---|
| operation (durable) | `_QA_FIXTURE_155_PATCH_client` | `485f3b01-fba5-4850-8281-2fc19decefb7` | 1 | 1 |
| connection (durable, pre-existing) | `Sandbox CDS Mock REST (no-auth)` | `2fe488e4-3169-4529-9515-d854570c8ffc` | 1 | 1 |
| source GET (scaffolding, deleted) | `_TEST_155E7_src_GET_20260902053242` | `8cb60420-7fdf-4b7d-adb6-06331a007d80` | 1 | deleted at teardown |
| process (scaffolding, deleted) | `_TEST_155E7_proc_PATCH_20260902053242` | `d658519f-7d7e-4a98-9e6d-6362ae289605` | 1 | deleted at teardown |

Version and byte stability across the capture: operation `version_unchanged=True` `bytes_unchanged=True`; connection `version_unchanged=True` `bytes_unchanged=True`. Both readbacks are the account's own `GET /Component/{id}` bytes, before and after.

## Admissibility

Both conjuncts of the E5/E6 bar:

1. **Two executions** of the same operation against the same live resource.
2. **A counterparty access log covering both**, each logged request attributed to exactly one execution by a HOST-CLOCK window — never by peer IP, because the atom and the QA host both appear as `192.168.65.1`.

| execution | id | platform status | counterparty | logged at |
|---|---|---|---|---|
| run1 | `execution-29bb7002-e230-48ff-8c4d-cd005fd32139-2026.09.02` | COMPLETE | 200 OK | `2026-09-02T05:33:04.854923711Z` |
| run2_identical_replay | `execution-bb0c2461-9716-4d53-84de-9be00425796d-2026.09.02` | COMPLETE | 200 OK | `2026-09-02T05:33:23.138927053Z` |

- `exactly_one_request_per_execution`: **True**
- `log_covers_both_executions`: **True**
- `window_overlaps`: `[]` (empty ⇒ no request is attributable to both)
- `agrees_with_string_comparison_pass`: **True** — the lexicographic pass and the parsed-datetime pass return the same attribution
- clock-skew control: a host-issued GET at `2026-09-02T05:33:02.372919+00:00` was logged at `2026-09-02T05:33:02.383541627Z`, inside the host bracket (`stamp_within_host_bracket=True`, round trip 12 ms)

## Staged readbacks: a positive subject and an untouched control

| subject | R0 before | R1 between | R2 after |
|---|---|---|---|
| target — **positive subject** | `626d379c30f8` (200) | `5d0c42fda31b` (200) | `5d0c42fda31b` (200) |
| template — body source | `8d33f3a55c04` (200) | `8d33f3a55c04` (200) | `8d33f3a55c04` (200) |
| control — **negative control** | `3668b82e92ff` (200) | `3668b82e92ff` (200) | `3668b82e92ff` (200) |

- first call moved the target's business state: **True**
- the replay converged (R1 == R2 on business state): **True**
- the control is byte-identical at all three moments: **True** — so "nothing else moved" is measured, not assumed
- `modifiedOn` did move R1→R2 (**True**) while `createdOn` held (**True**): the replay was really applied, and converged anyway. A raw sha256 would have called this a duplicate effect; the business-state key strips the volatile fields.

## Fixture provenance

Every component was created by POSTing **banked, proven-operable** component XML from `cap155-e5-patch-attested`, whose own double execution ran green (COMPLETE/COMPLETE, counterparty 200/200). Only identity and folder attributes were stripped and the root `name=` substituted; operation and connection references were remapped to this account. **No path byte was edited** — the counterparty honours a caller-supplied `key`, so the resources were re-seeded at the exact keys the banked operation XML already points at. The builder under test authored nothing here.

## The capture digest and its exclusion set

`capture.summarize()` digests every regular file in this directory, name and bytes. A record stored inside the directory it digests has no fixed point, so exactly two files sit outside the digest — the two that cannot precede their own input:

- `operation_record.json`
- `record_derivation.json`

Everything else, `PROVENANCE.md` and `records.json` included, is inside it — the E5/E6 convention. `record_derivation.json` carries the digest, the exact file list it covers, and a reproduction that removes those two files and re-runs the summariser. Note this differs from the E5/E6 captures, whose digests were computed by `ingest` over the complete directory because no record lived in it.

## What the record claims, and what computed it

Every field is produced by the repo's own code — see `record_derivation.json` for the values and the call that produced each:

| field | authority |
|---|---|
| `account_scope_hash` | `ingest._account_scope_hash(summary)` → `digests.account_scope_hash(account_id)`, the account id read out of the execution records |
| `capture` | `ingest._capture_reference(summary, side_effect)` |
| `operation_identity.config_digest` / `connection_identity.config_digest` | `digests.component_config_digest_v1(live readback XML, kind, family)` |
| `route_coverage.route_digests[0]` | `digests.route_digest_v1(live connection XML, live operation XML)` |
| `family` | `registry.family_for(<the live component's platform subType>)` |
| `action` | the live operation's own `customOperationType`, cross-checked against the counterparty log's method by the summariser |
| `operation_identity.version` / `connection_identity.version` | the account's post-capture `GET /Component/{id}` readback |
| `record_digest` | sha256 over `digests._canonical(record minus record_digest)` with a `OperationContractRecordV1\0` domain separator — **the repo publishes no minter for this field**, so the algorithm is stated here in full |

## Known gap: the registry publishes no semantics definition

`registry._refuse_unresolvable_records` requires an operation record's `(semantics_id, semantics_revision)` to be published by the registry, and the packaged `registry_v1.json` ships `semantics_definitions: []`. The definition this record cites is carried in `record_derivation.json` under `semantics_definition`, derived from the measured mechanism, key scope and duplicate guarantee. Ingesting this record without also publishing that definition will be refused — correctly.

