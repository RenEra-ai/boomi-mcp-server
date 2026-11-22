# cap159-s3-bound-profiles — provenance

Capture date: 2026-11-22 (UTC, date only). Account: the disposable renera demo account (written as
`demo-account-159`). Captured for #159 S3 as a canonical safe-edit witness: a REST GET operation with a stored
`responseProfile`, a REST **PATCH** operation with a stored `requestProfile`, and a `transform.map` between those two
JSON profiles. PATCH was used, not POST: the target accepted it and the run was green.

## What was built, and how

All components were authored by this repo's own builders at `ed17dde` (branch `codex/issue-159-s1`). This was fixture
provisioning, not the code under test.

- The REST connection `b0d70f36` came from `manage_connector(action="create")`. It uses
  `connector_type=rest`, `base_url=http://host.docker.internal:8081` (the local cds-mock no-auth service) and
  `auth=NONE`.
- The other components came from `build_from_archetype("api_to_api_sync")`, then `build_integration` (typed
  `intent_kind: integration_spec`: plan, then compile, then apply with the compile hash bound). Both bindings reuse the
  connection above by id. The archetype produced these components:
  - JSON profile `dfa08e0a` ("Source Profile"): Root object {name: character, active: boolean, key: character}.
  - REST GET operation `3cbe300a` on `/admin/refdata/api/v1/rdcds/currency/QA_currency_E0D9B5299C`, with
    `returnApplicationErrors="false"`, so a non-2xx response fails the shape instead of passing through.
  - JSON profile `d65fda99` ("Target Profile"): Root object {name: character}.
  - Map `a3e236b6`, with one direct mapping, source `Root/Object/key` to target `Root/Object/name`.
  - REST PATCH operation `db9108fc` on the same path.
  - Process `ef865f0a`: start (no data), then the GET connector, then the map, then the PATCH connector, then stop.

Component names carried the nonce prefix `_TEST_S3C1`.

## Proof of operability

The process was packaged and deployed to the account's local Docker atom, then executed manually:
`execution-76ba0751-3708-4169-8fc5-bcc055dc6c97-2026.11.22`, **status COMPLETE**.

- Inbound documents: 1. Inbound errors: 0. Outbound documents: 2.
- ExecutionConnector rows (see `execution-record.json`): two REST EXECUTE rows, each with successCount 1 and
  errorCount 0 (sizes 210 and 199), plus the no-data start.
- The data really went through the map. Before the run the mock item's `name` was `"QA currency"`. After the run it was
  `"QA_currency_E0D9B5299C"`, which is the `key` value the GET returned, mapped to `name` and sent by PATCH.
  `modifiedOn` advanced to `2026-11-22T15:48:49.983Z`, the execution time. The item was restored to `"QA currency"`
  afterwards.

The process was undeployed and its package deleted right after the run. Every component listed here was then deleted
from the account (soft delete).

## Independence caveat

The XML **shape** is not independent of the builders: these bytes are what the platform stored and served back for
builder-emitted XML. What makes the set evidence rather than an echo of the implementation is that a platform runtime
executed it green, with the observable data effect above. Boomi stores unvalidated XML, so a create alone would not
prove anything.

## Capture method

Each component was read once with `query_components(action="get")` through the MCP tool layer (profile `renera`,
read-only), after the green run and before any delete. The execution record came from
`invoke_boomi_api` `POST ExecutionRecord/query` and `POST ExecutionConnector/query`, filtered with `executionId EQUALS`.
`records.json` has one record per component XML, with the sha256 of the scrubbed bytes and the byte length. Each
record carries `binds_placeholder_ids: []`, so the set stays compatible with
`tests/_issue_157_flows_accounting.replay_identity_captures()`. That loader opens `<capture>.xml` for every record, so
the execution record is deliberately not listed in `records.json`. Its details are:

- File: `execution-record.json`.
- Execution: `execution-76ba0751-3708-4169-8fc5-bcc055dc6c97-2026.11.22`, status COMPLETE.
- sha256: `fc338b7b634e05ad66c0d8dbb1d83060b24721e401fca1262e5001dcda5ef7ff`, 2465 bytes.

## Scrubbing (same convention as `cap159-s1-reuse-identity/`)

- Component XML: `createdBy`/`modifiedBy` were replaced with `redacted@example.invalid`, and
  `folderName`/`folderFullPath` with `Demo-Account-Folder`. `folderId` and `branchId` were kept, as in the S1 set.
  No credential values were present: auth is NONE and `<bns:encryptedValues/>` is empty.
- Execution record: `account` was replaced with `demo-account-159`, and `atomId`/`atomName` with
  `demo-atom-159`/`demo-local-atom`. The account, atom and id fields were dropped from the ExecutionConnector rows.
- Leak sweep: the account id, its suffix, the user email and domain, the real folder name, the atom id and the atom
  name were searched for in the scrubbed files and found 0 times. The raw copies (positive control) contained all of
  them.

## Verified bindings (attribute paths in the captured XML)

| File | Path | Value |
|---|---|---|
| op-get-3cbe300a.xml | `bns:Component/bns:object/Operation/Configuration/GenericOperationConfig/@responseProfile` | dfa08e0a-479c-4aa8-b043-2113af48dad4 |
| op-get-3cbe300a.xml | `.../GenericOperationConfig/@responseProfileType` | json |
| op-patch-db9108fc.xml | `bns:Component/bns:object/Operation/Configuration/GenericOperationConfig/@requestProfile` | d65fda99-ae94-4608-b830-6754ebf229c5 |
| op-patch-db9108fc.xml | `.../GenericOperationConfig/@requestProfileType` | json |
| map-a3e236b6.xml | `bns:Component/bns:object/Map/@fromProfile` | dfa08e0a-479c-4aa8-b043-2113af48dad4 |
| map-a3e236b6.xml | `bns:Component/bns:object/Map/@toProfile` | d65fda99-ae94-4608-b830-6754ebf229c5 |

The GET operation has no `requestProfile` attribute, and the PATCH operation has no `responseProfile` attribute.
`SHA256SUMS` in the parent directory is tracked and was **not** updated. The capture's handback reply gives the lines
to add.

What the green run shows: the map's `fromProfile`/`toProfile` were exercised end to end, and both operations ran with
their `responseProfile`/`requestProfile` attributes in place. It does **not** show that the REST runtime reads those
two operation attributes. The PATCH operation has `returnApplicationErrors="true"`, so its success rests on the mock
item changing, not on the connector row alone.
