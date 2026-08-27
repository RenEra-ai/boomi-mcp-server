# Fixture provenance — cap155-e4-negative-control

The connector-action component that issued the captured request was NOT authored by the
implementation under test.  It was created by POSTing the banked E2 component XML through
the raw `/Component` escape hatch (`manage_component(action="create", config={"xml": ...})`),
substituting exactly two values: the root `name=` and the `<field id="path">` `value=`.

| item | value |
| --- | --- |
| banked source (by reference, NOT copied) | `docs/architecture/evidence/issue-155/captures/cap155-e2-delete/operation_component.xml` |
| banked source sha256 | `40633cb19b978923d1a0594b6f04ebc2b6c7394259b4434be7c5cdfada7228e4` |
| banked source component id (soft-deleted in the account) | `b065d785-55a6-480b-a355-bd90f2ce37b1` |
| banked source `customOperationType` | `DELETE` |
| derived component id (this run) | `b9701599-b09a-4ce8-a935-cb5303f1071b` |
| derived component name | `_TEST_155E4_DELETE_negctl_20260827132830` |
| derived stored path | `/admin/cdscm/api/v1/clients` |
| derived `customOperationType` (read back) | `DELETE` |
| substitutions applied | name `_TEST_155E2_DELETE_20260826070453` -> `_TEST_155E4_DELETE_negctl_20260827132830`; path `/admin/cdscm/api/v1/clients/df0c8de9-3a8b-4e13-b9a6-e08819540b24` -> `/admin/cdscm/api/v1/clients` |
| root attributes stripped for create | componentId, version, createdDate, createdBy, modifiedDate, modifiedBy, deleted, currentVersion, branchName, branchId |

The `sync_pipeline` fetch stage reused a GET operation derived the same way from
`docs/architecture/evidence/issue-155/captures/cap155-e2-head/source_operation_component.xml`
(sha256 `279806a7551e99079c1879fac6b8ca36ce5c64d2c408b16a01ce8a2957205add`), created as `_TEST_155E4_src_GET_20260827132830` = `615f52c8-0b38-4614-ae81-7e0ee484011e`.

The `build_integration` apply that built the process reported `status:"reused"` for every
operation component and the component readback was byte-identical before and after the
apply, so the implementation's operation builder never authored what executed:

    op_negctl   reused  b9701599-b09a-4ce8-a935-cb5303f1071b   (== derived component id: True)
    op_src    reused  615f52c8-0b38-4614-ae81-7e0ee484011e   (== derived component id: True)

Email addresses in `operation_component.xml` are redacted to `redacted@example.invalid`;
the SHA256SUMS in this directory cover the redacted bytes as written.
