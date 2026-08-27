# Fixture provenance — cap155-e4-head-status

The connector-action component that issued the captured request was NOT authored by the
implementation under test.  It was created by POSTing the banked E2 component XML through
the raw `/Component` escape hatch (`manage_component(action="create", config={"xml": ...})`),
substituting exactly two values: the root `name=` and the `<field id="path">` `value=`.

| item | value |
| --- | --- |
| banked source (by reference, NOT copied) | `docs/architecture/evidence/issue-155/captures/cap155-e2-head/operation_component.xml` |
| banked source sha256 | `a1e8c0cfaf9e8a30f4916ca1a4ab7da71d854fab6e095c32da4c9977bae79849` |
| banked source component id (soft-deleted in the account) | `a4589b6d-801d-47b7-9398-6d19f0ea2936` |
| banked source `customOperationType` | `HEAD` |
| derived component id (this run) | `d95ed2c3-720a-4f8d-a6c1-e01cce6aeae3` |
| derived component name | `_TEST_155E4_HEAD_head_20260827132830` |
| derived stored path | `/admin/cdscm/api/v1/clients/71cb4f7c-c1a7-482e-97a3-7d7a168b667b` |
| derived `customOperationType` (read back) | `HEAD` |
| substitutions applied | name `_TEST_155E2_HEAD_20260826070453` -> `_TEST_155E4_HEAD_head_20260827132830`; path `/admin/cdscm/api/v1/clients/ee9eaee2-bc25-46f6-b9ee-891252654736` -> `/admin/cdscm/api/v1/clients/71cb4f7c-c1a7-482e-97a3-7d7a168b667b` |
| root attributes stripped for create | componentId, version, createdDate, createdBy, modifiedDate, modifiedBy, deleted, currentVersion, branchName, branchId |

The `sync_pipeline` fetch stage reused a GET operation derived the same way from
`docs/architecture/evidence/issue-155/captures/cap155-e2-head/source_operation_component.xml`
(sha256 `279806a7551e99079c1879fac6b8ca36ce5c64d2c408b16a01ce8a2957205add`), created as `_TEST_155E4_src_GET_20260827132830` = `615f52c8-0b38-4614-ae81-7e0ee484011e`.

The `build_integration` apply that built the process reported `status:"reused"` for every
operation component and the component readback was byte-identical before and after the
apply, so the implementation's operation builder never authored what executed:

    op_head   reused  d95ed2c3-720a-4f8d-a6c1-e01cce6aeae3   (== derived component id: True)
    op_src    reused  615f52c8-0b38-4614-ae81-7e0ee484011e   (== derived component id: True)

Email addresses in `operation_component.xml` are redacted to `redacted@example.invalid`;
the SHA256SUMS in this directory cover the redacted bytes as written.
