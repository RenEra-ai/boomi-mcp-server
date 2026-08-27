# Fixture provenance — cap155-e4-trace-status

The connector-action component that issued the captured request was NOT authored by the
implementation under test.  It was created by POSTing the banked E2 component XML through
the raw `/Component` escape hatch (`manage_component(action="create", config={"xml": ...})`),
substituting exactly two values: the root `name=` and the `<field id="path">` `value=`.

| item | value |
| --- | --- |
| banked source (by reference, NOT copied) | `docs/architecture/evidence/issue-155/captures/cap155-e2-trace/operation_component.xml` |
| banked source sha256 | `e474e525829390525414c318284a62bf9c3808acc57474b58dba05095dc0bd1a` |
| banked source component id (soft-deleted in the account) | `9632e4a2-2fce-441c-ad1b-c507e3a4a22d` |
| banked source `customOperationType` | `TRACE` |
| derived component id (this run) | `0225ac9f-d288-412b-b76b-9761933b0139` |
| derived component name | `_TEST_155E4_TRACE_trace_20260827132830` |
| derived stored path | `/admin/cdscm/api/v1/clients/6186425e-a59a-4a19-b616-b4e18a58e0ce` |
| derived `customOperationType` (read back) | `TRACE` |
| substitutions applied | name `_TEST_155E2_TRACE_20260826070453` -> `_TEST_155E4_TRACE_trace_20260827132830`; path `/admin/cdscm/api/v1/clients/ee9eaee2-bc25-46f6-b9ee-891252654736` -> `/admin/cdscm/api/v1/clients/6186425e-a59a-4a19-b616-b4e18a58e0ce` |
| root attributes stripped for create | componentId, version, createdDate, createdBy, modifiedDate, modifiedBy, deleted, currentVersion, branchName, branchId |

The `sync_pipeline` fetch stage reused a GET operation derived the same way from
`docs/architecture/evidence/issue-155/captures/cap155-e2-head/source_operation_component.xml`
(sha256 `279806a7551e99079c1879fac6b8ca36ce5c64d2c408b16a01ce8a2957205add`), created as `_TEST_155E4_src_GET_20260827132830` = `615f52c8-0b38-4614-ae81-7e0ee484011e`.

The `build_integration` apply that built the process reported `status:"reused"` for every
operation component and the component readback was byte-identical before and after the
apply, so the implementation's operation builder never authored what executed:

    op_trace   reused  0225ac9f-d288-412b-b76b-9761933b0139   (== derived component id: True)
    op_src    reused  615f52c8-0b38-4614-ae81-7e0ee484011e   (== derived component id: True)

Email addresses in `operation_component.xml` are redacted to `redacted@example.invalid`;
the SHA256SUMS in this directory cover the redacted bytes as written.
