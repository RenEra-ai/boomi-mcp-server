# Fixture provenance — cap155-e4-options-status

The connector-action component that issued the captured request was NOT authored by the
implementation under test.  It was created by POSTing the banked E2 component XML through
the raw `/Component` escape hatch (`manage_component(action="create", config={"xml": ...})`),
substituting exactly two values: the root `name=` and the `<field id="path">` `value=`.

| item | value |
| --- | --- |
| banked source (by reference, NOT copied) | `docs/architecture/evidence/issue-155/captures/cap155-e2-options/operation_component.xml` |
| banked source sha256 | `b4d4f02e1ebc1a58efd0e80066b50c328e7006aeeb3aa5e0c16b2a1c90f16f64` |
| banked source component id (soft-deleted in the account) | `c50ff6ac-858b-4ab0-bd82-8f6168a5a3f0` |
| banked source `customOperationType` | `OPTIONS` |
| derived component id (this run) | `65d3129b-c4f7-441d-9172-999c6a282177` |
| derived component name | `_TEST_155E4_OPTIONS_options_20260827132830` |
| derived stored path | `/admin/cdscm/api/v1/clients/61ae0e89-bed1-466f-9cc0-5909c05204a8` |
| derived `customOperationType` (read back) | `OPTIONS` |
| substitutions applied | name `_TEST_155E2_OPTIONS_20260826070453` -> `_TEST_155E4_OPTIONS_options_20260827132830`; path `/admin/cdscm/api/v1/clients/ee9eaee2-bc25-46f6-b9ee-891252654736` -> `/admin/cdscm/api/v1/clients/61ae0e89-bed1-466f-9cc0-5909c05204a8` |
| root attributes stripped for create | componentId, version, createdDate, createdBy, modifiedDate, modifiedBy, deleted, currentVersion, branchName, branchId |

The `sync_pipeline` fetch stage reused a GET operation derived the same way from
`docs/architecture/evidence/issue-155/captures/cap155-e2-head/source_operation_component.xml`
(sha256 `279806a7551e99079c1879fac6b8ca36ce5c64d2c408b16a01ce8a2957205add`), created as `_TEST_155E4_src_GET_20260827132830` = `615f52c8-0b38-4614-ae81-7e0ee484011e`.

The `build_integration` apply that built the process reported `status:"reused"` for every
operation component and the component readback was byte-identical before and after the
apply, so the implementation's operation builder never authored what executed:

    op_options   reused  65d3129b-c4f7-441d-9172-999c6a282177   (== derived component id: True)
    op_src    reused  615f52c8-0b38-4614-ae81-7e0ee484011e   (== derived component id: True)

Email addresses in `operation_component.xml` are redacted to `redacted@example.invalid`;
the SHA256SUMS in this directory cover the redacted bytes as written.
