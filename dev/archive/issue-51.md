# Issue #51 M3.R1a Try/Catch + DLQ Emission Plan

## Summary
Implement only the verified zero-retry DLQ path. `reliability.retry_count > 0` continues to raise `PROCESS_RETRY_UNVERIFIED`; `retry_count == 0` may emit a Try/Catch wrapper when `dlq.mode` / `on_failure.mode` is `document_cache_ref` or `error_subprocess_ref`.

Static-only note: no commands, tests, MCP calls, or external services were run in this planning session.

## File-By-File Changes

### `tools/process_builders/process_flow_builder.py`
- Change the existing reliability guard:
  - Keep raising `PROCESS_RETRY_UNVERIFIED` for any `reliability.retry_count > 0`.
  - Stop raising it for `retry_count == 0` with DLQ mode `document_cache_ref` or `error_subprocess_ref`.
  - Continue rejecting unsupported DLQ/on-failure modes with the existing error style.
- Add a small helper such as `_should_emit_try_catch(reliability)`:
  - Returns true only when retry count is zero and DLQ/on-failure mode is one of the R1a-supported modes.
  - Defaults missing/disabled DLQ config to false.
- Add a catcherrors shape emitter using the verified structure:
  ```xml
  <shape shapetype="catcherrors" ...>
    <configuration>
      <catcherrors catchAll="true" retryCount="0" />
    </configuration>
    <dragpoints>
      <dragpoint identifier="default" toShape="{first_try_shape_id}" />
      <dragpoint identifier="error" toShape="{first_catch_shape_id}" />
    </dragpoints>
  </shape>
  ```
  Preserve the builder’s existing conventions for generated shape ids, labels, coordinates, `image`, `name`, `userlabel`, and dragpoint ordering.
- Insert the wrapper between Start and the normal first processing shape:
  - Current normal path: `Start -> source -> transform -> target -> Stop`.
  - New wrapped path: `Start -> catcherrors`.
  - Try dragpoint `identifier="default"` connects to the existing first normal shape.
  - Existing normal chain remains unchanged after that: `source -> transform -> target -> Stop`.
- Add Catch branch emission:
  - Catch dragpoint `identifier="error"` connects to the first catch-path shape.
  - If an `error_classifier` fragment is configured/enabled, emit it first.
  - Emit the `dlq_writer` fragment as a `doccacheload` shape after the optional classifier.
  - Connect catch-path terminal shape to the existing Stop shape, unless the file’s current pattern requires a separate generated Stop for alternate branches.
- Reuse primitive helpers from `tools/process_builders/primitives/` instead of hand-duplicating fragment XML.
- Acceptance criteria:
  - Zero-retry DLQ specs emit `catcherrors`.
  - The wrapper has `shapetype="catcherrors"`, `catchAll="true"`, `retryCount="0"`, Try dragpoint `identifier="default"`, Catch dragpoint `identifier="error"`.
  - Retry counts `1..5` still fail with `PROCESS_RETRY_UNVERIFIED`.

### `tools/process_builders/primitives/dlq_writer.py`
- Do not redesign the primitive.
- Use its existing public fragment builder/API from `ProcessFlowBuilder`.
- If the primitive currently exposes only fragment XML, adapt `ProcessFlowBuilder` minimally to insert that fragment as the catch-path `doccacheload`.
- Acceptance criteria:
  - Catch branch contains exactly one DLQ writer `doccacheload` for the supported DLQ config.

### `tools/process_builders/primitives/error_classifier.py`
- Do not change behavior unless its existing API needs a tiny compatibility shim for `ProcessFlowBuilder`.
- Use it only when the reliability/on-failure config requests classification.
- Acceptance criteria:
  - Golden XML without classifier omits it.
  - Golden XML with classifier includes it before the DLQ writer if a test covers that path.

### `tools/process_builders/primitives/rest_send_with_retry.py`
- No functional changes for R1a.
- Confirm during implementation that retry behavior remains gated outside this primitive.
- Acceptance criteria:
  - No retry-count `> 0` process can be emitted through the new Try/Catch path.

### `tools/process_builders/primitives/__init__.py`
- Export `dlq_writer` / `error_classifier` helpers only if `ProcessFlowBuilder` cannot already import them through existing conventions.
- Keep this diff minimal.

### `tools/build_integration.py`
- In `get_schema_template`, promote `reliability.on_failure` out of `deferred_fields`.
- Leave `execution.trigger` and `execution.run_metadata` deferred.
- Add supported schema/template shape for only R1a modes:
  ```json
  {
    "reliability": {
      "retry_count": 0,
      "on_failure": {
        "mode": "disabled",
        "supported_modes": [
          "disabled",
          "document_cache_ref",
          "error_subprocess_ref"
        ],
        "document_cache_ref": {
          "component_id": "",
          "operation_id": ""
        },
        "error_subprocess_ref": {
          "component_id": ""
        },
        "error_classifier": {
          "enabled": false
        }
      }
    }
  }
  ```
- Match the repo’s existing schema-template style exactly: if templates use examples instead of JSON Schema, add examples; if they use field descriptors, add descriptors.
- Acceptance criteria:
  - `reliability.on_failure` no longer appears in `deferred_fields`.
  - Only the two wired modes are documented as supported.

### Golden fixture
- Add fixture beside existing golden XML fixtures, using the repo’s current naming convention. Planned path:
  - `tests/fixtures/golden_xml/try_catch_dlq_process.xml`
- Content outline:
  ```xml
  <process ...>
    <shapes>
      <shape shapetype="start" ...>
        <dragpoints>
          <dragpoint toShape="{catcherrors_id}" />
        </dragpoints>
      </shape>

      <shape shapetype="catcherrors" ...>
        <configuration>
          <catcherrors catchAll="true" retryCount="0" />
        </configuration>
        <dragpoints>
          <dragpoint identifier="default" toShape="{source_id}" />
          <dragpoint identifier="error" toShape="{dlq_or_classifier_id}" />
        </dragpoints>
      </shape>

      <shape shapetype="{source}" ... />
      <shape shapetype="{transform}" ... />
      <shape shapetype="{target}" ... />
      <shape shapetype="doccacheload" ...>
        <!-- existing dlq_writer fragment configuration -->
      </shape>
      <shape shapetype="stop" ... />
    </shapes>
  </process>
  ```
- If the existing golden fixtures include full component wrappers, namespaces, or deterministic coordinates, copy that exact surrounding structure and only add the Try/Catch and catch-path shapes.

### Golden tests
- Add tests beside existing golden XML tests, matching current style. Planned file:
  - `tests/test_process_flow_builder_golden.py`
- Add:
  - `test_try_catch_dlq_document_cache_matches_golden`
  - `test_try_catch_dlq_keeps_retry_count_positive_gated`
  - `test_schema_template_promotes_reliability_on_failure`
- Assertions:
  - Rendered XML matches the new golden fixture byte-for-byte or normalized-XML-to-normalized-XML, whichever existing tests use.
  - Positive retry count still raises/returns `PROCESS_RETRY_UNVERIFIED`.
  - Schema template lists `reliability.on_failure` as supported and leaves `execution.trigger` / `execution.run_metadata` deferred.

## Acceptance Criteria
- No new dependencies.
- Minimal diff limited to process emission, schema-template exposure, fixture, and tests.
- Existing non-DLQ process XML remains unchanged.
- R1a emits verified Try/Catch only for `retry_count == 0`.
- Catch path writes to DLQ through the existing primitive layer.
- Golden XML test proves the emitted shape structure and branch wiring.
