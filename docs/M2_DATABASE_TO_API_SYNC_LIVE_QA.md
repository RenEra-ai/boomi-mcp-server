# M2 `database_to_api_sync` Live QA Runbook

Manual QA workflow + evidence template for validating the M2 vertical slice
against a live Boomi account. Pairs with the local end-to-end tests
(`tests/patterns/test_database_to_api_sync_e2e.py`,
`tests/test_build_integration_wrapper.py`). Run this when you need real-Boomi
confidence beyond the mocked local suite.

- **Milestone / Issue:** M2.10 / Issue #30
- **Default QA profile:** `reneraai-5RO3DD` (Renera test account — the only
  profile that may be mutated, and only with explicit operator approval)
- **Reference-only profile:** `work` (read-only; never mutate)
- **Scope:** archetype assembly → transformation review → plan → dry-run apply →
  *optional* explicitly-approved real apply → verify.
- **Out of scope (M3 / #51):** package deploy, runtime attachment, schedule
  activation, process execution, execution-log retrieval, failure-row runtime
  proof. Do these manually and record evidence separately only if required.

## Core rules

1. **Confirm reference components exist first** with `query_components`. Do not
   build against a component_id that no longer resolves.
2. **Reuse connections by default** so QA never handles plaintext secrets:
   - Database connection: `107aaef1-cb1e-4975-be44-69d120803864`
     (`MS SQL Server Orders DB`, folder `#Common`)
   - REST connection: `7f7e0730-1152-4467-b912-e3a8ed12782a`
     (`REST None`, folder `#Common`)
3. **Use a unique prefix** per QA run — `M2QA-<UTC timestamp>` (e.g.
   `M2QA-20260529T1430Z`) — and `conflict_policy='fail'` so a run can never
   silently reuse or overwrite a previous run's output.
4. **Dry-run before any mutation.** The default `build_integration apply` is a
   dry run; only set `dry_run=false` after explicit operator approval.
5. **Record blocks, don't paper over them.** If a reference component is missing,
   a prerequisite is unavailable, or mutation is not approved, record the block
   in the evidence template rather than marking live QA complete.

## Tooling note

All calls below use the `.fn()` pattern against the unified `server.py`
(see `docs/QA_REVIEW_MANUAL.md`). Example shape:

```python
import os
os.environ["BOOMI_LOCAL"] = "true"  # or your live MCP wiring
import server

PROFILE = "reneraai-5RO3DD"
```

## Step-by-step workflow

### 1. Confirm reference components

```python
server.query_components.fn(
    profile=PROFILE,
    config='{"type": "connector-settings", "name": "MS SQL Server Orders DB"}',
)
server.query_components.fn(
    profile=PROFILE,
    config='{"type": "connector-settings", "name": "REST None"}',
)
```

Confirm both component_ids above still resolve. If either is missing, **stop**
and record the block.

### 2. Build the spec from the archetype (no Boomi call)

Author a `database_to_api_sync` parameter payload with:
- `source.binding.mode = "reuse"`, `component_id = 107aaef1-...`
- `target.binding.mode = "reuse"`, `component_id = 7f7e0730-...`
- `naming.component_prefix = "M2QA-<UTC timestamp>"`
- caller-authored SQL, REST path, source/target schema, and field mappings.

```python
spec_result = server.build_from_archetype.fn("database_to_api_sync", PARAMS)
assert spec_result["_success"] is True
assert spec_result["boomi_mutation"] is False
assert spec_result["raw_xml_exposed"] is False
spec = spec_result["integration_spec"]
```

### 3. Review the transformation (read-only)

```python
import json
review = server.review_transformation.fn(
    action="validate_unmapped",
    config=json.dumps({"integration_spec": spec}),
)
assert review["_success"] is True and review["valid"] is True
```

Resolve any unmapped-required or invalid-mapping issues in PARAMS before
planning.

### 4. Plan (no mutation)

```python
plan = server.build_integration.fn(
    profile=PROFILE,
    action="plan",
    config=json.dumps({"integration_spec": spec, "conflict_policy": "fail"}),
)
assert plan["_success"] is True
# Record: execution_order (main_process must be last), per-step planned_action,
# and any validation_error.
```

### 5. Dry-run apply (no mutation)

```python
dry = server.build_integration.fn(
    profile=PROFILE,
    action="apply",
    config=json.dumps({"integration_spec": spec, "conflict_policy": "fail"}),
)
assert dry["dry_run"] is True
```

### 6. (Optional) Real apply — explicit approval required

Only after an operator approves mutation of `reneraai-5RO3DD`:

```python
applied = server.build_integration.fn(
    profile=PROFILE,
    action="apply",
    config=json.dumps({
        "integration_spec": spec,
        "conflict_policy": "fail",
        "dry_run": False,
    }),
)
assert applied["_success"] is True
build_id = applied["build_id"]
# Record created vs reused component IDs from applied["results"].
```

### 7. Verify

```python
verified = server.build_integration.fn(
    profile=PROFILE,
    action="verify",
    config=json.dumps({"build_id": build_id}),
)
assert verified["_success"] is True
assert verified["failed_components"] == 0
```

## Evidence template

Copy and fill per QA run (paste to Issue #30 or project notes):

```
M2 database_to_api_sync Live QA — Evidence
------------------------------------------
Profile:                 reneraai-5RO3DD
UTC timestamp / prefix:  M2QA-________________
Operator:                ________________
Mutation approved?       yes / no

Reference components confirmed:
  DB  107aaef1-cb1e-4975-be44-69d120803864  exists: yes / no
  REST 7f7e0730-1152-4467-b912-e3a8ed12782a  exists: yes / no

build_from_archetype:    _success = ____   component_count = ____
review_transformation:   valid = ____   issues = ____
plan:                    _success = ____
  execution_order:       ________________________________ (main_process last? ___)
  per-step planned_action / validation_error: ____________
dry_run apply:           dry_run = ____   _success = ____

Real apply (optional):   performed? yes / no   _success = ____
  build_id:              ________________________________
  created component IDs + names: ____________
  reused component IDs + names:  ____________

verify:                  _success = ____   failed_components = ____
  dependency_issues:     ____________

Warnings / validation errors: ____________
Blocked prerequisites (deploy/runtime/schedule/execution/logs/failure-row):
  ____________
```

## Safety reminders

- Never commit account-specific raw XML, secrets, payloads, SQL, mappings, or
  Boomi execution data to the repo.
- Connections are reused by reference; QA never authors plaintext credentials.
- The `work` profile is read-only reference and must not be mutated.
