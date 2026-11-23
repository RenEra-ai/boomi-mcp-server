# issue_157_direct fixtures

`database_to_api_sync.wm_off_dlq_off_create.direct.json` is the S1 direct-unit request (explicit
`components` + `units`, no recipe invocation) for the database_to_api_sync wm_off_dlq_off_create
case, byte-identical to `tests/fixtures/governance/issue_157/cases/database_to_api_sync/
wm_off_dlq_off_create/input.canonical.json` as committed at `abf5e80^` (git 9ae1222), i.e. before
#159 S2 rewrote that file as a recipe invocation.

Source: its components are the baseline archetype's emitted component list (baseline_sha
ba1be9f34317d720d3b012484622ab7e2742143b), process entry dropped, and the process unit is the
hand-authored ProcessIR; recorded in that case's `provenance.json` `superseded` block. It is
causally independent of the recipe the case now invokes. The recipe replay input stays the
authority for the replay case itself; this file serves only the plan/coverage tests that need
direct `intent.components`.
