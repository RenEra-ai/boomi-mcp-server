# Legacy process XML oracles for the migrated shipped examples (#159 L7)

Each `*.process.legacy.xml` is the process component file the LEGACY
`ProcessFlowBuilder.build(config, name=...)` produced for the example's process
configuration as shipped at ed17dde (the S1 tip), BEFORE the example was rewritten as a
direct canonical process unit. They were captured once and are never regenerated from
the canonical compiler.

Component ids: the legacy configs name components by `$ref:KEY`; every `$ref:KEY` was
resolved to `uuid5(NAMESPACE_URL, "l7/" + KEY)` before the builder ran. The M11 join
example named its connection and operation by literal ids (`1111...`/`2222...`/`3333...`/
`4444...`); in the captured text those four literals are replaced by the same uuid5 of the
supporting component key the migrated example now declares (`join_db_connection`,
`join_db_get_operation`, `join_rest_connection`, `join_rest_operation`).

`tests/_example_unit_support.py` renders the migrated example through
`plan_authoring_request_v1` -> `build_materialization_plan` -> late binding ->
`materialize_canonical_process_xml` with that registry; the tests assert byte equality.
The M11 basic example's oracle is the pre-existing golden
`tests/fixtures/golden_xml/m11_cache_property_basic.xml`.
