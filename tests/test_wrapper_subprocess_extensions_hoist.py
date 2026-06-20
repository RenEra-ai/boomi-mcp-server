"""Tests for issue #99 G3 — hoisting a called child's connection env-extension
declarations onto the wrapper parent.

#92 writes <ConnectionOverride> override points into the DECLARING process's
processOverrides, but the #91 capstone proved they do NOT surface through a #90
wrapper Process Call deployment (only when the declaring process is deployed
directly). G3 copies the child's process_extensions.connections onto the wrapper
at plan time (_synthesize_wrapper_subprocess_extensions) so the wrapper-deployed
package surfaces them, and type-checks the wrapper's connection refs
(_check_wrapper_subprocess_ref_types).
"""

from __future__ import annotations

import sys
from pathlib import Path

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from src.boomi_mcp.models.integration_models import (
    IntegrationComponentSpec,
    IntegrationSpecV1,
)
from src.boomi_mcp.categories.integration_builder import (
    _synthesize_wrapper_subprocess_edges,
    _synthesize_wrapper_subprocess_extensions,
    _check_wrapper_subprocess_ref_types,
)


def _field(fid, label, xpath):
    return {"id": fid, "label": label, "xpath": xpath}


_HOST = _field("host", "Host", "DatabaseConnectionSettings/@host")
_PORT = _field("port", "Port", "DatabaseConnectionSettings/@port")
_PASSWORD = _field("password", "Password", "DatabaseConnectionSettings/@password")


def _child(pe_connections, key="main_logic", conn_key="db_conn"):
    return IntegrationComponentSpec(
        key=key, type="process", action="create", name="Main",
        depends_on=[conn_key],
        config={
            "process_kind": "database_to_api_sync",
            "source": {}, "target": {}, "transform": {"mode": "passthrough"},
            "process_extensions": {"connections": pe_connections},
        },
    )


def _db_conn(key="db_conn"):
    return IntegrationComponentSpec(
        key=key, type="connector-settings", action="create", name="DB",
        config={"connector_type": "database"},
    )


def _wrapper(child_key="main_logic", process_extensions=None, key="wrap"):
    cfg = {
        "process_kind": "wrapper_subprocess",
        "process_calls": [{"subprocess_ref": f"$ref:{child_key}", "wait": True, "abort_on_error": False}],
    }
    if process_extensions is not None:
        cfg["process_extensions"] = process_extensions
    return IntegrationComponentSpec(
        key=key, type="process", action="create", name="Wrapper", depends_on=[], config=cfg,
    )


def _synth(spec):
    _synthesize_wrapper_subprocess_edges(spec)
    _synthesize_wrapper_subprocess_extensions(spec)
    return next(c for c in spec.components if c.key == "wrap")


def _spec(*components):
    return IntegrationSpecV1(name="t", integration_name="t", components=list(components))


# ---------------------------------------------------------------------------
# Hoisting
# ---------------------------------------------------------------------------

def test_child_overrides_hoisted_onto_wrapper():
    child = _child([{"connection_id": "$ref:db_conn", "fields": [_HOST, _PASSWORD]}])
    spec = _spec(_wrapper(), child, _db_conn())
    w = _synth(spec)
    conns = w.config["process_extensions"]["connections"]
    assert len(conns) == 1
    assert conns[0]["connection_id"] == "$ref:db_conn"
    assert [f["id"] for f in conns[0]["fields"]] == ["host", "password"]


def test_hoisted_connection_ref_added_to_wrapper_depends_on():
    child = _child([{"connection_id": "$ref:db_conn", "fields": [_HOST]}])
    spec = _spec(_wrapper(), child, _db_conn())
    w = _synth(spec)
    # main_logic (the child edge) + db_conn (the hoisted connection edge).
    assert "main_logic" in w.depends_on
    assert "db_conn" in w.depends_on


def test_literal_connection_id_not_added_to_depends_on():
    # A literal id needs no $ref dependency edge.
    child = _child([{"connection_id": "literal-conn-id", "fields": [_HOST]}])
    spec = _spec(_wrapper(), child, _db_conn())
    w = _synth(spec)
    assert "literal-conn-id" not in w.depends_on


def test_wrapper_without_child_extensions_gets_no_process_extensions():
    child = IntegrationComponentSpec(
        key="main_logic", type="process", action="create", name="Main",
        config={"process_kind": "database_to_api_sync", "source": {}, "target": {}, "transform": {"mode": "passthrough"}},
    )
    spec = _spec(_wrapper(), child, _db_conn())
    w = _synth(spec)
    assert "process_extensions" not in w.config


def test_child_keeps_its_own_extensions_after_hoist():
    child = _child([{"connection_id": "$ref:db_conn", "fields": [_HOST]}])
    spec = _spec(_wrapper(), child, _db_conn())
    _synth(spec)
    c = next(x for x in spec.components if x.key == "main_logic")
    # The child stays independently deployable — its declaration is untouched.
    assert c.config["process_extensions"]["connections"][0]["fields"][0]["id"] == "host"


# ---------------------------------------------------------------------------
# Dedup / precedence (wrapper-declared wins)
# ---------------------------------------------------------------------------

def test_wrapper_declared_override_takes_precedence_same_field():
    # Wrapper already declares host for db_conn with a DISTINCT xpath; the child
    # also declares host — the wrapper's entry is kept, not duplicated.
    wrapper_pe = {"connections": [{"connection_id": "$ref:db_conn",
                                   "fields": [_field("host", "WrapperHost", "X/@host")]}]}
    child = _child([{"connection_id": "$ref:db_conn", "fields": [_HOST, _PASSWORD]}])
    spec = _spec(_wrapper(process_extensions=wrapper_pe), child, _db_conn())
    w = _synth(spec)
    conns = w.config["process_extensions"]["connections"]
    assert len(conns) == 1
    fields = conns[0]["fields"]
    # host kept from the wrapper (its xpath), password merged in from the child.
    assert [f["id"] for f in fields] == ["host", "password"]
    host = next(f for f in fields if f["id"] == "host")
    assert host["xpath"] == "X/@host"  # wrapper's, not the child's


def test_missing_child_fields_merged_into_existing_connection():
    wrapper_pe = {"connections": [{"connection_id": "$ref:db_conn", "fields": [_HOST]}]}
    child = _child([{"connection_id": "$ref:db_conn", "fields": [_HOST, _PORT, _PASSWORD]}])
    spec = _spec(_wrapper(process_extensions=wrapper_pe), child, _db_conn())
    w = _synth(spec)
    fields = w.config["process_extensions"]["connections"][0]["fields"]
    assert [f["id"] for f in fields] == ["host", "port", "password"]


def test_wrapper_internal_duplicate_connection_deduped():
    # Two wrapper-declared entries for the SAME connection must collapse to one
    # ConnectionOverride (deduped by connection_id+field.id), not emit twice.
    wrapper_pe = {"connections": [
        {"connection_id": "$ref:db_conn", "fields": [_HOST]},
        {"connection_id": "$ref:db_conn", "fields": [_HOST, _PASSWORD]},  # dup conn, host repeats
    ]}
    child = IntegrationComponentSpec(
        key="main_logic", type="process", action="create", name="Main",
        config={"process_kind": "database_to_api_sync", "source": {}, "target": {}, "transform": {"mode": "passthrough"}},
    )
    spec = _spec(_wrapper(process_extensions=wrapper_pe), child, _db_conn())
    w = _synth(spec)
    conns = w.config["process_extensions"]["connections"]
    assert len(conns) == 1  # one connection, not two
    assert [f["id"] for f in conns[0]["fields"]] == ["host", "password"]  # host not duplicated


def test_whitespace_padded_duplicate_ids_deduped():
    # A wrapper declaring the same connection with inconsistent whitespace (a
    # padded literal connection_id, and a padded field id) must collapse to ONE
    # override — _extract/build strip these to the same canonical id, so dedup
    # must key on the stripped values too (else duplicate ConnectionOverride/field
    # XML is emitted).
    wrapper_pe = {"connections": [
        {"connection_id": "literal-conn", "fields": [_HOST]},
        {"connection_id": " literal-conn ", "fields": [
            {"id": " host ", "label": "H", "xpath": "X/@h"},  # padded dup of host
            _PASSWORD,
        ]},
    ]}
    child = IntegrationComponentSpec(
        key="main_logic", type="process", action="create", name="Main",
        config={"process_kind": "database_to_api_sync", "source": {}, "target": {}, "transform": {"mode": "passthrough"}},
    )
    spec = _spec(_wrapper(process_extensions=wrapper_pe), child, _db_conn())
    w = _synth(spec)
    conns = w.config["process_extensions"]["connections"]
    assert len(conns) == 1  # collapsed despite the connection_id whitespace
    field_ids = [f["id"].strip() for f in conns[0]["fields"]]
    assert field_ids == ["host", "password"]  # padded " host " deduped against "host"


def test_padded_ref_duplicate_not_masked_by_dedup():
    # A wrapper declaring the SAME connection as an exact "$ref:db_conn" AND a
    # whitespace-padded " $ref:db_conn " must NOT have the padded (malformed) ref
    # folded into the canonical twin by dedup — validate_config must still reject
    # the padded ref (MISSING_PROCESS_DEPENDENCY), not plan it.
    from src.boomi_mcp.categories.components.builders import WrapperSubprocessBuilder
    wrapper_pe = {"connections": [
        {"connection_id": "$ref:db_conn", "fields": [_HOST]},
        {"connection_id": " $ref:db_conn ", "fields": [_PASSWORD]},  # padded, malformed
    ]}
    child = IntegrationComponentSpec(
        key="main_logic", type="process", action="create", name="Main",
        config={"process_kind": "database_to_api_sync", "source": {}, "target": {}, "transform": {"mode": "passthrough"}},
    )
    spec = _spec(_wrapper(process_extensions=wrapper_pe), child, _db_conn())
    w = _synth(spec)  # synthesis must NOT dedup the padded ref away
    err = WrapperSubprocessBuilder.validate_config(w.config, depends_on=w.depends_on)
    assert err is not None
    assert err.error_code == "MISSING_PROCESS_DEPENDENCY"


def test_padded_ref_from_child_not_masked_by_dedup():
    # A CHILD declaring the same connection as exact "$ref:db_conn" AND padded
    # " $ref:db_conn " is hoisted onto the wrapper; the padded (malformed) ref
    # must NOT be deduped away — it must reach validate_config (which rejects it).
    # Covers the reuse-child path whose own validate_config is skipped.
    from src.boomi_mcp.categories.components.builders import WrapperSubprocessBuilder
    child = _child([
        {"connection_id": "$ref:db_conn", "fields": [_HOST]},
        {"connection_id": " $ref:db_conn ", "fields": [_PASSWORD]},  # padded, malformed
    ])
    spec = _spec(_wrapper(), child, _db_conn())
    w = _synth(spec)  # padded child ref must be preserved, not folded away
    err = WrapperSubprocessBuilder.validate_config(w.config, depends_on=w.depends_on)
    assert err is not None
    assert err.error_code == "MISSING_PROCESS_DEPENDENCY"


def test_intra_entry_duplicate_field_deduped():
    # A single connection entry declaring the same field id twice (fields=[host,
    # host]) must collapse to ONE field — no duplicate <field> in the override.
    wrapper_pe = {"connections": [{"connection_id": "$ref:db_conn", "fields": [_HOST, dict(_HOST)]}]}
    child = IntegrationComponentSpec(
        key="main_logic", type="process", action="create", name="Main",
        config={"process_kind": "database_to_api_sync", "source": {}, "target": {}, "transform": {"mode": "passthrough"}},
    )
    spec = _spec(_wrapper(process_extensions=wrapper_pe), child, _db_conn())
    w = _synth(spec)
    conns = w.config["process_extensions"]["connections"]
    assert len(conns) == 1
    assert [f["id"] for f in conns[0]["fields"]] == ["host"]  # not [host, host]


def test_malformed_reuse_child_block_not_masked():
    # A reuse/reference child (its own validate_config is skipped) with a malformed
    # TOP-LEVEL process_extensions (connections not a list) must be surfaced on the
    # wrapper, not silently dropped — validate_config must reject it.
    from src.boomi_mcp.categories.components.builders import WrapperSubprocessBuilder
    child = IntegrationComponentSpec(
        key="main_logic", type="process", action="create", name="Main",
        config={"process_kind": "database_to_api_sync", "source": {}, "target": {}, "transform": {"mode": "passthrough"},
                "process_extensions": {"connections": "not-a-list"}},
    )
    spec = _spec(_wrapper(), child, _db_conn())
    w = _synth(spec)
    err = WrapperSubprocessBuilder.validate_config(w.config, depends_on=w.depends_on)
    assert err is not None
    assert err.error_code == "PROCESS_EXTENSIONS_INVALID"


def test_malformed_reuse_child_nondict_entry_not_masked():
    # A reuse child whose connections list contains a non-dict entry must be
    # surfaced (not silently filtered) — validate_config rejects it.
    from src.boomi_mcp.categories.components.builders import WrapperSubprocessBuilder
    child = IntegrationComponentSpec(
        key="main_logic", type="process", action="create", name="Main",
        config={"process_kind": "database_to_api_sync", "source": {}, "target": {}, "transform": {"mode": "passthrough"},
                "process_extensions": {"connections": ["not-an-object"]}},
    )
    spec = _spec(_wrapper(), child, _db_conn())
    w = _synth(spec)  # must not crash (tail guards non-dict entries)
    err = WrapperSubprocessBuilder.validate_config(w.config, depends_on=w.depends_on)
    assert err is not None
    assert err.error_code == "PROCESS_EXTENSIONS_INVALID"


def test_malformed_duplicate_child_entry_not_masked():
    # A child declaring the same connection twice where the SECOND is malformed
    # (empty fields) must NOT have that malformed duplicate dropped by dedup —
    # validate_config must reject it. Covers the reuse-child path (no own validation).
    from src.boomi_mcp.categories.components.builders import WrapperSubprocessBuilder
    child = _child([
        {"connection_id": "$ref:db_conn", "fields": [_HOST]},
        {"connection_id": "$ref:db_conn", "fields": []},  # malformed duplicate
    ])
    spec = _spec(_wrapper(), child, _db_conn())
    w = _synth(spec)
    err = WrapperSubprocessBuilder.validate_config(w.config, depends_on=w.depends_on)
    assert err is not None
    assert err.error_code == "PROCESS_EXTENSIONS_INVALID"


def test_malformed_field_in_child_entry_not_masked():
    # A child entry whose duplicate field id is malformed (missing label) must not
    # be silently dropped by field dedup — the malformed entry is preserved and
    # validate_config rejects it.
    from src.boomi_mcp.categories.components.builders import WrapperSubprocessBuilder
    child = _child([{"connection_id": "$ref:db_conn", "fields": [
        _HOST,
        {"id": "host", "xpath": "DatabaseConnectionSettings/@host"},  # missing label, dup id
    ]}])
    spec = _spec(_wrapper(), child, _db_conn())
    w = _synth(spec)
    err = WrapperSubprocessBuilder.validate_config(w.config, depends_on=w.depends_on)
    assert err is not None
    assert err.error_code == "PROCESS_EXTENSIONS_INVALID"


def test_malformed_duplicate_wrapper_entry_not_masked():
    # A wrapper declaring the same connection twice where the SECOND (duplicate)
    # entry is malformed (empty fields) must NOT be silently collapsed by dedup —
    # validate_config must still reject it (PROCESS_EXTENSIONS_INVALID), not pass
    # on the valid first entry.
    from src.boomi_mcp.categories.components.builders import WrapperSubprocessBuilder
    wrapper_pe = {"connections": [
        {"connection_id": "$ref:db_conn", "fields": [_HOST]},
        {"connection_id": "$ref:db_conn", "fields": []},  # malformed duplicate (empty fields)
    ]}
    child = _child([{"connection_id": "$ref:db_conn", "fields": [_PASSWORD]}])
    spec = _spec(_wrapper(process_extensions=wrapper_pe), child, _db_conn())
    w = _synth(spec)  # must not crash, must not silently drop the malformed dup
    err = WrapperSubprocessBuilder.validate_config(w.config, depends_on=w.depends_on)
    assert err is not None
    assert err.error_code == "PROCESS_EXTENSIONS_INVALID"


def test_malformed_wrapper_connections_not_masked_by_hoist():
    # A wrapper whose process_extensions.connections is NOT a list must NOT be
    # silently overwritten by valid hoisted child connections — validate_config
    # must still reject it (PROCESS_EXTENSIONS_INVALID), not pass clean.
    from src.boomi_mcp.categories.components.builders import WrapperSubprocessBuilder
    wrapper_pe = {"connections": "not-a-list"}
    child = _child([{"connection_id": "$ref:db_conn", "fields": [_HOST, _PASSWORD]}])
    spec = _spec(_wrapper(process_extensions=wrapper_pe), child, _db_conn())
    w = _synth(spec)  # must not crash, must not overwrite the malformed block
    assert w.config["process_extensions"]["connections"] == "not-a-list"  # left untouched
    err = WrapperSubprocessBuilder.validate_config(w.config, depends_on=w.depends_on)
    assert err is not None
    assert err.error_code == "PROCESS_EXTENSIONS_INVALID"


def test_idempotent_second_synthesis_is_noop():
    child = _child([{"connection_id": "$ref:db_conn", "fields": [_HOST, _PASSWORD]}])
    spec = _spec(_wrapper(), child, _db_conn())
    _synth(spec)
    w = next(c for c in spec.components if c.key == "wrap")
    first = [f["id"] for f in w.config["process_extensions"]["connections"][0]["fields"]]
    _synthesize_wrapper_subprocess_extensions(spec)  # run again
    second = [f["id"] for f in w.config["process_extensions"]["connections"][0]["fields"]]
    assert first == second == ["host", "password"]


# ---------------------------------------------------------------------------
# Ref-type check
# ---------------------------------------------------------------------------

def _by_key(spec):
    return {c.key: c for c in spec.components}


def test_ref_type_check_accepts_connection_override():
    child = _child([{"connection_id": "$ref:db_conn", "fields": [_HOST]}])
    spec = _spec(_wrapper(), child, _db_conn())
    w = _synth(spec)
    err = _check_wrapper_subprocess_ref_types(w, w.config, _by_key(spec))
    assert err is None


def test_ref_type_check_rejects_non_connection_override():
    # A wrapper process_extensions ref pointing at the child PROCESS (not a
    # connection) is a type mismatch.
    wrapper_pe = {"connections": [{"connection_id": "$ref:main_logic", "fields": [_HOST]}]}
    child = _child([])
    spec = _spec(_wrapper(process_extensions=wrapper_pe), child, _db_conn())
    w = _synth(spec)
    err = _check_wrapper_subprocess_ref_types(w, w.config, _by_key(spec))
    assert err is not None
    assert err.error_code == "PROCESS_REF_TYPE_MISMATCH"
    assert err.field == "process_extensions.connections[0].connection_id"


# ---------------------------------------------------------------------------
# Codex review fixes — hand-authored wrapper process_extensions robustness
# ---------------------------------------------------------------------------

def test_wrapper_declared_ref_added_to_depends_on_even_without_child_match():
    # A wrapper that declares its OWN process_extensions $ref for an in-spec
    # connection that NO child references must still get the dependency edge, so
    # the connection is applied before the wrapper (apply-time $ref resolution).
    wrapper_pe = {"connections": [{"connection_id": "$ref:db_conn", "fields": [_HOST]}]}
    # child has NO process_extensions, so the connection is only wrapper-declared.
    child = IntegrationComponentSpec(
        key="main_logic", type="process", action="create", name="Main",
        config={"process_kind": "database_to_api_sync", "source": {}, "target": {}, "transform": {"mode": "passthrough"}},
    )
    spec = _spec(_wrapper(process_extensions=wrapper_pe), child, _db_conn())
    w = _synth(spec)
    assert "db_conn" in w.depends_on


def test_malformed_wrapper_seed_fields_does_not_crash_synthesis():
    # A hand-authored wrapper seed whose 'fields' is not a list must NOT crash
    # synthesis when a child declares the same connection (the merge previously
    # hit AttributeError via setdefault on a non-list). Synthesis must complete;
    # validate_config then surfaces the structured PROCESS_EXTENSIONS_INVALID.
    from src.boomi_mcp.categories.components.builders import WrapperSubprocessBuilder
    wrapper_pe = {"connections": [{"connection_id": "$ref:db_conn", "fields": "NOT-A-LIST"}]}
    child = _child([{"connection_id": "$ref:db_conn", "fields": [_HOST, _PASSWORD]}])
    spec = _spec(_wrapper(process_extensions=wrapper_pe), child, _db_conn())
    w = _synth(spec)  # must not raise
    err = WrapperSubprocessBuilder.validate_config(w.config, depends_on=w.depends_on)
    assert err is not None
    assert err.error_code == "PROCESS_EXTENSIONS_INVALID"


def _wrapper_pe_cfg(connection_id):
    return {
        "process_kind": "wrapper_subprocess",
        "process_calls": [{"process_id": "EXISTING-CHILD-ID"}],
        "process_extensions": {"connections": [{"connection_id": connection_id,
                                                "fields": [_HOST]}]},
    }


def test_unreachable_process_extensions_ref_fails_cleanly():
    # A wrapper process_extensions $ref to a connection NOT in depends_on must
    # fail with a clean reachability error, not leak an unresolved $ref at apply.
    from src.boomi_mcp.categories.components.builders import WrapperSubprocessBuilder
    err = WrapperSubprocessBuilder.validate_config(_wrapper_pe_cfg("$ref:nonexistent_conn"), depends_on=[])
    assert err is not None
    assert err.error_code == "MISSING_PROCESS_DEPENDENCY"


def test_padded_process_extensions_ref_rejected():
    # A whitespace-padded ref is NOT substituted by _resolve_dependency_tokens
    # (which requires '$ref:' at byte 0) — it would leak unresolved, so reject it.
    from src.boomi_mcp.categories.components.builders import WrapperSubprocessBuilder
    err = WrapperSubprocessBuilder.validate_config(_wrapper_pe_cfg(" $ref:db_conn "), depends_on=["db_conn"])
    assert err is not None
    assert err.error_code == "MISSING_PROCESS_DEPENDENCY"


def test_empty_key_process_extensions_ref_rejected():
    # '$ref:' with an empty key resolves to nothing — reject rather than leak.
    from src.boomi_mcp.categories.components.builders import WrapperSubprocessBuilder
    err = WrapperSubprocessBuilder.validate_config(_wrapper_pe_cfg("$ref:"), depends_on=[])
    assert err is not None
    assert err.error_code == "MISSING_PROCESS_DEPENDENCY"


def test_reachable_process_extensions_ref_passes_validate_config():
    # When the connection $ref IS in depends_on (the synthesis-added edge), the
    # reachability check passes.
    from src.boomi_mcp.categories.components.builders import WrapperSubprocessBuilder
    assert WrapperSubprocessBuilder.validate_config(_wrapper_pe_cfg("$ref:db_conn"), depends_on=["db_conn"]) is None


def test_literal_process_extensions_ref_passes_validate_config():
    # A literal (non-$ref) connection id needs no depends_on edge.
    from src.boomi_mcp.categories.components.builders import WrapperSubprocessBuilder
    assert WrapperSubprocessBuilder.validate_config(_wrapper_pe_cfg("literal-conn-id-1234"), depends_on=[]) is None
