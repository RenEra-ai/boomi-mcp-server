"""Issue #92 M4.5.7 — database_to_api_sync environment-extension emission.

Drives the archetype directly (DatabaseToApiSyncArchetype.emit_spec) so no
fastmcp/server import is needed. Asserts the default + opt-in policy for the
emitted process_extensions block and that no credential value or secret leaks
into the process config.
"""

from __future__ import annotations

import copy
import sys
from pathlib import Path

import pytest

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from src.boomi_mcp.categories.components.builders.process_flow_builder import (
    ProcessFlowBuilder,
)
from src.boomi_mcp.patterns.archetypes.database_to_api_sync import (
    DatabaseToApiSyncArchetype,
    DatabaseToApiSyncParameters,
)


_PAYLOAD = {
    "naming": {"integration_name": "demo-sync", "component_prefix": "DEMO"},
    "source": {
        "binding": {
            "mode": "create",
            "settings": {
                "driver": "microsoft_jdbc",
                "auth_mode": "username_password",
                "host": "db.internal",
                "database": "AppDB",
                "username": "svc_sync",
                "credential_ref": "secrets/db/svc_sync",
            },
        },
        "read_operation": {
            "sql": "<<user-authored DB read statement>>",
            "result_schema": {"fields": [{"name": "source_a", "data_type": "character"}]},
        },
    },
    "target": {
        "binding": {"mode": "create", "settings": {"base_url": "https://api.example.com", "auth_mode": "none"}},
        "send_request": {"method": "POST", "path": "/v1/items"},
        "payload_profile": {
            "format": "json",
            "root": {
                "name": "Root",
                "kind": "object",
                "children": [{"name": "target_a", "kind": "simple", "data_type": "character"}],
            },
        },
    },
    "transform": {
        "operations": [
            {"operation_type": "direct", "source_field": "source_a", "target_path": "Root/target_a"}
        ]
    },
    "execution": {"trigger": {"mode": "manual"}},
    "reliability": {"retry": {"max_attempts": 1}, "dlq": {"enabled": False}, "error_classifier": {}},
}

_DB_CONN_KEY = "source_db_connection"


def _emit(payload):
    spec = DatabaseToApiSyncArchetype.emit_spec(DatabaseToApiSyncParameters(**payload))
    return spec


def _main_process(spec):
    procs = [c for c in spec.components if getattr(c, "type", None) == "process"]
    assert len(procs) == 1
    return procs[0]


def _payload(**deep_overrides):
    p = copy.deepcopy(_PAYLOAD)
    p.update(deep_overrides)
    return p


def test_default_declares_credential_fields_only():
    mp = _main_process(_emit(_payload()))
    pe = mp.config.get("process_extensions")
    assert pe is not None
    conns = pe["connections"]
    assert len(conns) == 1
    # The override targets the same DB connection (by $ref) the source binds to.
    assert conns[0]["connection_id"] == f"$ref:{_DB_CONN_KEY}"
    assert conns[0]["connector_type"] == "database"
    assert [f["id"] for f in conns[0]["fields"]] == ["username", "password"]
    # The DB connection key is already declared in depends_on (ref-reachable).
    assert _DB_CONN_KEY in mp.depends_on


def test_endpoint_opt_in_adds_host_and_port():
    mp = _main_process(_emit(_payload(environment_extensions={"endpoint_connection_fields": True})))
    fields = mp.config["process_extensions"]["connections"][0]["fields"]
    assert [f["id"] for f in fields] == ["host", "port", "username", "password"]


def test_credentials_off_emits_no_declaration():
    mp = _main_process(_emit(_payload(environment_extensions={"credential_connection_fields": False})))
    assert "process_extensions" not in mp.config


def test_reuse_mode_emits_no_declaration():
    mp = _main_process(
        _emit(_payload(source={
            "binding": {"mode": "reuse", "component_id": "EXISTING-DB-CONN"},
            "read_operation": _PAYLOAD["source"]["read_operation"],
        }))
    )
    assert "process_extensions" not in mp.config


def test_emitted_config_carries_no_secret_value():
    mp = _main_process(_emit(_payload()))
    pe = mp.config["process_extensions"]
    # The forbidden-secret scanner (keys-only) must not trip on the declaration.
    assert ProcessFlowBuilder.scan_forbidden_secret_fields(pe) is None
    # And no credential VALUE / credential_ref is present anywhere in the block.
    blob = repr(pe)
    assert "svc_sync" not in blob
    assert "secrets/db" not in blob
    assert "credential_ref" not in blob


def test_emitted_declaration_builds_to_valid_process_xml():
    mp = _main_process(_emit(_payload(environment_extensions={"endpoint_connection_fields": True})))
    # The builder validates the emitted process_extensions shape (and the $ref
    # connection_id reachability against depends_on) without error.
    err = ProcessFlowBuilder.validate_config(mp.config, depends_on=mp.depends_on)
    assert err is None


def test_archetype_documents_environment_extensions_policy():
    desc = DatabaseToApiSyncArchetype.describe()
    blob = repr(desc).lower()
    assert "environment_extensions" in blob or "environment-extension" in blob
    # The parameter schema exposes the new typed field.
    schema_blob = repr(desc.get("parameter_schema", {}))
    assert "environment_extensions" in schema_blob
