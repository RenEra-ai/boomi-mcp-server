"""Issue #13 (M7): handler tests for ``discover_db_schema_action``.

Artifact-only — the DB tool must NEVER open a JDBC/network connection or touch
Boomi. Covers dict + JSON-string success, table derivation, ordering, PK/FK
constraints, indexes, nullable normalization, default-value suppression, error
branches, truncation, and a read-only proof that httpx / socket / credentials are
never touched.
"""

import json
import sys
from pathlib import Path
from unittest.mock import patch

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

import boomi_mcp.categories.schema_discovery as sd
from boomi_mcp.categories.schema_discovery import discover_db_schema_action


def _artifact():
    return {
        "database_product": "postgres",
        "catalog": "app",
        "columns": [
            {"table_schema": "public", "table_name": "orders", "column_name": "id",
             "ordinal_position": 1, "data_type": "integer", "is_nullable": "NO"},
            {"table_schema": "public", "table_name": "orders", "column_name": "note",
             "ordinal_position": 2, "data_type": "text", "is_nullable": "YES",
             "column_default": "s3cr3t-default-value"},
            {"table_schema": "public", "table_name": "customers", "column_name": "id",
             "ordinal_position": 1, "data_type": "integer", "is_nullable": "NO"},
        ],
        "constraints": [
            {"constraint_name": "pk_orders", "constraint_type": "PRIMARY KEY",
             "table_schema": "public", "table_name": "orders", "columns": ["id"]},
            {"constraint_name": "fk_orders_cust", "constraint_type": "FOREIGN KEY",
             "table_schema": "public", "table_name": "orders", "columns": ["cust_id"],
             "referenced_table_name": "customers", "referenced_columns": ["id"]},
        ],
        "indexes": [
            {"index_name": "ix_orders_id", "table_schema": "public", "table_name": "orders",
             "unique": True, "columns": ["id"]},
        ],
    }


# ---------------------------------------------------------------------------
# Success
# ---------------------------------------------------------------------------

def test_db_dict_success():
    r = discover_db_schema_action(_artifact())
    assert r["_success"] is True
    assert r["read_only"] is True and r["boomi_mutation"] is False and r["raw_xml_exposed"] is False
    assert r["format"] == "information_schema_json" and r["version"] is None
    assert r["source_mode"] == "artifact"
    assert r["database_product"] == "postgres" and r["catalog"] == "app"
    names = [t["name"] for t in r["tables"]]
    assert names == ["customers", "orders"]  # deterministic schema/name ordering
    assert r["counts"] == {"tables": 2, "columns": 3}


def test_db_json_string_success():
    r = discover_db_schema_action(json.dumps(_artifact()))
    assert r["_success"] is True and r["counts"]["tables"] == 2


def test_db_tables_derived_from_columns_when_absent():
    art = {"columns": [{"table_name": "solo", "column_name": "a", "data_type": "int"}]}
    r = discover_db_schema_action(art)
    assert [t["name"] for t in r["tables"]] == ["solo"]


def test_db_columns_ordered_and_nullable_normalized():
    r = discover_db_schema_action(_artifact())
    orders = next(t for t in r["tables"] if t["name"] == "orders")
    assert [c["name"] for c in orders["columns"]] == ["id", "note"]  # by ordinal_position
    assert orders["columns"][0]["nullable"] is False
    assert orders["columns"][1]["nullable"] is True


def test_db_constraints_and_indexes_attached():
    r = discover_db_schema_action(_artifact())
    orders = next(t for t in r["tables"] if t["name"] == "orders")
    types = {c["type"] for c in orders["constraints"]}
    assert "PRIMARY KEY" in types and "FOREIGN KEY" in types
    fk = next(c for c in orders["constraints"] if c["type"] == "FOREIGN KEY")
    assert fk["referenced_table"] == "customers" and fk["referenced_columns"] == ["id"]
    assert orders["indexes"][0] == {"name": "ix_orders_id", "unique": True, "columns": ["id"]}


def test_db_default_value_suppressed():
    r = discover_db_schema_action(_artifact())
    orders = next(t for t in r["tables"] if t["name"] == "orders")
    note = next(c for c in orders["columns"] if c["name"] == "note")
    assert note["default_present"] is True
    # the actual default VALUE must never appear anywhere in the response
    assert "s3cr3t-default-value" not in json.dumps(r)


def test_db_nullable_bool_and_none_passthrough():
    art = {"columns": [
        {"table_name": "t", "column_name": "a", "data_type": "int", "is_nullable": True},
        {"table_name": "t", "column_name": "b", "data_type": "int", "is_nullable": False},
        {"table_name": "t", "column_name": "c", "data_type": "int"},
    ]}
    r = discover_db_schema_action(art)
    cols = {c["name"]: c["nullable"] for c in r["tables"][0]["columns"]}
    assert cols == {"a": True, "b": False, "c": None}


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

def test_db_malformed_json_string_parse_error():
    assert discover_db_schema_action("{not json")["error_code"] == "DB_SCHEMA_PARSE_ERROR"


def test_db_missing_columns_invalid_spec():
    assert discover_db_schema_action({"tables": []})["error_code"] == "DB_SCHEMA_INVALID_SPEC"


def test_db_empty_columns_invalid_spec():
    assert discover_db_schema_action({"columns": []})["error_code"] == "DB_SCHEMA_INVALID_SPEC"


def test_db_non_dict_non_str_invalid_input():
    assert discover_db_schema_action(123)["error_code"] == "DB_SCHEMA_INVALID_INPUT"


def test_db_json_string_non_object_invalid_input():
    # valid JSON but not an object -> parses then fails structural check
    assert discover_db_schema_action("[1, 2, 3]")["error_code"] == "DB_SCHEMA_INVALID_INPUT"


def test_db_size_limit():
    big = json.dumps(_artifact())
    r = discover_db_schema_action(big, options={"max_input_chars": 10})
    assert r["error_code"] == "DB_SCHEMA_SIZE_LIMIT_EXCEEDED"


# ---------------------------------------------------------------------------
# Truncation
# ---------------------------------------------------------------------------

def test_db_node_truncation():
    r = discover_db_schema_action(_artifact(), options={"max_nodes": 1})
    assert r["_success"] is True and r["truncated"] is True
    assert len(r["tables"]) == 1
    assert r["warnings"] and r["warnings"][0]["code"] == "TRUNCATED"


def test_db_field_truncation():
    r = discover_db_schema_action(_artifact(), options={"max_fields": 1})
    assert r["_success"] is True and r["truncated"] is True


# ---------------------------------------------------------------------------
# Read-only proof — DB tool never opens JDBC/network/credentials
# ---------------------------------------------------------------------------

def test_db_never_touches_network_or_credentials():
    with (
        patch.object(sd.httpx, "Client") as m_client,
        patch.object(sd.socket, "getaddrinfo") as m_gai,
    ):
        r = discover_db_schema_action(_artifact())
    assert r["_success"] is True
    m_client.assert_not_called()
    m_gai.assert_not_called()
