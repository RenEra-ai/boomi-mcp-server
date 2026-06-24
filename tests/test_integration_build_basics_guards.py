"""Issue #102 (M9.8) — mechanical integration-build basics guards.

Plan-time guards over ``_build_plan`` / ``build_integration_action``:
  * A1 in-spec duplicate-connection guard (hard conflict / aliased warning)
  * A2 FQDN-not-IP lint (warning)
  * A3 minimal-connection lint (warning)
  * B3 reject literal ${ENV_VAR} tokens in connection fields (hard)
  * D1 bracketed naming convention lint (warning, opt-in)
  * D2 property-naming lint (warning)
  * E1 folder-on-create lint (warning — prevents silent root placement)

Run with PYTHONPATH=src (the editable install .pth is stale):
    PYTHONPATH=src .venv/bin/python -m pytest tests/test_integration_build_basics_guards.py
"""

from unittest.mock import MagicMock, patch

import pytest

from boomi_mcp.categories.integration_builder import _build_plan


@pytest.fixture(autouse=True)
def _no_existing_components():
    """All these guards are plan-time over CREATE components — there are no
    existing components to resolve. Patch paginate_metadata to [] so reuse-policy
    resolution does not paginate against the MagicMock client (which never ends).
    """
    with patch(
        "boomi_mcp.categories.integration_builder.paginate_metadata",
        return_value=[],
    ):
        yield


# ---------------------------------------------------------------------------
# Spec helpers (self-contained component dicts)
# ---------------------------------------------------------------------------

def _db_conn(key, name, **overrides):
    cfg = {
        "connector_type": "database",
        "driver_id": "sqlserver",
        "auth_mode": "username_password",
        "component_name": name,
        "folder_name": "Process Library",
        "host": "db.internal.example.com",
        "port": 1433,
        "dbname": "ExampleDB",
        "username": "svc_user",
        "credential_ref": "credential://example/sqlserver/password",
    }
    cfg.update(overrides)
    return {"key": key, "type": "connector-settings", "action": "create",
            "name": name, "config": cfg}


def _rest_conn(key, name, **overrides):
    cfg = {
        "connector_type": "rest",
        "component_name": name,
        "folder_name": "Process Library",
        "base_url": "https://api.example.com",
        "auth": "OAUTH2",
        "oauth2": {
            "grant_type": "client_credentials",
            "client_id": "client-id",
            "client_secret_ref": "credential://target-api/oauth-client-secret",
            "access_token_url": "https://api.example.com/oauth/token",
            "scope": "",
            "credentials_assertion_type": "client_secret",
        },
    }
    cfg.update(overrides)
    return {"key": key, "type": "connector-settings", "action": "create",
            "name": name, "config": cfg}


def _process(key, name, **cfg_overrides):
    cfg = {
        "name": name,
        "process_kind": "wrapper_subprocess",
        "process_calls": [{"process_id": "99999999-9999-9999-9999-999999999999"}],
        "folder_name": "Process Library",
    }
    cfg.update(cfg_overrides)
    return {"key": key, "type": "process", "action": "create", "name": name, "config": cfg}


def _plan(components, naming=None):
    spec = {
        "version": "1.0",
        "name": "test-integration",
        "components": components,
    }
    if naming is not None:
        spec["naming"] = naming
    return _build_plan(MagicMock(), {"conflict_policy": "reuse", "integration_spec": spec})


def _warnings(plan):
    return plan.get("warnings") or []


def _step(plan, key):
    return next(s for s in plan["steps"] if s["key"] == key)


def _has_warn(plan, token):
    return any(token in w for w in _warnings(plan))


# ---------------------------------------------------------------------------
# A1 — in-spec duplicate-connection guard
# ---------------------------------------------------------------------------

def test_duplicate_db_endpoint_compatible_auth_aliases_and_warns():
    plan = _plan([
        _db_conn("conn_a", "Orders DB A"),
        _db_conn("conn_b", "Orders DB B"),  # same host/port/driver + same auth
    ])
    assert plan["_success"] is True
    assert plan.get("connection_aliases") == {"conn_b": "conn_a"}
    assert _has_warn(plan, "same endpoint")
    # No step is converted to an error_* action on the compatible-alias path.
    assert _step(plan, "conn_b")["planned_action"] in ("create", "create_clone")


def test_duplicate_db_endpoint_differing_auth_warns_never_blocks():
    # A1 is advisory: a same-endpoint duplicate with DIFFERENT auth is flagged
    # as distinct connections (warning), but NEVER hard-blocks the build and is
    # NOT aliased (referencing the canonical would route to different auth).
    plan = _plan([
        _db_conn("conn_a", "Orders DB A", credential_ref="credential://a/pw"),
        _db_conn("conn_b", "Orders DB B", credential_ref="credential://b/pw"),
    ])
    assert plan["_success"] is True
    assert _has_warn(plan, "DUPLICATE_CONNECTION")
    assert _has_warn(plan, "different auth")
    assert "connection_aliases" not in plan
    assert _step(plan, "conn_b")["planned_action"] in ("create", "create_clone")


def test_multiple_auth_groups_aliases_within_each_group():
    # One endpoint, two auth groups: A (cred a); B and C (cred b). C must alias
    # to B (its own auth group's canonical), not be reported distinct from A
    # (Codex review — track a canonical per (endpoint, auth identity)).
    plan = _plan([
        _db_conn("conn_a", "DB A", credential_ref="credential://a"),
        _db_conn("conn_b", "DB B", credential_ref="credential://b"),
        _db_conn("conn_c", "DB C", credential_ref="credential://b"),
    ])
    assert plan["_success"] is True
    # C aliases to B (same auth group); A is a distinct group (no alias for A).
    assert plan.get("connection_aliases") == {"conn_c": "conn_b"}
    # B is flagged distinct from A (different auth group).
    assert _has_warn(plan, "different auth")


def test_distinct_db_endpoints_not_deduped():
    plan = _plan([
        _db_conn("conn_a", "Orders DB A"),
        _db_conn("conn_b", "Orders DB B", host="other.internal.example.com"),
    ])
    assert "connection_aliases" not in plan
    assert _step(plan, "conn_b")["planned_action"] in ("create", "create_clone")


def test_distinct_dbname_same_server_not_deduped():
    # Same host/port/driver but DIFFERENT database = distinct endpoints, not a
    # duplicate (Codex review — dbname is part of the DB endpoint identity).
    plan = _plan([
        _db_conn("conn_a", "Orders DB", dbname="OrdersDB"),
        _db_conn("conn_b", "Inventory DB", dbname="InventoryDB"),
    ])
    assert "connection_aliases" not in plan
    assert _step(plan, "conn_b")["planned_action"] in ("create", "create_clone")


def test_case_distinct_dbname_not_deduped():
    # dbname case is preserved in the endpoint key — `Sales` and `sales` are
    # different databases on case-sensitive engines (Codex review); they must NOT
    # collapse into a false duplicate / unsafe alias.
    plan = _plan([
        _db_conn("conn_a", "DB A", dbname="Sales"),
        _db_conn("conn_b", "DB B", dbname="sales"),
    ])
    assert "connection_aliases" not in plan
    assert not _has_warn(plan, "DUPLICATE_CONNECTION")


def test_duplicate_rest_endpoint_differing_oauth_scope_aliases_not_conflict():
    # Same auth method + same client_secret_ref, differing ONLY in oauth2.scope
    # is a LOW-confidence difference — it must NOT hard-block (a false conflict on
    # a build-equivalent-ish spec is worse than an advisory alias). Codex review:
    # the conflict decision uses the high-confidence auth identity, not scope.
    base_oauth = {
        "grant_type": "client_credentials", "client_id": "cid",
        "client_secret_ref": "credential://x/secret",
        "access_token_url": "https://api.example.com/oauth/token",
        "credentials_assertion_type": "client_secret",
    }
    a = _rest_conn("conn_a", "API A", oauth2={**base_oauth, "scope": "read"})
    b = _rest_conn("conn_b", "API B", oauth2={**base_oauth, "scope": "read write"})
    plan = _plan([a, b])
    assert plan["_success"] is True
    assert plan.get("connection_aliases") == {"conn_b": "conn_a"}
    assert _step(plan, "conn_b")["planned_action"] in ("create", "create_clone")


def test_build_equivalent_rest_connector_type_alias_does_not_conflict():
    # `rest` vs `rest_client` (same endpoint, same auth/secret) emit the SAME
    # Boomi connection — they must alias, never hard-conflict (Codex review).
    a = _rest_conn("conn_a", "API A")  # connector_type='rest'
    b = _rest_conn("conn_b", "API B", connector_type="rest_client")
    plan = _plan([a, b])
    assert plan["_success"] is True
    assert plan.get("connection_aliases") == {"conn_b": "conn_a"}
    assert _step(plan, "conn_b")["planned_action"] in ("create", "create_clone")


def test_build_equivalent_db_port_scalar_format_does_not_conflict():
    # port 1433 (int) vs '1433' (str) is the same endpoint/connection — alias,
    # never conflict (Codex review — scalar formatting is build-equivalent).
    a = _db_conn("conn_a", "DB A", port=1433)
    b = _db_conn("conn_b", "DB B", port="1433")
    plan = _plan([a, b])
    assert plan["_success"] is True
    assert plan.get("connection_aliases") == {"conn_b": "conn_a"}
    assert _step(plan, "conn_b")["planned_action"] in ("create", "create_clone")


def test_duplicate_db_endpoint_differing_username_warns_not_aliased():
    # Same endpoint + same auth_mode/credential_ref but DIFFERENT username =
    # distinct credentials → flagged as distinct (warning), NOT aliased, never
    # blocks (username is in the family-scoped auth identity).
    plan = _plan([
        _db_conn("conn_a", "DB A", username="svc_a"),
        _db_conn("conn_b", "DB B", username="svc_b"),
    ])
    assert plan["_success"] is True
    assert _has_warn(plan, "different auth")
    assert "connection_aliases" not in plan


def test_duplicate_db_endpoint_credential_ref_case_sensitive_not_aliased():
    # credential_ref is an opaque, possibly case-sensitive ref — case-distinct
    # refs are DIFFERENT, so the pair is flagged distinct (not aliased) but still
    # never blocks (Codex review: do not lowercase secret refs).
    plan = _plan([
        _db_conn("conn_a", "DB A", credential_ref="credential://vault/APISecret"),
        _db_conn("conn_b", "DB B", credential_ref="credential://vault/apisecret"),
    ])
    assert plan["_success"] is True
    assert _has_warn(plan, "different auth")
    assert "connection_aliases" not in plan


def test_auth_identity_is_family_scoped_and_case_sensitive():
    # The auth identity is scoped to the connector family's fields and compares
    # case-SENSITIVELY (aligned with the builders, which validate auth modes
    # case-sensitively) — Codex review.
    from boomi_mcp.categories.integration_builder import _connection_auth_identity

    db = {"auth_mode": "username_password", "username": "u", "credential_ref": "c"}
    # A stray REST-only `auth` key on a DB config is IGNORED for the DB identity,
    # so it cannot cause a false conflict against an otherwise-identical DB conn.
    db_with_stray_rest = {**db, "auth": "OAUTH2"}
    role = "database connector-settings"
    assert _connection_auth_identity(db, role) == _connection_auth_identity(db_with_stray_rest, role)
    # Case is significant (a case-variant auth_mode the builder would reject is a
    # different identity, not a false 'compatible' alias).
    db_upper = {**db, "auth_mode": "USERNAME_PASSWORD"}
    assert _connection_auth_identity(db, role) != _connection_auth_identity(db_upper, role)


def test_duplicate_rest_endpoint_differing_auth_warns_not_aliased():
    # Two REST connections to the same base_url but different auth are flagged as
    # distinct (warning), NOT aliased and NEVER blocked (A1 advisory).
    none_auth = {"connector_type": "rest", "component_name": "API None",
                 "folder_name": "F", "base_url": "https://api.example.com", "auth": "NONE"}
    plan = _plan([
        {"key": "conn_a", "type": "connector-settings", "action": "create",
         "name": "API None", "config": none_auth},
        _rest_conn("conn_b", "API OAuth"),  # same base_url, auth=OAUTH2
    ])
    assert plan["_success"] is True
    assert _has_warn(plan, "different auth")
    assert "connection_aliases" not in plan
    assert _step(plan, "conn_b")["planned_action"] in ("create", "create_clone")


def test_apply_does_not_block_on_duplicate_connection():
    # A1 is advisory — apply proceeds (no fail-fast) even for a same-endpoint
    # duplicate with differing auth; the warning is surfaced in the plan.
    from boomi_mcp.categories.integration_builder import _build_plan

    config = {
        "conflict_policy": "reuse",
        "integration_spec": {
            "version": "1.0",
            "name": "dup",
            "components": [
                _db_conn("conn_a", "Orders DB A", credential_ref="credential://a/pw"),
                _db_conn("conn_b", "Orders DB B", credential_ref="credential://b/pw"),
            ],
        },
    }
    plan = _build_plan(MagicMock(), config)
    assert plan["_success"] is True
    # No duplicate-connection error action is ever produced.
    assert all(s["planned_action"] != "error_duplicate_connection" for s in plan["steps"])
    assert _has_warn(plan, "DUPLICATE_CONNECTION")


# ---------------------------------------------------------------------------
# A2 — FQDN-not-IP lint (warning)
# ---------------------------------------------------------------------------

def test_db_ip_literal_host_warns():
    plan = _plan([_db_conn("conn_a", "Orders DB", host="10.0.0.5")])
    assert plan["_success"] is True
    assert _has_warn(plan, "CONNECTION_ENDPOINT_IP_LITERAL")


def test_rest_ip_literal_base_url_warns():
    plan = _plan([_rest_conn("conn_a", "API", base_url="https://192.168.1.10/v1")])
    assert _has_warn(plan, "CONNECTION_ENDPOINT_IP_LITERAL")


def test_fqdn_host_no_ip_warning():
    plan = _plan([_db_conn("conn_a", "Orders DB", host="db.example.com")])
    assert not _has_warn(plan, "CONNECTION_ENDPOINT_IP_LITERAL")


# ---------------------------------------------------------------------------
# A3 — minimal-connection lint (warning)
# ---------------------------------------------------------------------------

def test_rest_base_url_with_path_warns():
    plan = _plan([_rest_conn("conn_a", "API", base_url="https://api.example.com/v1/items?x=1")])
    assert _has_warn(plan, "CONNECTION_BASE_URL_HAS_PATH")


def test_rest_root_base_url_no_path_warning():
    plan = _plan([_rest_conn("conn_a", "API", base_url="https://api.example.com/")])
    assert not _has_warn(plan, "CONNECTION_BASE_URL_HAS_PATH")


# ---------------------------------------------------------------------------
# B3 — reject literal ${ENV_VAR} tokens in connection fields (hard)
# ---------------------------------------------------------------------------

def test_db_host_env_token_hard_fails():
    plan = _plan([_db_conn("conn_a", "Orders DB", host="${DB_HOST}")])
    step = _step(plan, "conn_a")
    assert step["planned_action"] == "error_database_validation"
    assert step["validation_error"]["error_code"] == "ENV_VAR_LITERAL_REJECTED"


def test_rest_base_url_env_token_hard_fails():
    plan = _plan([_rest_conn("conn_a", "API", base_url="https://${HOST}/v1")])
    step = _step(plan, "conn_a")
    assert step["planned_action"] == "error_rest_validation"
    assert step["validation_error"]["error_code"] == "ENV_VAR_LITERAL_REJECTED"


def test_credential_ref_uri_not_flagged_as_env_token():
    # A real credential:// reference must NOT be a false positive.
    plan = _plan([_db_conn("conn_a", "Orders DB")])
    step = _step(plan, "conn_a")
    assert step["planned_action"] in ("create", "create_clone")


# ---------------------------------------------------------------------------
# D1 — bracketed naming convention (opt-in, warning)
# ---------------------------------------------------------------------------

def test_bracketed_convention_inactive_no_naming_flags():
    plan = _plan([_process("p1", "Sync Customers")])  # no naming.convention
    assert not _has_warn(plan, "NAMING_CONVENTION_BRACKETED")


def test_bracketed_process_name_flagged_when_active():
    plan = _plan([_process("p1", "Sync Customers")], naming={"convention": "bracketed"})
    assert plan["_success"] is True
    assert _has_warn(plan, "NAMING_CONVENTION_BRACKETED")
    # Warning only — never an error action.
    assert _step(plan, "p1")["planned_action"] in ("create", "create_clone")


def test_bracketed_conformant_subprocess_clean():
    plan = _plan([_process("p1", "SUB De-dupe Customers")], naming={"convention": "bracketed"})
    assert not _has_warn(plan, "NAMING_CONVENTION_BRACKETED")


def test_bracketed_connection_env_in_name_flagged():
    plan = _plan([_db_conn("conn_a", "Orders DB PROD")], naming={"convention": "bracketed"})
    assert _has_warn(plan, "NAMING_CONVENTION_BRACKETED")


# ---------------------------------------------------------------------------
# D2 — property-naming lint (warning)
# ---------------------------------------------------------------------------

def test_ddp_name_non_conformant_flagged_under_target():
    # The builder consumes config.target.dynamic_path.ddp_name (Codex review).
    plan = _plan([_process("p1", "Proc", target={"dynamic_path": {"ddp_name": "rest.path"}})])
    assert _has_warn(plan, "PROPERTY_NAMING")


def test_ddp_name_non_conformant_flagged_top_level_fallback():
    # A hand-authored top-level config.dynamic_path is still linted (fallback).
    plan = _plan([_process("p1", "Proc", dynamic_path={"ddp_name": "rest.path"})])
    assert _has_warn(plan, "PROPERTY_NAMING")


def test_ddp_name_conformant_clean():
    plan = _plan([_process("p1", "Proc", target={"dynamic_path": {"ddp_name": "DDP_REST_PATH"}})])
    assert not _has_warn(plan, "PROPERTY_NAMING")


# ---------------------------------------------------------------------------
# E1 — folder-on-create lint (warning, never blocks)
# ---------------------------------------------------------------------------

def test_create_without_folder_warns_but_does_not_block():
    plan = _plan([_db_conn("conn_a", "Orders DB", folder_name="")])
    assert plan["_success"] is True
    assert _has_warn(plan, "FOLDER_REQUIRED_ON_CREATE")
    assert _step(plan, "conn_a")["planned_action"] in ("create", "create_clone")


def test_create_with_folder_no_warning():
    plan = _plan([_db_conn("conn_a", "Orders DB")])  # _db_conn sets folder_name
    assert not _has_warn(plan, "FOLDER_REQUIRED_ON_CREATE")
