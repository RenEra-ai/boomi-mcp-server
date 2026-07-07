"""Issue #83 (M7.3): handler tests for ``suggest_connection_reuse_action``.

Mocks ``paginate_metadata`` and ``component_get_xml`` at the point of use to
exercise subtype/alias resolution, folder-locality + endpoint-hint + name-similarity
ranking, empty results, the no-credential-material guarantee, per-candidate
component-read failures, the IntegrationSpecV1 reuse bindings, and optional
paired connector-action metadata — without any live Boomi access.
"""

import json
import sys
from pathlib import Path
from unittest.mock import patch

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

from boomi_mcp.categories.components import connection_reuse
from boomi_mcp.categories.components.connection_reuse import (
    suggest_connection_reuse_action,
)
from boomi_mcp.categories.components._shared import ComponentGetDeadlineExceeded
from boomi_mcp.patterns.primitives._helpers import value_looks_secret, _key_looks_secret

_MODULE = "boomi_mcp.categories.components.connection_reuse"
_CLIENT = object()  # unused — paginate/get_xml are mocked


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _meta(cid, name, folder):
    return {
        "component_id": cid,
        "name": name,
        "folder_name": folder,
        "type": "connector-settings",
    }


def _db_xml(host, dbname="ORDERS", driver="sqlserver", port="1433"):
    """Live-style database connector-settings XML with an ENCRYPTED password and
    a username — neither of which must ever surface in the tool output."""
    return (
        '<bns:Component xmlns:bns="http://api.platform.boomi.com/" '
        'type="connector-settings" subType="database" name="db" folderName="f">'
        "<bns:encryptedValues>"
        '<bns:encryptedValue path="//DatabaseConnectionSettings/@password" isSet="true"/>'
        "</bns:encryptedValues>"
        "<bns:object>"
        f'<DatabaseConnectionSettings xmlns="" host="{host}" port="{port}" '
        f'dbname="{dbname}" driverId="{driver}" username="svc_secret_user" '
        f'password="[encrypted]" urlFormat="jdbc:sqlserver://{host}:{port}"/>'
        "</bns:object></bns:Component>"
    )


def _rest_xml(url):
    """REST connector-settings XML with a base URL plus an OAuth2 client secret
    (long-base64 ciphertext) that must never surface."""
    return (
        '<bns:Component xmlns:bns="http://api.platform.boomi.com/" '
        'type="connector-settings" subType="officialboomi-X3979C-rest-prod" '
        'name="rest" folderName="f">'
        "<bns:object>"
        '<GenericConnectionConfig xmlns="">'
        f'<field id="url" type="string" value="{url}"/>'
        '<field id="user" type="string" value="svc_secret_user"/>'
        '<field id="oauthContext" type="oauth">'
        "<OAuth2Config><credentials "
        'clientSecret="ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefXYZ="/>'
        "</OAuth2Config></field>"
        "</GenericConnectionConfig>"
        "</bns:object></bns:Component>"
    )


def _paginate(settings, actions=None):
    """side_effect: 1st call → connector-settings, 2nd (if any) → actions."""
    state = {"n": 0}

    def _fn(client, query_config, **kw):
        state["n"] += 1
        if state["n"] == 1:
            return settings
        return actions or []

    return _fn


def _get_xml(xml_by_id):
    def _fn(client, component_id, **kw):
        return {"xml": xml_by_id.get(component_id, ""), "component_id": component_id}

    return _fn


def _walk(node):
    if isinstance(node, dict):
        for key, value in node.items():
            yield "key", key
            yield from _walk(value)
    elif isinstance(node, list):
        for item in node:
            yield from _walk(item)
    else:
        yield "value", node


# ---------------------------------------------------------------------------
# Subtype / alias resolution
# ---------------------------------------------------------------------------


def test_database_subtype_passthrough():
    with patch(f"{_MODULE}.paginate_metadata", side_effect=_paginate([])):
        out = suggest_connection_reuse_action(_CLIENT, "prod", "database")
    assert out["_success"] is True
    assert out["resolved_subtype"] == "database"
    assert out["connector_family"] == "database"


def test_rest_alias_resolves_to_canonical_subtype():
    with patch(f"{_MODULE}.paginate_metadata", side_effect=_paginate([])):
        out = suggest_connection_reuse_action(_CLIENT, "prod", "rest")
    assert out["resolved_subtype"] == "officialboomi-X3979C-rest-prod"
    assert out["connector_family"] == "rest"


def test_soap_alias_resolves_to_canonical_subtype():
    with patch(f"{_MODULE}.paginate_metadata", side_effect=_paginate([])):
        out = suggest_connection_reuse_action(_CLIENT, "prod", "soap_client")
    assert out["resolved_subtype"] == "wssoapclientsdk"
    assert out["connector_family"] == "soap_client"


# ---------------------------------------------------------------------------
# Ranking
# ---------------------------------------------------------------------------


def test_common_folder_and_endpoint_hint_rank_first():
    settings = [
        _meta("db-legacy", "Misc DB", "Projects/Legacy"),
        _meta("db-common", "Orders Warehouse DB", "#Common"),
    ]
    xml_by_id = {
        "db-common": _db_xml("db.prod.acme.com"),
        "db-legacy": _db_xml("other.example.com"),
    }
    with (
        patch(f"{_MODULE}.paginate_metadata", side_effect=_paginate(settings)),
        patch(f"{_MODULE}.component_get_xml", side_effect=_get_xml(xml_by_id)),
    ):
        out = suggest_connection_reuse_action(
            _CLIENT, "prod", "database",
            purpose="orders", endpoint_hint="db.prod.acme.com",
        )
    cands = out["candidates"]
    assert cands[0]["component_id"] == "db-common"
    # #Common folder (15) + exact host (30) push it clearly ahead of the legacy one.
    assert cands[0]["score"] > cands[1]["score"]
    assert any("exact host match" in r for r in cands[0]["why_matched"])
    assert any("#Common" in r for r in cands[0]["why_matched"])
    # Safe context carries the endpoint, never the credentials.
    ctx = cands[0]["safe_context"]
    assert ctx.get("host") == "db.prod.acme.com"
    assert "username" not in ctx and "password" not in ctx


def test_name_similarity_ranks_when_no_endpoint_hint():
    settings = [
        _meta("c1", "Random Connection", "Projects"),
        _meta("c2", "Customer Orders API", "Projects"),
    ]
    xml_by_id = {"c1": _rest_xml("https://a.example.com"), "c2": _rest_xml("https://b.example.com")}
    with (
        patch(f"{_MODULE}.paginate_metadata", side_effect=_paginate(settings)),
        patch(f"{_MODULE}.component_get_xml", side_effect=_get_xml(xml_by_id)),
    ):
        out = suggest_connection_reuse_action(
            _CLIENT, "prod", "rest", purpose="customer orders",
        )
    assert out["candidates"][0]["component_id"] == "c2"


def test_empty_metadata_returns_success_no_candidates():
    with patch(f"{_MODULE}.paginate_metadata", side_effect=_paginate([])):
        out = suggest_connection_reuse_action(_CLIENT, "prod", "database")
    assert out["_success"] is True
    assert out["candidates"] == []
    assert out["total_matched"] == 0
    assert out["read_only"] is True
    assert out["boomi_mutation"] is False
    assert out["raw_xml_exposed"] is False


# ---------------------------------------------------------------------------
# No credential material ever surfaces
# ---------------------------------------------------------------------------


def test_no_secret_material_in_response():
    settings = [
        _meta("db1", "DB One", "#Common"),
        _meta("rest1", "REST One", "#Common"),
    ]
    xml_by_id = {
        "db1": _db_xml("db.acme.com"),
        "rest1": _rest_xml("https://api.acme.com"),
    }
    with (
        patch(f"{_MODULE}.paginate_metadata", side_effect=_paginate(settings)),
        patch(f"{_MODULE}.component_get_xml", side_effect=_get_xml(xml_by_id)),
    ):
        out = suggest_connection_reuse_action(
            _CLIENT, "prod", "database", endpoint_hint="db.acme.com",
        )

    for kind, item in _walk(out):
        if kind == "key":
            assert not _key_looks_secret(item), f"secret-shaped key leaked: {item!r}"
        else:
            assert not value_looks_secret(item), f"secret-shaped value leaked: {item!r}"

    blob = json.dumps(out)
    for banned in ("svc_secret_user", "[encrypted]", "clientSecret", "ABCDEFGHIJKLMNOP"):
        assert banned not in blob, f"credential material {banned!r} leaked into output"


# ---------------------------------------------------------------------------
# Robustness: a per-candidate component read failure doesn't abort the call
# ---------------------------------------------------------------------------


def test_component_get_failure_keeps_candidate():
    # Non-shared folder + no purpose/hint isolates the score to the subtype match,
    # so the read failure is the only variable under test.
    settings = [_meta("db1", "DB One", "Projects")]

    def _boom(client, component_id, **kw):
        raise ComponentGetDeadlineExceeded(component_id, 90, 91.0)

    with (
        patch(f"{_MODULE}.paginate_metadata", side_effect=_paginate(settings)),
        patch(f"{_MODULE}.component_get_xml", side_effect=_boom),
    ):
        out = suggest_connection_reuse_action(_CLIENT, "prod", "database")
    assert out["_success"] is True
    assert len(out["candidates"]) == 1
    cand = out["candidates"][0]
    # No endpoint context → endpoint score 0; base subtype(40) still stands.
    assert cand["score"] == 40
    assert cand["safe_context"] == {}
    assert any("endpoint context unavailable" in r for r in cand["why_matched"])


# ---------------------------------------------------------------------------
# Reuse bindings (IntegrationSpecV1 reference_only / conflict_policy='reuse')
# ---------------------------------------------------------------------------


def test_reference_bindings_shape():
    settings = [_meta("db1", "Orders DB", "#Common")]
    with (
        patch(f"{_MODULE}.paginate_metadata", side_effect=_paginate(settings)),
        patch(f"{_MODULE}.component_get_xml", side_effect=_get_xml({"db1": _db_xml("h")})),
    ):
        out = suggest_connection_reuse_action(_CLIENT, "prod", "database")
    ref = out["candidates"][0]["reference"]

    assert ref["archetype_binding"] == {"mode": "reuse", "component_id": "db1"}
    assert ref["reference_only_config"] == {
        "reference_only": True,
        "connector_type": "database",
        "component_id": "db1",
    }
    example = ref["integration_spec_component_example"]
    assert example["type"] == "connector-settings"
    assert example["config"]["reference_only"] is True
    assert example["config"]["component_id"] == "db1"

    fallback = ref["exact_name_fallback"]
    assert fallback["conflict_policy"] == "reuse"
    assert fallback["component"]["name"] == "Orders DB"
    assert fallback["component"]["config"]["component_name"] == "Orders DB"
    assert "component_id" not in fallback["component"]["config"]


# ---------------------------------------------------------------------------
# Optional paired connector-action metadata
# ---------------------------------------------------------------------------


def test_paired_actions_same_subtype_and_folder_non_authoritative():
    settings = [_meta("db1", "Orders DB", "#Common")]
    actions = [
        _meta("act-same", "Orders DB GET", "#Common"),
        _meta("act-elsewhere", "Unrelated Op", "Projects/Other"),
    ]
    with (
        patch(f"{_MODULE}.paginate_metadata", side_effect=_paginate(settings, actions)),
        patch(f"{_MODULE}.component_get_xml", side_effect=_get_xml({"db1": _db_xml("h")})),
    ):
        out = suggest_connection_reuse_action(_CLIENT, "prod", "database")
    paired = out["candidates"][0]["paired_actions"]
    assert paired, "expected a same-folder paired action"
    ids = {p["component_id"] for p in paired}
    assert "act-same" in ids
    for p in paired:
        assert p["component_type"] == "connector-action"
        assert p["authoritative"] is False


def test_paired_actions_query_failure_is_swallowed():
    settings = [_meta("db1", "Orders DB", "#Common")]

    def _paginate_fail(client, query_config, **kw):
        if _paginate_fail.calls == 0:
            _paginate_fail.calls += 1
            return settings
        raise RuntimeError("actions query failed")

    _paginate_fail.calls = 0
    with (
        patch(f"{_MODULE}.paginate_metadata", side_effect=_paginate_fail),
        patch(f"{_MODULE}.component_get_xml", side_effect=_get_xml({"db1": _db_xml("h")})),
    ):
        out = suggest_connection_reuse_action(_CLIENT, "prod", "database")
    assert out["_success"] is True
    assert out["candidates"][0]["paired_actions"] == []


# ---------------------------------------------------------------------------
# top_k clamping
# ---------------------------------------------------------------------------


def test_top_k_clamped_high_and_low():
    settings = [_meta(f"c{i}", f"Conn {i}", "Projects") for i in range(4)]
    xml_by_id = {f"c{i}": _db_xml(f"h{i}.example.com") for i in range(4)}
    # Fresh _paginate() per call — its call-order state must not leak between the
    # two independent handler invocations.
    with (
        patch(f"{_MODULE}.paginate_metadata", side_effect=_paginate(settings)),
        patch(f"{_MODULE}.component_get_xml", side_effect=_get_xml(xml_by_id)),
    ):
        high = suggest_connection_reuse_action(_CLIENT, "prod", "database", top_k=100)
    assert high["top_k"] == 25
    assert len(high["candidates"]) == 4  # only 4 exist

    with (
        patch(f"{_MODULE}.paginate_metadata", side_effect=_paginate(settings)),
        patch(f"{_MODULE}.component_get_xml", side_effect=_get_xml(xml_by_id)),
    ):
        low = suggest_connection_reuse_action(_CLIENT, "prod", "database", top_k=0)
    assert low["top_k"] == 1
    assert len(low["candidates"]) == 1


def test_missing_connector_type_errors_with_flags():
    out = suggest_connection_reuse_action(_CLIENT, "prod", "")
    assert out["_success"] is False
    assert out["error_code"] == "CONNECTION_REUSE_QUERY_FAILED"
    assert out["read_only"] is True
    assert out["boomi_mutation"] is False
    assert out["raw_xml_exposed"] is False
