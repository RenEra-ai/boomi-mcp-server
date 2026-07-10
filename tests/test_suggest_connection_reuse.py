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

import pytest

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


def _db_xml(host, dbname="ORDERS", driver="sqlserver", port="1433", url_format=None):
    """Live-style database connector-settings XML with an ENCRYPTED password and
    a username — neither of which must ever surface in the tool output."""
    if url_format is None:
        url_format = f"jdbc:sqlserver://{host}:{port}"
    url_format = url_format.replace("&", "&amp;")  # real Boomi XML escapes '&'
    return (
        '<bns:Component xmlns:bns="http://api.platform.boomi.com/" '
        'type="connector-settings" subType="database" name="db" folderName="f">'
        "<bns:encryptedValues>"
        '<bns:encryptedValue path="//DatabaseConnectionSettings/@password" isSet="true"/>'
        "</bns:encryptedValues>"
        "<bns:object>"
        f'<DatabaseConnectionSettings xmlns="" host="{host}" port="{port}" '
        f'dbname="{dbname}" driverId="{driver}" username="svc_secret_user" '
        f'password="[encrypted]" urlFormat="{url_format}"/>'
        "</bns:object></bns:Component>"
    )


def _rest_xml_url(url):
    """Minimal REST connector-settings XML whose base URL is exactly ``url``."""
    return (
        '<bns:Component xmlns:bns="http://api.platform.boomi.com/" '
        'type="connector-settings" subType="officialboomi-X3979C-rest-prod" '
        'name="rest" folderName="f">'
        "<bns:object>"
        '<GenericConnectionConfig xmlns="">'
        f'<field id="url" type="string" value="{url}"/>'
        "</GenericConnectionConfig>"
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


# ---------------------------------------------------------------------------
# Endpoint-value sanitization — credential-free skeleton echo.
#
# safe_context echoes only scheme://host[:port][/path] (standard URL) or
# jdbc:sub://host:port[/db] (JDBC). Userinfo, query, fragment, and the JDBC
# ;property block are DROPPED — the only components that can carry credentials —
# so any embedded credential is removed by construction, for any URL shape.
# ---------------------------------------------------------------------------


def test_any_at_in_url_omitted():
    # Definitive rule (through round-27): ANY '@' in a url value → OMIT. Userinfo
    # can contain ':' '/' '?' '#' '%' ';' that make host extraction ambiguous, and
    # a real base URL never carries an '@'. Covers userinfo, query-'@', userinfo
    # spanning delimiters, and nested-credential-url-in-query.
    settings = [_meta("rest1", "Acme REST", "#Common")]
    leaky_urls = [
        "https://svcuser:hunter2@api.acme.com/v1?api_key=SECRETKEY123&plain=ok",
        "https://api.acme.com?email=ops@example.com",
        "https://user:p@ss@api.example.com/v1",
        "https://svc_secret_user:p?ss@api.example.com",   # '?' in password
        "https://api.example.com/cb?redirect=https://user:SUPERSECRET@evil/x",
    ]
    for leaky in leaky_urls:
        xml_url = leaky.replace("&", "&amp;")
        with (
            patch(f"{_MODULE}.paginate_metadata", side_effect=_paginate(settings)),
            patch(f"{_MODULE}.component_get_xml", side_effect=_get_xml({"rest1": _rest_xml_url(xml_url)})),
        ):
            out = suggest_connection_reuse_action(_CLIENT, "prod", "rest")
        ctx = out["candidates"][0]["safe_context"]
        assert "url" not in ctx, leaky  # omitted, not echoed
        blob = json.dumps(out)
        for banned in ("svcuser", "hunter2", "SECRETKEY123", "svc_secret_user", "SUPERSECRET", "p@ss"):
            assert banned not in blob


def test_webhook_secret_in_path_dropped():
    # Codex round-10: a credential embedded in the PATH (webhook token) is dropped.
    settings = [_meta("rest1", "Acme REST", "#Common")]
    url = "https://hooks.example.com/services/T00000/B00000/XXXXWEBHOOKSECRET"
    with (
        patch(f"{_MODULE}.paginate_metadata", side_effect=_paginate(settings)),
        patch(f"{_MODULE}.component_get_xml", side_effect=_get_xml({"rest1": _rest_xml_url(url)})),
    ):
        out = suggest_connection_reuse_action(_CLIENT, "prod", "rest")
    ctx_url = out["candidates"][0]["safe_context"]["url"]
    assert ctx_url == "https://hooks.example.com"
    assert "WEBHOOKSECRET" not in json.dumps(out) and "services" not in json.dumps(out)


def test_ambiguous_matrix_at_authority_omitted():
    # Codex round-23 P2a + Bug #153: an authority combining a ';'/'\' matrix with
    # an '@' is ambiguous (userinfo vs matrix are grammatically identical) — omit
    # (both the standard-URL AND schemeless branches, consistently).
    from boomi_mcp.categories.components.connection_reuse import _safe_url_skeleton
    for leaky in (
        "https://api.example.com;token=abc@secret.internal/v1",
        "https://api.example.com\\x=y@evil.internal",
        "https://user:p;ass@api.example.com/v1",
        "user:p;ass@host",              # schemeless
        "host;token=x@secret.internal",  # schemeless
    ):
        assert _safe_url_skeleton(leaky) is None, leaky


def test_any_literal_at_omitted_skeleton():
    # Definitive rule (round-27): ANY literal '@' → omit, in either branch and
    # regardless of where the '@' sits (userinfo, path, or query — a '?' or '/' in
    # the password can move the '@' anywhere). Real base URLs never carry '@'.
    from boomi_mcp.categories.components.connection_reuse import _safe_url_skeleton
    for leaky in (
        "https://svc:pa/ss@api.example.com",
        "svc:pa/ss@api.example.com",
        "https://u:p\\q@api.example.com",
        "https://api.example.com/v1?x=a@b",       # '@' in query
        "api.example.com?x=a@b",
        "api.example.com/p@x",
        "https://svc_secret_user:p?ss@api.example.com",  # '?' in password
        "https://svc%2Fpass@api.example.com",     # literal '@', inner encoding
    ):
        assert _safe_url_skeleton(leaky) is None, leaky


def test_nonnumeric_port_and_arbitrary_phrase_omitted():
    # Codex round-30: a malformed `user:password` (no scheme/'@') makes urlsplit
    # read the password as a non-numeric port and echo the username; and an
    # arbitrary phrase must not be echoed as a placeholder.
    from boomi_mcp.categories.components.connection_reuse import _safe_url_skeleton
    # P1a: non-numeric port that is NOT a ${…} placeholder → omit (username leak).
    assert _safe_url_skeleton("https://svc_secret_user:hunter2") is None
    assert _safe_url_skeleton("svc_secret_user:hunter2") is None
    # A ${PORT}-style externalized port keeps the host (real, not a leak).
    assert _safe_url_skeleton("https://api.acme.com:${PORT}/v1") == "https://api.acme.com"
    assert _safe_url_skeleton("api.acme.com:${PORT}") == "api.acme.com"
    # P1b: only a KNOWN Boomi sentinel echoes verbatim; arbitrary text is omitted.
    assert _safe_url_skeleton("MY PASSWORD IS hunter2") is None
    assert _safe_url_skeleton("SET IN EXTENSION") == "SET IN EXTENSION"


def test_percent_encoded_userinfo_host_omitted():
    # Codex round-26: percent-encoded userinfo delimiters (%3A=':', %40='@') hide
    # the '@' from urlsplit so the host includes the username; a '%' in the host
    # means encoded delimiters → omit (never truncate to the username).
    from boomi_mcp.categories.components.connection_reuse import _safe_url_skeleton
    assert _safe_url_skeleton("https://svc_secret_user%3Ahunter2%40api.example.com/v1") is None


def test_matrix_without_at_reduces_to_host():
    # A ';' matrix WITHOUT an '@' is unambiguous — reduce to the host (both
    # branches), dropping the matrix. Verified secret-safe through the handler.
    from boomi_mcp.categories.components.connection_reuse import _safe_url_skeleton
    assert _safe_url_skeleton("https://api.example.com;jsessionid=SECRET/v1") == "https://api.example.com"
    assert _safe_url_skeleton("api.example.com;jsessionid=SECRET") == "api.example.com"
    settings = [_meta("rest1", "Acme REST", "#Common")]
    with (
        patch(f"{_MODULE}.paginate_metadata", side_effect=_paginate(settings)),
        patch(f"{_MODULE}.component_get_xml", side_effect=_get_xml({"rest1": _rest_xml_url("https://api.example.com;jsessionid=SECRET/v1")})),
    ):
        out = suggest_connection_reuse_action(_CLIENT, "prod", "rest")
    assert out["candidates"][0]["safe_context"]["url"] == "https://api.example.com"
    assert "jsessionid" not in json.dumps(out) and "SECRET" not in json.dumps(out)


def test_schemeless_ipv6_preserved():
    # Codex round-23 P2b: a schemeless IPv6 literal must not be split at an
    # internal ':' (bare) or dropped (bracketed).
    settings = [_meta("rest1", "Acme REST", "#Common")]
    cases = {
        "2001:db8::1/path": "2001:db8::1",
        "[2001:db8::1]:8443/v1": "[2001:db8::1]:8443",
    }
    for value, expected in cases.items():
        with (
            patch(f"{_MODULE}.paginate_metadata", side_effect=_paginate(settings)),
            patch(f"{_MODULE}.component_get_xml", side_effect=_get_xml({"rest1": _rest_xml_url(value)})),
        ):
            out = suggest_connection_reuse_action(_CLIENT, "prod", "rest")
        assert out["candidates"][0]["safe_context"]["url"] == expected, value


def test_skeleton_is_exactly_scheme_host_port():
    # Belt-and-suspenders: a valid skeleton passes strict validation; the values
    # echoed for common shapes are exactly scheme://host[:port]. A ';'-authority
    # is omitted (malformed), not reduced.
    from boomi_mcp.categories.components.connection_reuse import _safe_url_skeleton
    assert _safe_url_skeleton("https://api.acme.com/v1/orders?x=1") == "https://api.acme.com"
    assert _safe_url_skeleton("https://api.acme.com:8443/v1") == "https://api.acme.com:8443"
    assert _safe_url_skeleton("https://[2001:db8::1]:8443/v1") == "https://[2001:db8::1]:8443"
    assert _safe_url_skeleton("sftp://files.corp.net/in") == "sftp://files.corp.net"
    # ';' matrix without '@' reduces to host; ';'+'@' (ambiguous) is omitted.
    assert _safe_url_skeleton("https://api.acme.com;x=SECRET/v1") == "https://api.acme.com"
    assert _safe_url_skeleton("https://api.acme.com;x=y@z/v1") is None


def test_url_fragment_dropped_from_skeleton():
    # A credential-shaped fragment param is dropped with the whole fragment.
    settings = [_meta("rest1", "Acme REST", "#Common")]
    url = "https://api.example.com/v1#access_token=FRAGMENTSECRET"
    with (
        patch(f"{_MODULE}.paginate_metadata", side_effect=_paginate(settings)),
        patch(f"{_MODULE}.component_get_xml", side_effect=_get_xml({"rest1": _rest_xml_url(url)})),
    ):
        out = suggest_connection_reuse_action(_CLIENT, "prod", "rest")
    ctx_url = out["candidates"][0]["safe_context"]["url"]
    assert ctx_url == "https://api.example.com"
    assert "FRAGMENTSECRET" not in json.dumps(out) and "access_token" not in json.dumps(out)


def test_nested_credential_url_in_query_omitted():
    # A nested URL with userinfo inside a query value: the outer '@' triggers the
    # omit rule (any '@' → omit), so nothing leaks.
    settings = [_meta("rest1", "Acme REST", "#Common")]
    url = "https://api.example.com/cb?redirect=https://user:SUPERSECRET@evil.example/x"
    xml_url = url.replace("&", "&amp;")
    with (
        patch(f"{_MODULE}.paginate_metadata", side_effect=_paginate(settings)),
        patch(f"{_MODULE}.component_get_xml", side_effect=_get_xml({"rest1": _rest_xml_url(xml_url)})),
    ):
        out = suggest_connection_reuse_action(_CLIENT, "prod", "rest")
    assert "url" not in out["candidates"][0]["safe_context"]
    assert "SUPERSECRET" not in json.dumps(out)


def test_schemeless_endpoint_reduced_to_host():
    # A schemeless url/endpoint value is reduced to its bare host — query/path
    # (and any credentials therein) are dropped.
    settings = [_meta("rest1", "Acme REST", "#Common")]
    url = "api.example.com/v1?api_key=SECRETVALUE"
    xml_url = url.replace("&", "&amp;")
    with (
        patch(f"{_MODULE}.paginate_metadata", side_effect=_paginate(settings)),
        patch(f"{_MODULE}.component_get_xml", side_effect=_get_xml({"rest1": _rest_xml_url(xml_url)})),
    ):
        out = suggest_connection_reuse_action(_CLIENT, "prod", "rest")
    ctx_url = out["candidates"][0]["safe_context"]["url"]
    assert ctx_url == "api.example.com"
    assert "SECRETVALUE" not in json.dumps(out) and "api_key" not in json.dumps(out)


def test_boomi_placeholder_url_preserved():
    # A Boomi externalization placeholder ("SET IN EXTENSION") has whitespace but
    # no path delimiters — it must be echoed intact, not truncated to "SET".
    settings = [_meta("rest1", "Acme REST", "#Common")]
    with (
        patch(f"{_MODULE}.paginate_metadata", side_effect=_paginate(settings)),
        patch(f"{_MODULE}.component_get_xml", side_effect=_get_xml({"rest1": _rest_xml_url("SET IN EXTENSION")})),
    ):
        out = suggest_connection_reuse_action(_CLIENT, "prod", "rest")
    assert out["candidates"][0]["safe_context"]["url"] == "SET IN EXTENSION"


def test_schemeless_with_space_in_query_stripped():
    # Codex round-11: a schemeless URL with an unescaped space in its query must
    # still lose its path/query (the whitespace must NOT make it echo raw).
    settings = [_meta("rest1", "Acme REST", "#Common")]
    with (
        patch(f"{_MODULE}.paginate_metadata", side_effect=_paginate(settings)),
        patch(f"{_MODULE}.component_get_xml", side_effect=_get_xml({"rest1": _rest_xml_url("api.acme.com/callback?api_key=SECRET VALUE")})),
    ):
        out = suggest_connection_reuse_action(_CLIENT, "prod", "rest")
    ctx_url = out["candidates"][0]["safe_context"]["url"]
    assert ctx_url == "api.acme.com"
    assert "SECRET" not in json.dumps(out) and "callback" not in json.dumps(out)


def test_schemeless_matrix_without_at_reduces_to_host():
    # Codex round-12: a schemeless ';' matrix/property WITHOUT an '@' is
    # unambiguous — reduce to the bare host (the ';...' block dropped).
    settings = [_meta("rest1", "Acme REST", "#Common")]
    with (
        patch(f"{_MODULE}.paginate_metadata", side_effect=_paginate(settings)),
        patch(f"{_MODULE}.component_get_xml", side_effect=_get_xml({"rest1": _rest_xml_url("api.example.com;jsessionid=SECRET")})),
    ):
        out = suggest_connection_reuse_action(_CLIENT, "prod", "rest")
    ctx_url = out["candidates"][0]["safe_context"]["url"]
    assert ctx_url == "api.example.com"
    blob = json.dumps(out)
    assert "SECRET" not in blob and "jsessionid" not in blob


def test_schemeless_semicolon_and_at_omitted():
    # Codex round-14/23: a schemeless authority with a ';' AND an '@' is ambiguous
    # (userinfo-';' vs matrix-'@' can't be told apart) — omit, never echo.
    from boomi_mcp.categories.components.connection_reuse import _safe_url_skeleton
    for leaky in (
        "user:p;ass@api.example.com",
        "user:pass@api.example.com;token=SECRET",
        "user:p;ass@api.example.com:8080/path;jsessionid=Z",
    ):
        assert _safe_url_skeleton(leaky) is None, leaky


def test_jdbc_host_stops_at_last_userinfo_at():
    # Codex round-14/15 P2: a JDBC password with an unescaped '@' must not leak
    # into the extracted host — userinfo runs to the LAST '@' in BOTH the '//'
    # form and the Oracle Thin SID '@host' fallback.
    from boomi_mcp.categories.components.connection_reuse import _jdbc_host
    assert _jdbc_host("jdbc:mysql://user:p@ss@db.acme.com:3306/x") == "db.acme.com"
    assert _jdbc_host("jdbc:mysql://user:pass@db.acme.com:3306/x") == "db.acme.com"
    assert _jdbc_host("jdbc:mysql://db.acme.com:3306/x") == "db.acme.com"
    # Oracle Thin SID (no '//') with a multi-'@' password.
    assert _jdbc_host("jdbc:oracle:thin:scott/tig@er@db.acme.com:1521:ORCL") == "db.acme.com"
    assert _jdbc_host("jdbc:oracle:thin:@db.acme.com:1521:ORCL") == "db.acme.com"
    assert _jdbc_host("jdbc:oracle:thin:@//db.acme.com:1521/svc") == "db.acme.com"
    # Colon-separated host descriptor (Sybase jConnect / Informix) — host is the
    # segment before the numeric port.
    assert _jdbc_host("jdbc:sybase:Tds:db.acme.com:5000/orders") == "db.acme.com"
    assert _jdbc_host("jdbc:informix-sqli:db.acme.com:1526/stores") == "db.acme.com"
    # No host to find → '' (in-memory / file DBs).
    assert _jdbc_host("jdbc:h2:mem:testdb") == ""
    assert _jdbc_host("jdbc:sqlite:/var/db/app.db") == ""
    # Codex round-18: a ';password=p@ss' PROPERTY '@' after the host must NOT be
    # taken as the userinfo '@' (userinfo stops at the ';' authority delimiter).
    assert _jdbc_host("jdbc:sqlserver://db.acme.com:1433;password=p@ss;database=x") == "db.acme.com"
    assert _jdbc_host("jdbc:sqlserver://db.acme.com;user=a@b") == "db.acme.com"
    # Codex round-20: SQL Server property-based host form (serverName=), where the
    # host is not in the //authority.
    assert _jdbc_host("jdbc:sqlserver://;serverName=db.acme.com;databaseName=x") == "db.acme.com"
    assert _jdbc_host("jdbc:sqlserver://;serverName=db.acme.com\\INST;databaseName=x") == "db.acme.com"
    # Codex round-21: a ';password=p@ss' property '@' must NOT shadow the
    # serverName host via the Oracle @-fallback (which is restricted to pre-';').
    assert _jdbc_host("jdbc:sqlserver://;serverName=db.acme.com;password=p@ss") == "db.acme.com"
    assert _jdbc_host("jdbc:sqlserver://;password=p@ss;serverName=db.acme.com") == "db.acme.com"
    # Codex round-22 P2b: bracketed IPv6 authority — the full address, not '[2001'.
    assert _jdbc_host("jdbc:postgresql://[2001:db8::1]:5432/db") == "2001:db8::1"
    assert _jdbc_host("jdbc:mysql://user:p@ss@[2001:db8::2]:3306/x") == "2001:db8::2"
    # Codex round-22 P2a: a braced password containing 'serverName=' text must NOT
    # be mistaken for the real host.
    assert _jdbc_host("jdbc:sqlserver://;password={p;serverName=secret};serverName=db.acme.com") == "db.acme.com"
    # Codex round-24 P2a: a '//' inside a property value must NOT shadow the
    # serverName host (the //authority scan is restricted to the pre-';' segment).
    assert _jdbc_host("jdbc:sqlserver://;serverName=db.acme.com;password=p//secret") == "db.acme.com"
    assert _jdbc_host("jdbc:mysql://host.acme.com:3306/db;sessionVariables=x") == "host.acme.com"
    # Codex round-26: a SQL Server named-instance suffix ('\INST') must be stripped
    # so an endpoint_hint of the bare host scores an exact (not substring) match.
    assert _jdbc_host("jdbc:sqlserver://db.acme.com\\INST:1433;databaseName=x") == "db.acme.com"
    # Codex round-27: a colon-style JDBC IPv4 host (starts with a digit) must not
    # be mistaken for the port (the port segment is purely numeric).
    assert _jdbc_host("jdbc:sybase:Tds:10.0.0.1:5000/orders") == "10.0.0.1"
    # Codex round-28/29/31: JDBC userinfo ends at the LAST '@' regardless of how
    # many '@'/'/'/':'/';' the password contains — never a pre-'@' username.
    assert _jdbc_host("jdbc:mysql://svc:p/ss@db.acme.com:3306/x") == "db.acme.com"
    assert _jdbc_host("jdbc:mysql://user:p@/ss@db.acme.com:3306/app") == "db.acme.com"
    assert _jdbc_host("jdbc:mysql://svc_user:pa;ss@db.acme.com:3306/app") == "db.acme.com"  # ';' in pwd
    assert _jdbc_host("jdbc:mysql://a@b@c@db.acme.com:3306") == "db.acme.com"
    assert _jdbc_host("jdbc:mysql://user:pass@[2001:db8::1]:3306") == "2001:db8::1"
    # A '//' inside a property value is NOT the authority (first '//' wins).
    assert _jdbc_host("jdbc:sqlserver://;serverName=db.acme.com;password=p//secret") == "db.acme.com"


def test_endpoint_hint_normalized_like_candidates():
    # Codex round-24 P2b: a hint with a query/matrix/scheme must be reduced the
    # same way candidate values are, so it still matches an exact host.
    from boomi_mcp.categories.components.connection_reuse import _endpoint_score
    assert _endpoint_score("api.example.com?wsdl", ["api.example.com"])[0] == 30
    assert _endpoint_score("https://api.example.com;x=1/v1", ["api.example.com"])[0] == 30
    assert _endpoint_score("host.docker.internal", ["http://host.docker.internal:8081"])[0] == 30


def test_host_of_handles_ipv6():
    # QA round-22 follow-up: a bare IPv6 must NOT be truncated at the first ':'
    # (would false-match distinct IPv6 hosts sharing the first hextet).
    from boomi_mcp.categories.components.connection_reuse import _host_of, _endpoint_score
    assert _host_of("2001:db8::1") == "2001:db8::1"
    assert _host_of("[2001:db8::1]:8443") == "2001:db8::1"
    assert _host_of("https://[2001:db8::1]:8443/x") == "2001:db8::1"
    assert _host_of("host.docker.internal:8081") == "host.docker.internal"
    # Distinct IPv6 hosts must NOT collapse to a false exact match.
    assert _endpoint_score("2001:db8::1", ["2001:db8::99"])[0] < 30
    assert _endpoint_score("2001:db8::1", ["2001:db8::1"])[0] == 30


def test_schemeless_backslash_path_stripped():
    # Codex round-18: a schemeless value using backslash path separators must be
    # reduced to the bare host (the '\path' must not be echoed).
    settings = [_meta("rest1", "Acme REST", "#Common")]
    for leaky in (
        "api.example.com\\services\\WEBHOOKSECRET",
        "api.example.com\\..\\..\\etc\\passwd",
    ):
        with (
            patch(f"{_MODULE}.paginate_metadata", side_effect=_paginate(settings)),
            patch(f"{_MODULE}.component_get_xml", side_effect=_get_xml({"rest1": _rest_xml_url(leaky)})),
        ):
            out = suggest_connection_reuse_action(_CLIENT, "prod", "rest")
        ctx_url = out["candidates"][0]["safe_context"]["url"]
        assert ctx_url == "api.example.com", leaky
        blob = json.dumps(out)
        assert "WEBHOOKSECRET" not in blob and "services" not in blob and "passwd" not in blob


def test_jdbc_endpoint_hint_matches_custom_url_candidate():
    # Codex round-12: a caller passing a JDBC URL as the endpoint_hint must still
    # match a custom_url candidate (hint host extracted the same way as candidate).
    settings = [_meta("db1", "Snowflake DW", "#Common")]
    url_format = "jdbc:snowflake://acct.us-east-1.snowflakecomputing.com/?db=ANALYTICS"
    xml = _db_xml_custom_url(url_format)
    with (
        patch(f"{_MODULE}.paginate_metadata", side_effect=_paginate(settings)),
        patch(f"{_MODULE}.component_get_xml", side_effect=_get_xml({"db1": xml})),
    ):
        out = suggest_connection_reuse_action(
            _CLIENT, "prod", "database",
            endpoint_hint="jdbc:snowflake://acct.us-east-1.snowflakecomputing.com/?db=OTHER",
        )
    cand = out["candidates"][0]
    assert any("exact host match" in r for r in cand["why_matched"])
    assert "snowflakecomputing.com/?db" not in json.dumps(out)


def test_oracle_thin_sid_host_extracted_for_matching():
    # Codex round-13 P2b: Oracle Thin SID URLs (jdbc:oracle:thin:@host:port:sid)
    # have no '//' — the host must still be extracted for matching (not echoed).
    settings = [_meta("db1", "Oracle DW", "#Common")]
    url_format = "jdbc:oracle:thin:scott/tiger@db.acme.com:1521:ORCL"
    xml = _db_xml_custom_url(url_format)
    with (
        patch(f"{_MODULE}.paginate_metadata", side_effect=_paginate(settings)),
        patch(f"{_MODULE}.component_get_xml", side_effect=_get_xml({"db1": xml})),
    ):
        out = suggest_connection_reuse_action(
            _CLIENT, "prod", "database", endpoint_hint="db.acme.com",
        )
    cand = out["candidates"][0]
    assert any("exact host match (db.acme.com)" in r for r in cand["why_matched"])
    # The connection string (with creds) is never echoed.
    assert "urlFormat" not in cand["safe_context"]
    assert "tiger" not in json.dumps(out) and "scott" not in json.dumps(out)


def _db_v2_xml(url_value):
    """Database V2-style connector-settings: a GenericConnectionConfig with a
    JDBC url field (no DatabaseConnectionSettings scalar attrs)."""
    return (
        '<bns:Component xmlns:bns="http://api.platform.boomi.com/" '
        'type="connector-settings" subType="officialboomi-X3979C-dbv2da-prod" '
        'name="db" folderName="f">'
        "<bns:object>"
        '<GenericConnectionConfig xmlns="">'
        f'<field id="url" type="string" value="{url_value}"/>'
        '<field id="password" type="password" value="[encrypted]"/>'
        "</GenericConnectionConfig>"
        "</bns:object></bns:Component>"
    )


def test_dbv2_jdbc_url_field_host_extracted_not_echoed():
    # Codex round-13 P2a: a Database V2 GenericConnectionConfig JDBC url field is
    # never echoed, but its host is extracted for matching.
    settings = [_meta("db1", "DBv2 Conn", "#Common")]
    xml = _db_v2_xml("jdbc:mysql://db.acme.com:3306/app")
    with (
        patch(f"{_MODULE}.paginate_metadata", side_effect=_paginate(settings)),
        patch(f"{_MODULE}.component_get_xml", side_effect=_get_xml({"db1": xml})),
    ):
        out = suggest_connection_reuse_action(
            _CLIENT, "prod", "officialboomi-X3979C-dbv2da-prod",
            endpoint_hint="db.acme.com",
        )
    cand = out["candidates"][0]
    assert any("exact host match (db.acme.com)" in r for r in cand["why_matched"])
    # The JDBC url is never echoed in safe_context.
    assert "url" not in cand["safe_context"]
    assert "jdbc:" not in json.dumps(out)


def test_schemeless_percent_encoded_delimiters_omitted():
    # Codex round-19 P1: percent-encoded delimiters (%3F, %2F) evade literal
    # splitting; such a value is NOT a pure placeholder, so it must be omitted,
    # never echoed with the embedded credential.
    settings = [_meta("rest1", "Acme REST", "#Common")]
    for leaky in (
        "api.example.com%3Fapi_key=SECRET",
        "api.example.com%2Fservices%2FWEBHOOKSECRET",
    ):
        with (
            patch(f"{_MODULE}.paginate_metadata", side_effect=_paginate(settings)),
            patch(f"{_MODULE}.component_get_xml", side_effect=_get_xml({"rest1": _rest_xml_url(leaky)})),
        ):
            out = suggest_connection_reuse_action(_CLIENT, "prod", "rest")
        ctx = out["candidates"][0]["safe_context"]
        assert "url" not in ctx, leaky  # omitted, not echoed
        blob = json.dumps(out)
        assert "SECRET" not in blob and "api_key" not in blob and "WEBHOOK" not in blob


def test_schemeless_templated_port_keeps_host():
    # Codex round-19 P2: a schemeless externalized port (…:${PORT}) must keep the
    # host (mirrors the standard-URL branch), not drop it.
    settings = [_meta("rest1", "Acme REST", "#Common")]
    with (
        patch(f"{_MODULE}.paginate_metadata", side_effect=_paginate(settings)),
        patch(f"{_MODULE}.component_get_xml", side_effect=_get_xml({"rest1": _rest_xml_url("api.example.com:${PORT}/v1")})),
    ):
        out = suggest_connection_reuse_action(
            _CLIENT, "prod", "rest", endpoint_hint="api.example.com",
        )
    cand = out["candidates"][0]
    assert cand["safe_context"]["url"] == "api.example.com"
    assert any("exact host match (api.example.com)" in r for r in cand["why_matched"])


def test_templated_port_keeps_host():
    # Codex round-11: a templated/externalized port (…:${PORT}) makes parts.port
    # raise ValueError; the host must be kept (not discarded to "https:").
    settings = [_meta("rest1", "Acme REST", "#Common")]
    with (
        patch(f"{_MODULE}.paginate_metadata", side_effect=_paginate(settings)),
        patch(f"{_MODULE}.component_get_xml", side_effect=_get_xml({"rest1": _rest_xml_url("https://api.acme.com:${PORT}/v1")})),
    ):
        out = suggest_connection_reuse_action(
            _CLIENT, "prod", "rest", endpoint_hint="api.acme.com",
        )
    cand = out["candidates"][0]
    assert cand["safe_context"]["url"] == "https://api.acme.com"
    assert any("exact host match (api.acme.com)" in r for r in cand["why_matched"])


def test_ipv6_host_skeleton_bracketed():
    # An IPv6 literal host is bracketed correctly in the skeleton; no leak.
    settings = [_meta("rest1", "Acme REST", "#Common")]
    url = "https://[2001:db8::1]:8443/v1?token=SECRET"
    xml_url = url.replace("&", "&amp;")
    with (
        patch(f"{_MODULE}.paginate_metadata", side_effect=_paginate(settings)),
        patch(f"{_MODULE}.component_get_xml", side_effect=_get_xml({"rest1": _rest_xml_url(xml_url)})),
    ):
        out = suggest_connection_reuse_action(_CLIENT, "prod", "rest")
    ctx_url = out["candidates"][0]["safe_context"]["url"]
    assert ctx_url == "https://[2001:db8::1]:8443"
    assert "token" not in json.dumps(out)


# Every JDBC connection-string shape (driver-specific credential grammars) is
# reduced to nothing echoable: urlFormat is OMITTED from safe_context (its host
# is carried by the separate `host` scalar attr), so no JDBC credential can leak.
_JDBC_LEAKY_URL_FORMATS = [
    "jdbc:mysql://db.acme.com:3306/orders;user=sa;password=topsecret",       # ';' matrix
    "jdbc:postgresql://db.acme.com/app?user=svc&password=topsecret",         # '?' query (Postgres)
    "jdbc:sqlserver://db.acme.com:1433;password=p@ss;database=x",            # '@' in password
    "jdbc:sqlserver://db.acme.com:1433;password={p;ass};database=x",         # braced ';' value
    "jdbc:mysql://user:pass@db.acme.com:3306/orders;applicationName=svc@corp",  # '//user:pass@'
    "jdbc:oracle:thin:scott/tiger@//db.acme.com:1521/orcl",                  # Oracle thin creds
]


@pytest.mark.parametrize("url_format", _JDBC_LEAKY_URL_FORMATS)
def test_jdbc_connection_string_never_echoed(url_format):
    settings = [_meta("db1", "Orders DB", "#Common")]
    with (
        patch(f"{_MODULE}.paginate_metadata", side_effect=_paginate(settings)),
        patch(f"{_MODULE}.component_get_xml", side_effect=_get_xml({"db1": _db_xml("db.acme.com", url_format=url_format)})),
    ):
        out = suggest_connection_reuse_action(_CLIENT, "prod", "database")
    ctx = out["candidates"][0]["safe_context"]
    # urlFormat (the JDBC string) is never echoed; DB identity is the scalars.
    assert "urlFormat" not in ctx
    assert ctx["host"] == "db.acme.com"
    assert ctx["driverId"] == "sqlserver"
    blob = json.dumps(out)
    for banned in ("topsecret", "password", "p@ss", "p;ass", "tiger", "user:pass", "scott"):
        assert banned not in blob


def _db_xml_custom_url(url_format):
    """custom_url-shape database XML: no host/dbname attrs, host lives only in
    urlFormat (Snowflake and similar)."""
    return (
        '<bns:Component xmlns:bns="http://api.platform.boomi.com/" '
        'type="connector-settings" subType="database" name="db" folderName="f">'
        "<bns:object>"
        f'<DatabaseConnectionSettings xmlns="" driverId="snowflake" '
        f'urlFormat="{url_format}"/>'
        "</bns:object></bns:Component>"
    )


def test_custom_jdbc_host_matches_via_endpoint_hint():
    # Codex round-10 P2: a custom_url DB connection has the host ONLY in urlFormat
    # (empty host/port/dbname attrs). The host must still be extracted for
    # endpoint MATCHING (not echoed) so an endpoint_hint can distinguish it.
    settings = [_meta("db1", "Snowflake DW", "#Common")]
    url_format = "jdbc:snowflake://acct.us-east-1.snowflakecomputing.com/?db=ANALYTICS"
    xml = _db_xml_custom_url(url_format)
    with (
        patch(f"{_MODULE}.paginate_metadata", side_effect=_paginate(settings)),
        patch(f"{_MODULE}.component_get_xml", side_effect=_get_xml({"db1": xml})),
    ):
        out = suggest_connection_reuse_action(
            _CLIENT, "prod", "database",
            endpoint_hint="acct.us-east-1.snowflakecomputing.com",
        )
    cand = out["candidates"][0]
    # Host matched for ranking even though it is never echoed.
    assert any("exact host match" in r for r in cand["why_matched"])
    assert "urlFormat" not in cand["safe_context"]
    # The connection string itself never leaks.
    assert "snowflakecomputing.com/?db" not in json.dumps(out)


def test_enrichment_passes_shared_deadline_budget():
    # Codex P2: each component read must receive an explicit deadline budget.
    settings = [_meta("db1", "Orders DB", "#Common")]
    seen = {}

    def _capture(client, component_id, deadline_seconds=None, **kw):
        seen["deadline_seconds"] = deadline_seconds
        return {"xml": _db_xml("h"), "component_id": component_id}

    with (
        patch(f"{_MODULE}.paginate_metadata", side_effect=_paginate(settings)),
        patch(f"{_MODULE}.component_get_xml", side_effect=_capture),
    ):
        out = suggest_connection_reuse_action(_CLIENT, "prod", "database")
    assert out["_success"] is True
    assert isinstance(seen["deadline_seconds"], int) and seen["deadline_seconds"] >= 1


def test_enrichment_budget_exhausted_skips_reads_gracefully():
    # Codex P2: when the aggregate budget is spent, remaining candidates are left
    # un-enriched rather than starting more (possibly stalling) reads.
    settings = [_meta(f"c{i}", f"Conn {i}", "#Common") for i in range(3)]
    calls = {"n": 0}

    def _get(client, component_id, deadline_seconds=None, **kw):
        calls["n"] += 1
        return {"xml": _db_xml("h"), "component_id": component_id}

    with (
        patch(f"{_MODULE}.paginate_metadata", side_effect=_paginate(settings)),
        patch(f"{_MODULE}._component_get_deadline_seconds", return_value=0),
        patch(f"{_MODULE}.component_get_xml", side_effect=_get),
    ):
        out = suggest_connection_reuse_action(_CLIENT, "prod", "database")
    assert out["_success"] is True
    assert out["enrichment_budget_exhausted"] is True
    assert calls["n"] == 0  # zero budget → no reads started
    # Candidates still returned, just un-enriched (subtype score only).
    assert len(out["candidates"]) == 3
    assert all(c["safe_context"] == {} for c in out["candidates"])


def test_generic_exception_is_type_only():
    # Repo-gate follow-up: a generic (non-ApiError) exception is an UNBOUNDED text
    # surface — the envelope must echo ONLY the exception type, never str(e), so no
    # credential in any format (JSON, spaced passphrase, composite key, userinfo)
    # can leak. We do NOT sanitize-and-echo the unknowable; we drop the message.
    def _boom(client, query_config, **kw):
        raise RuntimeError(
            'Login failed: {"db_password": "hunter2secret"} '
            "scott/tiger@//db token=eyJabc.def.ghijklmnop.qrstuvwx"
        )

    with patch(f"{_MODULE}.paginate_metadata", side_effect=_boom):
        out = suggest_connection_reuse_action(_CLIENT, "prod", "database")
    assert out["_success"] is False
    blob = json.dumps(out)
    # Not one fragment of the exception message survives — only the type name.
    assert "hunter2secret" not in blob and "eyJabc" not in blob and "tiger" not in blob
    assert out["exception_type"] == "RuntimeError"
    assert out["error"] == "Failed to query reusable connections (unexpected RuntimeError)."
    assert out["read_only"] is True and out["boomi_mutation"] is False and out["raw_xml_exposed"] is False


def test_apierror_is_type_only_with_status():
    # Repo-gate final resolution: an ApiError message is an UNBOUNDED text surface
    # (a platform/driver error can embed a credential in any quoted/escaped/JSON/
    # header format), so the envelope NEVER echoes it. It surfaces only leak-proof
    # bounded signals — the exception type name and the numeric HTTP status — which
    # is the actionable part anyway. Contract holds by construction, not by regex.
    from boomi.net.transport.api_error import ApiError

    def _boom(client, query_config, **kw):
        # A credential embedded with an escaped quote (the format that defeated the
        # old regex sanitizer) must not appear even as a suffix.
        raise ApiError('rejected: {"password": "hunter\\"2secret"} token=eyJabc.def', status=401)

    with patch(f"{_MODULE}.paginate_metadata", side_effect=_boom):
        out = suggest_connection_reuse_action(_CLIENT, "prod", "database")
    assert out["_success"] is False
    blob = json.dumps(out)
    # No fragment of the message survives — not the secret, not the escaped suffix.
    assert "hunter" not in blob and "2secret" not in blob and "eyJabc" not in blob
    assert out["exception_type"] == "ApiError"
    assert out["http_status"] == 401
    assert out["error"] == "Failed to query reusable connections (HTTP 401)."
    assert out["read_only"] is True and out["boomi_mutation"] is False and out["raw_xml_exposed"] is False


def test_apierror_without_status_omits_http_status():
    # A non-int / absent status degrades gracefully: http_status is None and the
    # message carries no "(HTTP …)" suffix — still no echoed text.
    from boomi.net.transport.api_error import ApiError

    def _boom(client, query_config, **kw):
        raise ApiError("password=leaked")  # no status arg

    with patch(f"{_MODULE}.paginate_metadata", side_effect=_boom):
        out = suggest_connection_reuse_action(_CLIENT, "prod", "database")
    assert "leaked" not in json.dumps(out)
    assert out["http_status"] is None
    assert out["error"] == "Failed to query reusable connections."


def test_unknown_connector_family_is_raw():
    # Architect review §6 (low): an unrecognised subtype must yield connector_type
    # "raw" in the reference binding, with the real subtype kept in `subtype`.
    settings = [_meta("c1", "SFTP Conn", "#Common")]
    with (
        patch(f"{_MODULE}.paginate_metadata", side_effect=_paginate(settings)),
        patch(f"{_MODULE}.component_get_xml", side_effect=_get_xml({"c1": ""})),
    ):
        out = suggest_connection_reuse_action(_CLIENT, "prod", "sftp")
    assert out["resolved_subtype"] == "sftp"
    assert out["connector_family"] == "raw"
    ref = out["candidates"][0]["reference"]
    assert ref["reference_only_config"]["connector_type"] == "raw"
    assert out["candidates"][0]["subtype"] == "sftp"  # real subtype preserved


def test_endpoint_affinity_admits_exact_host_folder_over_higher_cheap():
    # Repo-gate P2 + §6 re-review: in a large account (> working_cap matching
    # components), a folder that names the hinted host must reach the bounded
    # XML-enrichment window even against MANY higher-cheap candidates. Here 25
    # #Common candidates score cheap 55 (40 subtype + 15 shared-folder) — a +5
    # prefilter nudge (→45) could NOT beat them, so the admission tier ranks any
    # endpoint-affinity folder STRICTLY ahead. The true match (cheap 40) is admitted
    # and earns its real endpoint score despite being outscored on cheap.
    hint = "db.acme.com"
    settings = [_meta(f"f{i:02d}", f"zzzz-{i:02d}", "/#Common") for i in range(25)]
    xml = {
        f"f{i:02d}": _rest_xml_url(f"https://other{i:02d}.example.internal/v1")
        for i in range(25)
    }
    settings.append(_meta("TGT", "0000-zzzz", "/Integrations/db.acme.com/prod"))
    xml["TGT"] = _rest_xml_url("https://db.acme.com/v1")

    with (
        patch(f"{_MODULE}.paginate_metadata", side_effect=_paginate(settings)),
        patch(f"{_MODULE}.component_get_xml", side_effect=_get_xml(xml)),
    ):
        out = suggest_connection_reuse_action(
            _CLIENT, "prod", "rest", purpose=None, endpoint_hint=hint, top_k=5
        )

    assert out["enrichment_capped"] is True  # 26 matches > working_cap (20)
    ids = [c["component_id"] for c in out["candidates"]]
    assert "TGT" in ids, "exact-host-folder match crowded out by higher-cheap #Common"
    tgt = next(c for c in out["candidates"] if c["component_id"] == "TGT")
    # Exact host match earns the 30-pt endpoint bucket; the admission tier stays OUT
    # of the final score → 40 (subtype) + 30 (exact host) == 70. Its endpoint score
    # (70) also beats the crowd of cheap-55 #Common candidates in the final ranking.
    assert tgt["score"] == 70
    assert out["candidates"][0]["component_id"] == "TGT"
    assert any("exact host match" in r for r in tgt["why_matched"])


def test_affinity_false_positives_do_not_evict_high_cheap_exact_match():
    # Repo-gate P2 (symmetric): a STRICT affinity-first tier over-corrects — many
    # affinity FALSE POSITIVES (folders that merely share a host token but whose
    # endpoint does not match) would evict a genuine high-cheap non-affinity exact
    # match. The cheap-primary window must keep that match. Here 20 stale /Acme
    # candidates (cheap 40, affinity on "acme") must NOT crowd the /#Common
    # exact-endpoint match (cheap 55) out of enrichment.
    hint = "api.acme.com"
    settings = [_meta(f"a{i:02d}", f"zzzz-{i:02d}", "/Acme") for i in range(20)]
    xml = {f"a{i:02d}": _rest_xml_url(f"https://acme{i:02d}.example.internal/v1") for i in range(20)}
    settings.append(_meta("CMN", "0000-zzzz", "/#Common"))  # cheap 55 (shared folder), no affinity
    xml["CMN"] = _rest_xml_url("https://api.acme.com/v1")  # the true exact-endpoint match

    with (
        patch(f"{_MODULE}.paginate_metadata", side_effect=_paginate(settings)),
        patch(f"{_MODULE}.component_get_xml", side_effect=_get_xml(xml)),
    ):
        out = suggest_connection_reuse_action(
            _CLIENT, "prod", "rest", purpose=None, endpoint_hint=hint, top_k=5
        )

    ids = [c["component_id"] for c in out["candidates"]]
    assert "CMN" in ids, "high-cheap non-affinity exact match evicted by affinity false positives"
    cmn = next(c for c in out["candidates"] if c["component_id"] == "CMN")
    assert cmn["score"] == 85  # 40 subtype + 15 #Common + 30 exact host
    assert out["candidates"][0]["component_id"] == "CMN"


def test_reserve_ranks_by_affinity_strength_not_cheap_tie():
    # Repo-gate P2 (reserve ranking): when more below-cap affinity candidates exist
    # than the reserve holds, the reserve must prefer the MOST specific host match,
    # not an arbitrary cheap/name tie. Hint api.acme.com: 6 weak /Acme matches
    # (strength 1, names sort high) + 1 exact /Integrations/api.acme.com match
    # (strength 3, name sorts low), all below a full top-20 cheap window. A cheap/
    # name reserve would take the 6 /Acme and drop the exact; strength ranking keeps
    # the exact one.
    hint = "api.acme.com"
    settings = [_meta(f"c{i:02d}", f"0000-w{i:02d}", "/#Common") for i in range(20)]  # cheap 55, fills primary
    xml = {f"c{i:02d}": _rest_xml_url(f"https://cmn{i:02d}.example.internal/v1") for i in range(20)}
    for i in range(6):  # weak affinity, cheap 40, names sort ABOVE the exact match
        settings.append(_meta(f"a{i}", f"zzzz-{i}", "/Acme"))
        xml[f"a{i}"] = _rest_xml_url(f"https://acme{i}.example.internal/v1")
    settings.append(_meta("EXACT", "0000-zzzz", "/Integrations/api.acme.com"))  # strength 3, sorts low
    xml["EXACT"] = _rest_xml_url("https://api.acme.com/v1")

    with (
        patch(f"{_MODULE}.paginate_metadata", side_effect=_paginate(settings)),
        patch(f"{_MODULE}.component_get_xml", side_effect=_get_xml(xml)),
    ):
        out = suggest_connection_reuse_action(
            _CLIENT, "prod", "rest", purpose=None, endpoint_hint=hint, top_k=5
        )

    ids = [c["component_id"] for c in out["candidates"]]
    assert "EXACT" in ids, "most-specific host match lost a reserve slot to weaker affinity matches"
    exact = next(c for c in out["candidates"] if c["component_id"] == "EXACT")
    assert exact["score"] == 70  # 40 subtype + 30 exact host (no affinity/tier points)
    assert out["candidates"][0]["component_id"] == "EXACT"


def test_endpoint_affinity_absent_from_final_score():
    # The admission tier must never reach the final score: a folder whose segment
    # shares a host token but whose endpoint does NOT match scores subtype-only (40),
    # not 45 — the tier only orders enrichment admission, it grants no points.
    settings = [_meta("c1", "0000-zzzz", "/Integrations/db.acme.com/prod")]
    xml = {"c1": _rest_xml_url("https://unrelated.example.internal/v1")}
    with (
        patch(f"{_MODULE}.paginate_metadata", side_effect=_paginate(settings)),
        patch(f"{_MODULE}.component_get_xml", side_effect=_get_xml(xml)),
    ):
        out = suggest_connection_reuse_action(
            _CLIENT, "prod", "rest", purpose=None, endpoint_hint="db.acme.com", top_k=5
        )
    assert out["candidates"][0]["score"] == 40  # subtype only — no leaked tier points


def test_no_endpoint_hint_ordering_unchanged():
    # With no endpoint_hint every `_affinity` is 0, so the reserve is empty and the
    # window reduces exactly to the prior `(_cheap, name)` ordering — #Common (55) beats a
    # plain-folder one (cheap 40); no endpoint machinery perturbs the no-hint path.
    settings = [
        _meta("shared", "0000-a", "/#Common"),
        _meta("plain", "0000-b", "/Misc"),
    ]
    xml = {
        "shared": _rest_xml_url("https://a.example.internal/v1"),
        "plain": _rest_xml_url("https://b.example.internal/v1"),
    }
    with (
        patch(f"{_MODULE}.paginate_metadata", side_effect=_paginate(settings)),
        patch(f"{_MODULE}.component_get_xml", side_effect=_get_xml(xml)),
    ):
        out = suggest_connection_reuse_action(
            _CLIENT, "prod", "rest", purpose=None, endpoint_hint=None, top_k=5
        )
    assert out["candidates"][0]["component_id"] == "shared"  # #Common wins on cheap


def test_query_uses_type_and_subtype_filter():
    # Architect review §6 (low): the metadata query must actually filter by
    # TYPE == connector-settings AND SUBTYPE == the resolved subtype.
    captured = []

    def _cap(client, query_config, **kw):
        exprs = query_config.query_filter.expression.nested_expression
        captured.append([a for e in exprs for a in (getattr(e, "argument", None) or [])])
        return []

    with patch(f"{_MODULE}.paginate_metadata", side_effect=_cap):
        suggest_connection_reuse_action(_CLIENT, "prod", "rest")
    assert captured, "paginate_metadata was not called"
    args = captured[0]
    assert "connector-settings" in args
    assert "officialboomi-X3979C-rest-prod" in args  # resolved REST subtype


def test_missing_connector_type_errors_with_flags():
    out = suggest_connection_reuse_action(_CLIENT, "prod", "")
    assert out["_success"] is False
    assert out["error_code"] == "CONNECTION_REUSE_QUERY_FAILED"
    assert out["read_only"] is True
    assert out["boomi_mutation"] is False
    assert out["raw_xml_exposed"] is False
