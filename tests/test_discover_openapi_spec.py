"""Issue #13 (M7): handler tests for ``discover_openapi_spec_action``.

Exercises artifact + mocked-URL modes, Swagger 2.0 / OpenAPI 3.x parsing, the
exactly-one-of source contract, the structured error taxonomy (parse / invalid
spec / unsupported format+version / auth / unreachable / redirect / SSRF / size),
SSRF controls (no auth/cookies/redirects/proxy), truncation, and leak-proofing —
all without real network access.
"""

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

import boomi_mcp.categories.schema_discovery as sd
from boomi_mcp.categories.schema_discovery import discover_openapi_spec_action

_PUBLIC_IP = "93.184.216.34"


# ---------------------------------------------------------------------------
# Mock helpers
# ---------------------------------------------------------------------------

def _stream_client(status=200, body=b"", raise_exc=None):
    """Build a MagicMock standing in for ``httpx.Client`` (the class) whose
    ``stream(...)`` returns a context manager yielding a response with the given
    status and streamed body bytes."""
    resp = MagicMock()
    resp.status_code = status
    resp.iter_bytes.return_value = iter([body] if body else [])
    stream_cm = MagicMock()
    stream_cm.__enter__.return_value = resp
    stream_cm.__exit__.return_value = False
    client = MagicMock()
    if raise_exc is not None:
        client.stream.side_effect = raise_exc
    else:
        client.stream.return_value = stream_cm
    client.__enter__.return_value = client
    client.__exit__.return_value = False
    return MagicMock(return_value=client), client


def _public_gai(host, *a, **k):
    return [(2, 1, 6, "", (_PUBLIC_IP, 0))]


def _private_gai(host, *a, **k):
    return [(2, 1, 6, "", ("10.1.2.3", 0))]


def _oas3():
    return {
        "openapi": "3.0.1",
        "info": {"title": "Petstore"},
        "servers": [{"url": "https://user:pw@api.example.com/v1?token=abc"}],
        "paths": {
            "/pets": {
                "parameters": [
                    {"name": "tenant", "in": "header", "required": True, "schema": {"type": "string"}}
                ],
                "get": {
                    "operationId": "listPets",
                    "summary": "List pets",
                    "parameters": [
                        {"name": "limit", "in": "query", "required": False, "schema": {"type": "integer"}}
                    ],
                    "responses": {
                        "200": {
                            "description": "ok",
                            "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Pet"}}},
                        }
                    },
                },
                "post": {
                    "operationId": "createPet",
                    "requestBody": {"content": {"application/json": {"schema": {"$ref": "#/components/schemas/Pet"}}}},
                    "responses": {"201": {"description": "created"}},
                },
            }
        },
        "components": {
            "schemas": {
                "Pet": {
                    "type": "object",
                    "required": ["id"],
                    "properties": {"id": {"type": "integer"}, "name": {"type": "string"}},
                }
            }
        },
    }


def _swagger2():
    return {
        "swagger": "2.0",
        "info": {"title": "Acme"},
        "host": "api.acme.com",
        "basePath": "/v2",
        "schemes": ["https"],
        "paths": {"/things": {"get": {"responses": {"200": {"description": "ok", "schema": {"type": "string"}}}}}},
        "definitions": {"Thing": {"type": "object", "properties": {"a": {"type": "string"}}}},
    }


# ---------------------------------------------------------------------------
# Success — artifact mode
# ---------------------------------------------------------------------------

def test_openapi3_artifact_dict_success():
    r = discover_openapi_spec_action(artifact=_oas3())
    assert r["_success"] is True
    assert r["read_only"] is True and r["boomi_mutation"] is False and r["raw_xml_exposed"] is False
    assert r["format"] == "openapi" and r["version"] == "3.0.1"
    assert r["source_mode"] == "artifact"
    assert [(o["path"], o["method"]) for o in r["operations"]] == [("/pets", "GET"), ("/pets", "POST")]
    get_op = r["operations"][0]
    # path-level + operation-level parameters merge
    assert {p["name"] for p in get_op["parameters"]} == {"tenant", "limit"}
    assert get_op["responses"][0]["schema"]["ref"] == "Pet"
    post_op = r["operations"][1]
    assert post_op["request_schema"]["ref"] == "Pet"
    assert r["schemas"][0]["name"] == "Pet"
    assert r["schemas"][0]["required_fields"] == ["id"]
    assert r["counts"] == {"operations": 2, "schemas": 1}


def test_openapi3_servers_sanitized_no_credentials():
    r = discover_openapi_spec_action(artifact=_oas3())
    assert r["servers"] == ["https://api.example.com/v1"]  # userinfo + query stripped
    assert "token" not in json.dumps(r["servers"])


def test_swagger2_json_string_success():
    r = discover_openapi_spec_action(artifact=json.dumps(_swagger2()))
    assert r["_success"] is True and r["version"] == "2.0"
    assert r["servers"] == ["https://api.acme.com/v2"]
    assert r["schemas"][0]["name"] == "Thing"


def test_deterministic_sorting():
    doc = {
        "openapi": "3.0.0",
        "info": {},
        "paths": {"/b": {"get": {"responses": {}}}, "/a": {"post": {"responses": {}}, "get": {"responses": {}}}},
    }
    r = discover_openapi_spec_action(artifact=doc)
    assert [(o["path"], o["method"]) for o in r["operations"]] == [
        ("/a", "GET"),
        ("/a", "POST"),
        ("/b", "GET"),
    ]


# ---------------------------------------------------------------------------
# Input-contract errors
# ---------------------------------------------------------------------------

def test_neither_source_is_invalid_input():
    assert discover_openapi_spec_action()["error_code"] == "OPENAPI_INVALID_INPUT"


def test_both_sources_is_invalid_input():
    r = discover_openapi_spec_action(spec_url="https://api.example.com/o.json", artifact=_oas3())
    assert r["error_code"] == "OPENAPI_INVALID_INPUT"


def test_non_dict_non_str_artifact_is_invalid_input():
    assert discover_openapi_spec_action(artifact=123)["error_code"] == "OPENAPI_INVALID_INPUT"


def test_malformed_json_artifact_is_parse_error():
    assert discover_openapi_spec_action(artifact='{"openapi": ')["error_code"] == "OPENAPI_PARSE_ERROR"


def test_yaml_artifact_is_unsupported_format():
    assert discover_openapi_spec_action(artifact="openapi: 3.0.0\npaths: {}")["error_code"] == "OPENAPI_UNSUPPORTED_FORMAT"


def test_missing_paths_is_invalid_spec():
    assert discover_openapi_spec_action(artifact={"openapi": "3.0.0", "info": {}})["error_code"] == "OPENAPI_INVALID_SPEC"


def test_unknown_version_is_unsupported_version():
    assert discover_openapi_spec_action(artifact={"openapi": "9.9.9", "paths": {}})["error_code"] == "OPENAPI_UNSUPPORTED_VERSION"


def test_not_an_openapi_doc_is_invalid_spec():
    assert discover_openapi_spec_action(artifact={"hello": "world"})["error_code"] == "OPENAPI_INVALID_SPEC"


# ---------------------------------------------------------------------------
# URL mode — success + fetch hardening
# ---------------------------------------------------------------------------

def test_url_mode_success_and_no_auth_no_redirects_no_proxy():
    body = json.dumps(_oas3()).encode()
    cls, client = _stream_client(200, body)
    with patch.object(sd.socket, "getaddrinfo", _public_gai), patch.object(sd.httpx, "Client", cls):
        r = discover_openapi_spec_action(spec_url="https://api.example.com/openapi.json")
    assert r["_success"] is True and r["source_mode"] == "url"
    _, ckwargs = cls.call_args
    assert ckwargs.get("follow_redirects") is False
    assert ckwargs.get("trust_env") is False
    _, skwargs = client.stream.call_args
    headers = skwargs["headers"]
    assert "Authorization" not in headers and "Cookie" not in headers
    assert "auth" not in skwargs and "cookies" not in skwargs


def test_url_mode_401_is_auth_failure_with_status():
    cls, _ = _stream_client(401, b"")
    with patch.object(sd.socket, "getaddrinfo", _public_gai), patch.object(sd.httpx, "Client", cls):
        r = discover_openapi_spec_action(spec_url="https://api.example.com/o.json")
    assert r["error_code"] == "OPENAPI_AUTH_FAILURE" and r["http_status"] == 401


def test_url_mode_403_is_auth_failure():
    cls, _ = _stream_client(403, b"")
    with patch.object(sd.socket, "getaddrinfo", _public_gai), patch.object(sd.httpx, "Client", cls):
        r = discover_openapi_spec_action(spec_url="https://api.example.com/o.json")
    assert r["error_code"] == "OPENAPI_AUTH_FAILURE"


def test_url_mode_redirect_blocked():
    cls, _ = _stream_client(302, b"")
    with patch.object(sd.socket, "getaddrinfo", _public_gai), patch.object(sd.httpx, "Client", cls):
        r = discover_openapi_spec_action(spec_url="https://api.example.com/o.json")
    assert r["error_code"] == "OPENAPI_REDIRECT_BLOCKED"


def test_url_mode_5xx_is_network_error():
    cls, _ = _stream_client(503, b"")
    with patch.object(sd.socket, "getaddrinfo", _public_gai), patch.object(sd.httpx, "Client", cls):
        r = discover_openapi_spec_action(spec_url="https://api.example.com/o.json")
    assert r["error_code"] == "OPENAPI_NETWORK_ERROR" and r["http_status"] == 503


def test_url_mode_timeout_is_unreachable():
    cls, _ = _stream_client(raise_exc=httpx.ConnectTimeout("t"))
    with patch.object(sd.socket, "getaddrinfo", _public_gai), patch.object(sd.httpx, "Client", cls):
        r = discover_openapi_spec_action(spec_url="https://api.example.com/o.json")
    assert r["error_code"] == "OPENAPI_UNREACHABLE_ENDPOINT"


def test_url_mode_transport_error_is_network_error():
    cls, _ = _stream_client(raise_exc=httpx.ReadError("boom"))
    with patch.object(sd.socket, "getaddrinfo", _public_gai), patch.object(sd.httpx, "Client", cls):
        r = discover_openapi_spec_action(spec_url="https://api.example.com/o.json")
    assert r["error_code"] == "OPENAPI_NETWORK_ERROR"


def test_url_mode_size_cap_fails_closed():
    cls, _ = _stream_client(200, b"x" * 100)
    with patch.object(sd.socket, "getaddrinfo", _public_gai), patch.object(sd.httpx, "Client", cls):
        r = discover_openapi_spec_action(spec_url="https://api.example.com/o.json", options={"max_input_chars": 10})
    assert r["error_code"] == "OPENAPI_SIZE_LIMIT_EXCEEDED"


# ---------------------------------------------------------------------------
# SSRF
# ---------------------------------------------------------------------------

def test_ssrf_literal_loopback_blocked():
    assert discover_openapi_spec_action(spec_url="http://127.0.0.1/o.json")["error_code"] == "OPENAPI_SSRF_BLOCKED"


def test_ssrf_literal_metadata_ip_blocked():
    assert discover_openapi_spec_action(spec_url="http://169.254.169.254/latest")["error_code"] == "OPENAPI_SSRF_BLOCKED"


def test_ssrf_single_label_host_blocked():
    assert discover_openapi_spec_action(spec_url="http://intranet/o.json")["error_code"] == "OPENAPI_SSRF_BLOCKED"


def test_ssrf_internal_suffix_blocked():
    assert discover_openapi_spec_action(spec_url="https://svc.internal/o.json")["error_code"] == "OPENAPI_SSRF_BLOCKED"


def test_ssrf_userinfo_blocked():
    assert discover_openapi_spec_action(spec_url="https://user:pw@api.example.com/o.json")["error_code"] == "OPENAPI_SSRF_BLOCKED"


def test_ssrf_secret_query_key_blocked():
    assert discover_openapi_spec_action(spec_url="https://api.example.com/o.json?apikey=x")["error_code"] == "OPENAPI_SSRF_BLOCKED"


def test_ssrf_non_http_scheme_blocked():
    assert discover_openapi_spec_action(spec_url="file:///etc/passwd")["error_code"] == "OPENAPI_SSRF_BLOCKED"


def test_ssrf_dns_resolves_private_blocked():
    with patch.object(sd.socket, "getaddrinfo", _private_gai):
        r = discover_openapi_spec_action(spec_url="https://sneaky.example.com/o.json")
    assert r["error_code"] == "OPENAPI_SSRF_BLOCKED"


def test_dns_failure_is_unreachable():
    import socket as _socket

    def _fail(*a, **k):
        raise _socket.gaierror("nope")

    with patch.object(sd.socket, "getaddrinfo", _fail):
        r = discover_openapi_spec_action(spec_url="https://nope.example.com/o.json")
    assert r["error_code"] == "OPENAPI_UNREACHABLE_ENDPOINT"


def test_url_mode_never_constructs_httpx_when_ssrf_blocked():
    with patch.object(sd.httpx, "Client") as m_client:
        discover_openapi_spec_action(spec_url="http://127.0.0.1/o.json")
    m_client.assert_not_called()


# ---------------------------------------------------------------------------
# Truncation
# ---------------------------------------------------------------------------

def test_node_truncation_partial_summary():
    doc = {
        "openapi": "3.0.0",
        "info": {},
        "paths": {"/a": {"get": {"responses": {}}, "post": {"responses": {}}}, "/b": {"get": {"responses": {}}}},
    }
    r = discover_openapi_spec_action(artifact=doc, options={"max_nodes": 1})
    assert r["_success"] is True
    assert r["truncated"] is True
    assert len(r["operations"]) == 1
    assert r["warnings"] and r["warnings"][0]["code"] == "TRUNCATED"
    assert r["truncation"]["reasons"]


def test_description_clip_registers_truncation():
    long_desc = "x" * 1000
    doc = {"openapi": "3.0.0", "info": {}, "paths": {"/a": {"get": {"summary": long_desc, "responses": {}}}}}
    r = discover_openapi_spec_action(artifact=doc)
    assert len(r["operations"][0]["summary"]) == 512
    assert r["truncated"] is True


# ---------------------------------------------------------------------------
# Leak-proofing
# ---------------------------------------------------------------------------

def test_external_ref_never_fetched():
    doc = {
        "openapi": "3.0.0",
        "info": {},
        "paths": {"/a": {"get": {"responses": {"200": {"description": "ok", "content": {"application/json": {"schema": {"$ref": "https://evil.example.com/schema.json#/X"}}}}}}}},
    }
    with patch.object(sd.httpx, "Client") as m_client:
        r = discover_openapi_spec_action(artifact=doc)
    m_client.assert_not_called()
    # An external ref is neither fetched NOR echoed (name suppressed to None).
    assert r["operations"][0]["responses"][0]["schema"]["ref"] is None
    assert "evil.example.com" not in json.dumps(r)


def test_error_envelope_carries_flags_and_no_leak():
    r = discover_openapi_spec_action(spec_url="https://api.example.com/o.json?token=SEKRET")
    assert r["_success"] is False
    assert r["read_only"] is True and r["boomi_mutation"] is False and r["raw_xml_exposed"] is False
    assert "SEKRET" not in json.dumps(r)


# ---------------------------------------------------------------------------
# Codex review regressions
# ---------------------------------------------------------------------------

def test_dotted_component_ref_preserved():
    """A component name containing dots must not be dot-split, or refs stop
    matching their schema (Codex P2)."""
    doc = {
        "openapi": "3.0.0",
        "info": {},
        "paths": {"/x": {"get": {"responses": {"200": {"description": "ok", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/com.example.Pet"}}}}}}}},
        "components": {"schemas": {"com.example.Pet": {"type": "object", "properties": {"id": {"type": "integer"}}}}},
    }
    r = discover_openapi_spec_action(artifact=doc)
    assert r["schemas"][0]["name"] == "com.example.Pet"
    assert r["operations"][0]["responses"][0]["schema"]["ref"] == "com.example.Pet"


def test_relative_server_url_strips_query():
    """A relative server URL must have its query stripped like absolute ones do,
    or a token leaks (Codex P2)."""
    doc = {"openapi": "3.0.0", "info": {}, "servers": [{"url": "/v1?token=SECRET"}], "paths": {}}
    r = discover_openapi_spec_action(artifact=doc)
    assert r["servers"] == ["/v1"]
    assert "SECRET" not in json.dumps(r)


def test_templated_server_port_does_not_crash():
    """OpenAPI 3 allows a variable in servers[].url; a templated PORT must not
    crash the spec — it is safely omitted (it cannot be sanitized without risking
    a userinfo-spill leak), while the spec still succeeds (Codex P2)."""
    doc = {"openapi": "3.0.0", "info": {}, "servers": [{"url": "https://api.example.com:{port}/v1"}], "paths": {}}
    r = discover_openapi_spec_action(artifact=doc)
    assert r["_success"] is True
    assert r["servers"] == []  # templated-port authority omitted (not reconstructed)


def test_host_variable_server_preserved():
    """A host/path-level '{variable}' parses as a clean authority (no invalid
    port), so it is preserved rather than omitted."""
    doc = {"openapi": "3.0.0", "info": {}, "servers": [{"url": "https://{region}.api.example.com/v1"}], "paths": {}}
    r = discover_openapi_spec_action(artifact=doc)
    assert r["_success"] is True
    assert r["servers"] == ["https://{region}.api.example.com/v1"]


def test_effective_params_operation_overrides_path_and_dedupes():
    """A path-level and operation-level parameter sharing (name, in) collapse to
    one, with the operation-level definition winning (Codex P2)."""
    doc = {"openapi": "3.0.0", "info": {}, "paths": {"/x": {
        "parameters": [{"name": "id", "in": "path", "required": True, "schema": {"type": "string"}}],
        "get": {"parameters": [{"name": "id", "in": "path", "required": True, "schema": {"type": "integer"}}], "responses": {}},
    }}}
    r = discover_openapi_spec_action(artifact=doc)
    ids = [p for p in r["operations"][0]["parameters"] if p["name"] == "id"]
    assert len(ids) == 1 and ids[0]["type"] == "integer"


def test_swagger2_path_level_body_param_in_request_schema():
    """A Swagger 2 body parameter declared at path level must still populate
    request_schema (Codex P2)."""
    doc = {"swagger": "2.0", "info": {}, "paths": {"/x": {
        "parameters": [{"name": "b", "in": "body", "schema": {"$ref": "#/definitions/T"}}],
        "post": {"responses": {}},
    }}, "definitions": {"T": {"type": "object"}}}
    r = discover_openapi_spec_action(artifact=doc)
    assert r["operations"][0]["request_schema"]["ref"] == "T"


def test_swagger2_ref_body_param_overrides_path_level():
    """An operation-level parameter $ref that resolves to the same (name, in) as a
    path-level inline body param must override it, so request_schema reflects the
    operation's override (Codex round-2 P2)."""
    doc = {
        "swagger": "2.0", "info": {},
        "parameters": {"NewBody": {"name": "b", "in": "body", "schema": {"$ref": "#/definitions/New"}}},
        "paths": {"/x": {
            "parameters": [{"name": "b", "in": "body", "schema": {"$ref": "#/definitions/Old"}}],
            "post": {"parameters": [{"$ref": "#/parameters/NewBody"}], "responses": {}},
        }},
        "definitions": {"Old": {"type": "object"}, "New": {"type": "object"}},
    }
    r = discover_openapi_spec_action(artifact=doc)
    assert r["operations"][0]["request_schema"]["ref"] == "New"


def test_external_ref_uri_not_echoed():
    """An external '$ref' URI must not have a segment echoed as a name — it could
    carry credential-like authority text (Codex round-3 P1)."""
    doc = {
        "openapi": "3.0.0", "info": {},
        "paths": {"/x": {"get": {"responses": {"200": {"description": "ok", "content": {"application/json": {"schema": {"$ref": "https://user:SEKRET@evil.example.com"}}}}}}}},
    }
    r = discover_openapi_spec_action(artifact=doc)
    assert "SEKRET" not in json.dumps(r)
    assert r["operations"][0]["responses"][0]["schema"]["ref"] is None


def test_invalid_port_authority_not_echoed():
    """A non-integer port makes the authority unparseable; the URL is omitted
    entirely, never echoed (Codex round-3 P2 / round-13 P1)."""
    doc = {"openapi": "3.0.0", "info": {}, "servers": [{"url": "https://api.example.com:SEKRET/v1"}], "paths": {}}
    r = discover_openapi_spec_action(artifact=doc)
    assert "SEKRET" not in json.dumps(r)
    assert r["servers"] == []


def test_chained_parameter_ref_resolved():
    """A component parameter that aliases another local Reference Object must be
    followed transitively so its (name, in) survive (Codex round-3 P2)."""
    doc = {
        "openapi": "3.0.0", "info": {},
        "components": {"parameters": {
            "A": {"$ref": "#/components/parameters/A0"},
            "A0": {"name": "id", "in": "query", "schema": {"type": "string"}},
        }},
        "paths": {"/x": {"get": {"parameters": [{"$ref": "#/components/parameters/A"}], "responses": {}}}},
    }
    r = discover_openapi_spec_action(artifact=doc)
    params = r["operations"][0]["parameters"]
    assert len(params) == 1 and params[0]["name"] == "id" and params[0]["in"] == "query"


def test_openapi31_anchor_ref_name_preserved():
    """OpenAPI 3.1 '$anchor' refs ('#Pet', fragment-only) are valid same-document
    references and their name must be kept, not dropped as external (Codex
    round-4 P2)."""
    doc = {
        "openapi": "3.1.0", "info": {},
        "paths": {"/x": {"get": {"responses": {"200": {"description": "ok", "content": {"application/json": {"schema": {"$ref": "#Pet"}}}}}}}},
    }
    r = discover_openapi_spec_action(artifact=doc)
    assert r["operations"][0]["responses"][0]["schema"]["ref"] == "Pet"


def test_json_pointer_array_index_resolved():
    """A parameter $ref that traverses an array index (e.g.
    '#/paths/~1shared/parameters/0') must resolve, so it overrides/dedupes
    correctly (Codex round-7 P2)."""
    doc = {
        "openapi": "3.0.0", "info": {},
        "paths": {"/shared": {
            "parameters": [{"name": "id", "in": "query", "schema": {"type": "string"}}],
            "get": {"parameters": [{"$ref": "#/paths/~1shared/parameters/0"}], "responses": {}},
        }},
    }
    r = discover_openapi_spec_action(artifact=doc)
    params = r["operations"][0]["parameters"]
    # path-level 'id' and the op-level $ref to the same array element dedupe to one
    assert len(params) == 1 and params[0]["name"] == "id" and params[0]["in"] == "query"


def test_percent_encoded_ref_token_resolved():
    """A standards-conforming percent-encoded local $ref token
    ('#/components/parameters/Foo%2DBar' -> 'Foo-Bar') must be decoded before
    lookup (Codex round-8 P2)."""
    doc = {
        "openapi": "3.0.0", "info": {},
        "components": {"parameters": {"Foo-Bar": {"name": "q", "in": "query", "schema": {"type": "string"}}}},
        "paths": {"/x": {"get": {"parameters": [{"$ref": "#/components/parameters/Foo%2DBar"}], "responses": {}}}},
    }
    r = discover_openapi_spec_action(artifact=doc)
    params = r["operations"][0]["parameters"]
    assert len(params) == 1 and params[0]["name"] == "q" and params[0]["in"] == "query"


def test_malformed_authority_userinfo_slash_suppressed():
    """An unescaped '/' in userinfo makes urlsplit push credential material into
    the path; the URL must be suppressed, not echoed (Codex round-10 P1)."""
    doc = {"openapi": "3.0.0", "info": {}, "servers": [{"url": "https://user:x/SEKRET@api.example.com/v1"}], "paths": {}}
    r = discover_openapi_spec_action(artifact=doc)
    assert "SEKRET" not in json.dumps(r)
    assert r["servers"] == []


def test_at_in_authority_path_suppressed_for_credential_safety():
    """An '@' in the path of an authority-bearing URL is ambiguous with a userinfo
    spill (which can leave a parseable prefix like 'user:443'), so it is
    suppressed — credential safety (P1) supersedes preserving a rare legitimate
    '@'-in-path endpoint (Codex round-14 P1 over round-11 P2)."""
    doc = {"openapi": "3.0.0", "info": {}, "servers": [{"url": "https://api.example.com/users/a@b"}], "paths": {}}
    r = discover_openapi_spec_action(artifact=doc)
    assert r["servers"] == []


def test_resolve_pointer_encoded_leading_slash():
    """A fragment that percent-encodes its leading '/' ('#%2F...') is still a
    valid JSON Pointer and must resolve (Codex round-12 P2)."""
    doc = {"components": {"parameters": {"Foo": {"name": "q", "in": "query"}}}}
    assert sd._resolve_pointer(doc, "#%2Fcomponents%2Fparameters%2FFoo") == {"name": "q", "in": "query"}


def test_json_pointer_name_encoded_leading_slash():
    """_json_pointer_name must decode before classifying pointer-vs-anchor, so a
    '%2F'-encoded leading slash yields the final token, not the whole path (Codex
    round-12 P2)."""
    assert sd._json_pointer_name("#%2Fcomponents%2Fschemas%2FFoo") == "Foo"


def test_sanitize_url_matrix_no_credential_spill():
    """_sanitize_url must never echo spilled userinfo, regardless of which path
    segment the spilled '@' lands in, while preserving clean authorities and
    legitimate path '@' (Codex round-13 P1 — multi-segment spill)."""
    cases = {
        # userinfo spills — ANY '@' in an authority's path is suppressed, whether
        # the truncated authority prefix is unparseable ('user:x'), a valid
        # host:port ('user:443'), or bare ('user'), and regardless of segment.
        "https://user:x/SEKRET@api.example.com/v1": None,
        "https://user:x/SEKRET/more@api.example.com/v1": None,    # multi-segment
        "https://user:443/SEKRET@api.example.com/v1": None,       # parseable prefix
        "https://user/SEKRET@api.example.com/v1": None,           # bare prefix
        # an '@' anywhere in an authority's path is suppressed for safety
        "https://api.example.com/users/a@b": None,
        # clean authority, no path '@': userinfo stripped, URL echoed
        "https://user:pw@api.example.com/x": "https://api.example.com/x",
        "https://api.example.com/v1": "https://api.example.com/v1",
        # host-level template parses cleanly -> preserved
        "https://{region}.api.example.com/v1": "https://{region}.api.example.com/v1",
        # unparseable authority (templated/invalid port) -> omitted entirely
        "https://api.example.com:{port}/v1": None,
        "https://api.example.com:SEKRET/v1": None,
        # relative reference (no authority) -> '@' is legit path data, preserved
        "/@tenant/v1": "/@tenant/v1",
    }
    for url, expected in cases.items():
        assert sd._sanitize_url(url) == expected, url
        if expected is None:
            # and the suppressed value must never appear
            assert "SEKRET" not in (sd._sanitize_url(url) or "")


def test_multi_segment_spill_not_in_summary():
    doc = {"openapi": "3.0.0", "info": {}, "servers": [{"url": "https://user:x/SEKRET/more@api.example.com/v1"}], "paths": {}}
    r = discover_openapi_spec_action(artifact=doc)
    assert "SEKRET" not in json.dumps(r)
    assert r["servers"] == []


def test_relative_at_first_segment_preserved():
    """A relative server URL whose first path segment contains '@' ('/@tenant/v1')
    has no authority, so '@' is a legitimate path char and must be preserved
    (Codex round-12 P2)."""
    doc = {"openapi": "3.0.0", "info": {}, "servers": [{"url": "/@tenant/v1"}], "paths": {}}
    r = discover_openapi_spec_action(artifact=doc)
    assert r["servers"] == ["/@tenant/v1"]


def test_json_pointer_name_percent_decoded():
    """The extracted schema-ref name must be percent-decoded to match the schema
    key (Codex round-10 P2)."""
    doc = {
        "openapi": "3.0.0", "info": {},
        "paths": {"/x": {"get": {"responses": {"200": {"description": "ok", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Foo%2DBar"}}}}}}}},
        "components": {"schemas": {"Foo-Bar": {"type": "object"}}},
    }
    r = discover_openapi_spec_action(artifact=doc)
    assert r["schemas"][0]["name"] == "Foo-Bar"
    assert r["operations"][0]["responses"][0]["schema"]["ref"] == "Foo-Bar"


def test_resolve_pointer_encoded_slash_is_separator():
    """RFC 6901: '%2F' decodes to a '/' pointer separator (whole-fragment decode
    before tokenizing), not a literal-slash token (Codex round-10 P2)."""
    doc = {"x-data": {"y": {"name": "q", "in": "query"}}}
    assert sd._resolve_pointer(doc, "#/x-data/y") == {"name": "q", "in": "query"}
    assert sd._resolve_pointer(doc, "#/x-data%2Fy") == {"name": "q", "in": "query"}


def test_cyclic_parameter_ref_does_not_hang():
    """A cyclic parameter $ref (A -> A) must be detected, not loop forever (Codex
    round-3 P2 — cycle detection)."""
    doc = {
        "openapi": "3.0.0", "info": {},
        "components": {"parameters": {"A": {"$ref": "#/components/parameters/A"}}},
        "paths": {"/x": {"get": {"parameters": [{"$ref": "#/components/parameters/A"}], "responses": {}}}},
    }
    r = discover_openapi_spec_action(artifact=doc)
    assert r["_success"] is True


def test_node_omitted_count_not_double_counted():
    """3 operations with max_nodes=1 -> exactly 2 omitted, not 4 (Codex P3)."""
    doc = {"openapi": "3.0.0", "info": {}, "paths": {"/a": {"get": {"responses": {}}, "post": {"responses": {}}}, "/b": {"get": {"responses": {}}}}}
    r = discover_openapi_spec_action(artifact=doc, options={"max_nodes": 1})
    node_reasons = [x for x in r["truncation"]["reasons"] if x["kind"] == "nodes:operations"]
    assert node_reasons and node_reasons[0]["omitted"] == 2


def test_long_scalar_clipped_and_flags_truncation():
    """An arbitrarily long emitted scalar (operationId/path) is clipped to the
    bound and flags truncation — the bounded-summary contract applies to every
    scalar, not just descriptions (§6 impl-review #2)."""
    doc = {"openapi": "3.0.0", "info": {}, "paths": {"/" + "p" * 1000: {"get": {"operationId": "o" * 1000, "responses": {}}}}}
    r = discover_openapi_spec_action(artifact=doc)
    op = r["operations"][0]
    assert len(op["operation_id"]) == 512
    assert len(op["path"]) == 512
    assert r["truncated"] is True


def test_long_ref_clipped_matches_declaration_and_flags_truncation():
    """A >512-char component ref is clipped identically to its (also-clipped)
    declaration name — bounded, still matching, AND truncation is registered
    (repo-gate: route reference clips through truncation-aware clipping)."""
    longname = "S" * 800
    doc = {
        "openapi": "3.0.0", "info": {},
        "paths": {"/x": {"get": {"responses": {"200": {"description": "ok", "content": {"application/json": {"schema": {"$ref": f"#/components/schemas/{longname}"}}}}}}}},
        "components": {"schemas": {longname: {"type": "object"}}},
    }
    r = discover_openapi_spec_action(artifact=doc)
    decl = r["schemas"][0]["name"]
    ref = r["operations"][0]["responses"][0]["schema"]["ref"]
    assert len(decl) == 512 and decl == ref
    assert r["truncated"] is True


def test_handler_never_calls_boomi_or_credentials():
    # The discovery module must never CALL a credential helper or CONSTRUCT the
    # Boomi SDK (the docstring may mention the names, so match call forms only).
    src = Path(sd.__file__).read_text()
    assert "get_secret(" not in src
    assert "get_current_user(" not in src
    assert "Boomi(" not in src
    assert "import boomi_mcp" not in src  # no dependency on account-scoped modules
