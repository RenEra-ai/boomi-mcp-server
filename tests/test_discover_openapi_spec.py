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
    # only the trailing ref name is kept, never the URL
    assert r["operations"][0]["responses"][0]["schema"]["ref"] == "X"
    assert "evil.example.com" not in json.dumps(r)


def test_error_envelope_carries_flags_and_no_leak():
    r = discover_openapi_spec_action(spec_url="https://api.example.com/o.json?token=SEKRET")
    assert r["_success"] is False
    assert r["read_only"] is True and r["boomi_mutation"] is False and r["raw_xml_exposed"] is False
    assert "SEKRET" not in json.dumps(r)


def test_handler_never_calls_boomi_or_credentials():
    # The discovery module must never CALL a credential helper or CONSTRUCT the
    # Boomi SDK (the docstring may mention the names, so match call forms only).
    src = Path(sd.__file__).read_text()
    assert "get_secret(" not in src
    assert "get_current_user(" not in src
    assert "Boomi(" not in src
    assert "import boomi_mcp" not in src  # no dependency on account-scoped modules
