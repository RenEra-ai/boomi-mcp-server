"""Schema/spec discovery tools (Issue #13, M7) — read-only, no Boomi mutation.

Four discovery tools that turn an API/DB schema source into a bounded, structured
summary an LLM can reason over BEFORE filling archetype/profile parameters:

- ``discover_openapi_spec`` — OpenAPI/Swagger (2.0 + 3.x, JSON only).
- ``discover_soap_wsdl``    — WSDL 1.1 (SOAP 1.1 + 1.2 bindings).
- ``discover_odata_metadata`` — OData v2 + v4 EDMX ``$metadata``.
- ``discover_db_schema``     — a normalized information-schema JSON artifact.

Design invariants (every handler, every success AND error branch):

- READ-ONLY. No Boomi SDK, no ``get_secret``/``get_current_user``, no credentials,
  cookies, or caller-supplied headers are ever sent or read.
- NO MUTATION of Boomi or customer systems.
- Raw specs/XML are NEVER echoed back; only bounded summaries. Error envelopes
  carry a structured ``error_code`` and a fixed generic message — never a URL,
  response body, artifact, redirect location, or exception message.
- Every response carries ``read_only=True`` / ``boomi_mutation=False`` /
  ``raw_xml_exposed=False`` (``_FLAGS``).

Input modes:

- OpenAPI + WSDL: EXACTLY one of a fetch URL or a caller-supplied artifact.
- OData: URL-only (no ``/$metadata`` inference, no artifact).
- DB: artifact-only — it NEVER opens JDBC or any network connection.

No authentication is forwarded to external endpoints; a 401/403 becomes
``*_AUTH_FAILURE`` and the caller must download a private spec themselves and pass
it via artifact mode (OpenAPI/WSDL). External ``$ref`` / WSDL-XSD imports / OData
links are NEVER fetched. Uses only the already-declared ``httpx`` and the Python
standard library (no new dependency, no ``lxml``/``defusedxml``).
"""

from __future__ import annotations

import ipaddress
import json
import re
import socket
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, urlsplit
from xml.etree import ElementTree as ET

import httpx

# ---------------------------------------------------------------------------
# Mandatory response-contract flags
# ---------------------------------------------------------------------------

_FLAGS = {"read_only": True, "boomi_mutation": False, "raw_xml_exposed": False}

# ---------------------------------------------------------------------------
# Limits (default, hard cap). Options may raise a value up to the hard cap.
#   max_input_chars — caps URL response bytes (pre-decode) AND string-artifact
#                     chars. Pre-parsed dict artifacts rely on node/field caps.
#   max_nodes       — top-level containers (operations+schemas / XML elements /
#                     tables).
#   max_fields      — leaf members (parameters+responses+properties / ports+
#                     operations+parts / properties+navigation+sets / columns+
#                     constraints+indexes).
# ---------------------------------------------------------------------------

_MIB = 1024 * 1024
_LIMITS_API = {
    "max_input_chars": (1 * _MIB, 5 * _MIB),
    "max_nodes": (2_000, 10_000),
    "max_fields": (1_000, 5_000),
}
_LIMITS_DB = {
    "max_input_chars": (1 * _MIB, 5 * _MIB),
    "max_nodes": (1_000, 10_000),
    "max_fields": (2_000, 10_000),
}

_TEXT_CLIP = 512  # individual description/summary strings are bounded to this

# ---------------------------------------------------------------------------
# XXE / billion-laughs mitigation — reject DOCTYPE/ENTITY declarations outright
# (mirrors profile_inference._safe_fromstring; stdlib-only, no defusedxml).
# ---------------------------------------------------------------------------

_DOCTYPE_RE = re.compile(r"<!\s*(DOCTYPE|ENTITY)", re.IGNORECASE)

# ---------------------------------------------------------------------------
# SSRF controls
# ---------------------------------------------------------------------------

_ALLOWED_SCHEMES = {"http", "https"}
# Any query key whose lowercased name contains one of these is rejected so a
# secret can never be smuggled into a fetched URL (and then into logs).
_SECRET_QUERY_TOKENS = (
    "token",
    "key",
    "secret",
    "password",
    "passwd",
    "pwd",
    "signature",
    "sig",
    "credential",
    "auth",
    "access",
    "apikey",
    "api_key",
)
_BLOCKED_HOST_SUFFIXES = (".local", ".internal", ".localhost", ".home.arpa")
_BLOCKED_HOSTS = {
    "localhost",
    "metadata.google.internal",
    "metadata",
    "169.254.169.254",
}

_HTTP_TIMEOUT = httpx.Timeout(20.0, connect=5.0)
_ACCEPT_JSON = "application/json, text/json, */*"
_ACCEPT_XML = "application/xml, text/xml, */*"


# ---------------------------------------------------------------------------
# Structured error taxonomy
# ---------------------------------------------------------------------------

# API-fetch prefixes reuse a common code family via f"{prefix}_{suffix}".
_SUFFIX_MESSAGES = {
    "INVALID_INPUT": "Invalid input: supply exactly one valid source.",
    "UNSUPPORTED_FORMAT": "Unsupported source format.",
    "UNSUPPORTED_VERSION": "Unsupported spec version.",
    "PARSE_ERROR": "Source is not well-formed.",
    "INVALID_SPEC": "Source is not a supported or valid spec.",
    "AUTH_FAILURE": "Endpoint returned an authentication/authorization error.",
    "UNREACHABLE_ENDPOINT": "Endpoint was unreachable.",
    "NETWORK_ERROR": "A network error occurred while fetching the endpoint.",
    "REDIRECT_BLOCKED": "Endpoint returned a redirect, which is blocked.",
    "SSRF_BLOCKED": "The URL was blocked by SSRF protection.",
    "SIZE_LIMIT_EXCEEDED": "Source exceeds the maximum allowed size.",
    "DISCOVERY_FAILED": "Discovery failed unexpectedly.",
}


def _message_for(error_code: str) -> str:
    for suffix, msg in _SUFFIX_MESSAGES.items():
        if error_code.endswith(suffix):
            return msg
    return "Discovery failed."


class _DiscoveryError(Exception):
    """Internal control-flow signal carrying a structured, leak-free code."""

    def __init__(self, error_code: str, http_status: Optional[int] = None):
        super().__init__(error_code)
        self.error_code = error_code
        self.http_status = http_status


def _error(error_code: str, http_status: Optional[int] = None) -> Dict[str, Any]:
    env: Dict[str, Any] = {
        "_success": False,
        **_FLAGS,
        "error_code": error_code,
        "error": _message_for(error_code),
    }
    if http_status is not None:
        env["http_status"] = http_status
    return env


# ---------------------------------------------------------------------------
# Options / truncation
# ---------------------------------------------------------------------------

def _normalize_options(options: Any, limits: Dict[str, Tuple[int, int]]) -> Dict[str, int]:
    """Return clamped {max_input_chars, max_nodes, max_fields}. Accepts a dict, a
    JSON-object string, or None; garbage values fall back to the default."""
    opts: Dict[str, Any] = {}
    if isinstance(options, str):
        try:
            parsed = json.loads(options)
            if isinstance(parsed, dict):
                opts = parsed
        except (ValueError, json.JSONDecodeError):
            opts = {}
    elif isinstance(options, dict):
        opts = options

    resolved: Dict[str, int] = {}
    for key, (default, hard) in limits.items():
        value = default
        if key in opts:
            try:
                value = int(opts[key])
            except (TypeError, ValueError):
                value = default
        resolved[key] = min(max(value, 1), hard)
    return resolved


class _Truncation:
    """Accumulates truncation reasons (deduped by kind) and renders the
    truncation payload + a single bounded TRUNCATED warning."""

    def __init__(self, limits: Dict[str, int]):
        self._limits = dict(limits)
        self._by_kind: Dict[str, Dict[str, Any]] = {}

    def add(self, kind: str, limit: int, observed: int, omitted: int) -> None:
        if omitted <= 0:
            return
        entry = self._by_kind.get(kind)
        if entry is None:
            self._by_kind[kind] = {
                "kind": kind,
                "limit": limit,
                "observed": observed,
                "omitted": omitted,
            }
        else:
            entry["observed"] = max(entry["observed"], observed)
            entry["omitted"] += omitted

    @property
    def truncated(self) -> bool:
        return bool(self._by_kind)

    def payload(self) -> Optional[Dict[str, Any]]:
        if not self._by_kind:
            return None
        return {"limits": self._limits, "reasons": list(self._by_kind.values())}

    def warnings(self) -> List[Dict[str, Any]]:
        if not self._by_kind:
            return []
        reasons = list(self._by_kind.values())
        total_omitted = sum(r["omitted"] for r in reasons)
        return [
            {
                "code": "TRUNCATED",
                "message": (
                    "Summary was truncated; some elements were omitted to stay "
                    "within limits."
                ),
                "details": {"total_omitted": total_omitted, "reasons": reasons},
            }
        ]


class _Budget:
    """Node/field budgets. take_* returns True while under budget, else records a
    single truncation reason for that kind and returns False."""

    def __init__(self, trunc: _Truncation, node_limit: int, field_limit: int):
        self._trunc = trunc
        self._node_limit = node_limit
        self._field_limit = field_limit
        self._nodes = 0
        self._fields = 0

    def take_node(self, kind: str) -> bool:
        if self._nodes >= self._node_limit:
            self._trunc.add("nodes:" + kind, self._node_limit, self._nodes + 1, 1)
            return False
        self._nodes += 1
        return True

    def take_field(self, kind: str) -> bool:
        if self._fields >= self._field_limit:
            self._trunc.add("fields:" + kind, self._field_limit, self._fields + 1, 1)
            return False
        self._fields += 1
        return True


def _clip(value: Any, trunc: _Truncation) -> Optional[str]:
    """Bound an individual free-text string to _TEXT_CLIP chars; register a
    truncation reason when clipped. Non-strings pass through unchanged."""
    if not isinstance(value, str):
        return value
    if len(value) <= _TEXT_CLIP:
        return value
    trunc.add("text", _TEXT_CLIP, len(value), len(value) - _TEXT_CLIP)
    return value[:_TEXT_CLIP]


def _success(
    fmt: str,
    version: Optional[str],
    source_mode: str,
    counts: Dict[str, int],
    body: Dict[str, Any],
    trunc: _Truncation,
) -> Dict[str, Any]:
    env: Dict[str, Any] = {
        "_success": True,
        **_FLAGS,
        "source_mode": source_mode,
        "format": fmt,
        "version": version,
        "counts": counts,
        "truncated": trunc.truncated,
        "truncation": trunc.payload(),
        "warnings": trunc.warnings(),
    }
    env.update(body)
    return env


# ---------------------------------------------------------------------------
# SSRF-safe fetch
# ---------------------------------------------------------------------------

def _reject_ip(ip: Any) -> bool:
    return (
        ip.is_loopback
        or ip.is_private
        or ip.is_link_local
        or ip.is_multicast
        or ip.is_reserved
        or ip.is_unspecified
        or not ip.is_global
    )


def _validate_url(url: Any, prefix: str) -> str:
    """Validate scheme/host and reject SSRF-shaped URLs. Returns the host to
    resolve. Raises _DiscoveryError with a prefixed code."""
    if not isinstance(url, str) or not url.strip():
        raise _DiscoveryError(prefix + "_INVALID_INPUT")
    parsed = urlsplit(url.strip())
    if parsed.scheme.lower() not in _ALLOWED_SCHEMES:
        raise _DiscoveryError(prefix + "_SSRF_BLOCKED")
    if parsed.username or parsed.password:
        raise _DiscoveryError(prefix + "_SSRF_BLOCKED")
    host = parsed.hostname
    if not host:
        raise _DiscoveryError(prefix + "_INVALID_INPUT")

    for raw_key, _ in parse_qsl(parsed.query, keep_blank_values=True):
        low = raw_key.lower()
        if any(tok in low for tok in _SECRET_QUERY_TOKENS):
            raise _DiscoveryError(prefix + "_SSRF_BLOCKED")

    try:
        ipaddress.ip_address(host)
        is_ip = True
    except ValueError:
        is_ip = False

    if not is_ip:
        low = host.lower()
        if low in _BLOCKED_HOSTS:
            raise _DiscoveryError(prefix + "_SSRF_BLOCKED")
        if "." not in low:  # single-label internal host
            raise _DiscoveryError(prefix + "_SSRF_BLOCKED")
        if low.endswith(_BLOCKED_HOST_SUFFIXES):
            raise _DiscoveryError(prefix + "_SSRF_BLOCKED")
    return host


def _check_addresses(host: str, prefix: str) -> None:
    """Resolve host and reject if ANY address is non-global. A literal IP is
    checked directly. DNS failures map to *_UNREACHABLE_ENDPOINT.

    NOTE (residual DNS-rebinding TOCTOU): httpx re-resolves at connect time, so a
    hostname that resolves to a public IP here could in principle resolve to a
    private IP microseconds later. Pinning the validated IP into a custom
    transport would close this but requires more machinery than the no-new-deps
    constraint allows; resolve-then-fetch is the accepted mitigation.
    """
    try:
        ip = ipaddress.ip_address(host)
        if _reject_ip(ip):
            raise _DiscoveryError(prefix + "_SSRF_BLOCKED")
        return
    except ValueError:
        pass

    try:
        infos = socket.getaddrinfo(host, None, proto=socket.IPPROTO_TCP)
    except socket.gaierror:
        raise _DiscoveryError(prefix + "_UNREACHABLE_ENDPOINT")
    except Exception:
        raise _DiscoveryError(prefix + "_UNREACHABLE_ENDPOINT")

    if not infos:
        raise _DiscoveryError(prefix + "_UNREACHABLE_ENDPOINT")
    for info in infos:
        sockaddr = info[4]
        addr = sockaddr[0]
        try:
            ip = ipaddress.ip_address(addr.split("%", 1)[0])
        except ValueError:
            raise _DiscoveryError(prefix + "_SSRF_BLOCKED")
        if _reject_ip(ip):
            raise _DiscoveryError(prefix + "_SSRF_BLOCKED")


def _fetch(url: str, max_bytes: int, accept: str, prefix: str) -> bytes:
    """GET the URL with no auth/cookies/redirects, SSRF-validated, capping the
    body at max_bytes (fail closed). Returns raw bytes. Raises _DiscoveryError."""
    host = _validate_url(url, prefix)
    _check_addresses(host, prefix)

    headers = {"Accept": accept, "User-Agent": "boomi-mcp-discovery/1.0"}
    try:
        with httpx.Client(
            timeout=_HTTP_TIMEOUT, follow_redirects=False, trust_env=False
        ) as client:
            with client.stream("GET", url.strip(), headers=headers) as resp:
                status = getattr(resp, "status_code", None)
                if isinstance(status, int) and 300 <= status <= 399:
                    raise _DiscoveryError(prefix + "_REDIRECT_BLOCKED", http_status=status)
                if status in (401, 403):
                    raise _DiscoveryError(prefix + "_AUTH_FAILURE", http_status=status)
                if not isinstance(status, int) or not (200 <= status <= 299):
                    raise _DiscoveryError(
                        prefix + "_NETWORK_ERROR",
                        http_status=status if isinstance(status, int) else None,
                    )
                chunks: List[bytes] = []
                total = 0
                for chunk in resp.iter_bytes():
                    total += len(chunk)
                    if total > max_bytes:
                        raise _DiscoveryError(prefix + "_SIZE_LIMIT_EXCEEDED")
                    chunks.append(chunk)
                return b"".join(chunks)
    except _DiscoveryError:
        raise
    except httpx.TimeoutException:
        raise _DiscoveryError(prefix + "_UNREACHABLE_ENDPOINT")
    except httpx.ConnectError:
        raise _DiscoveryError(prefix + "_UNREACHABLE_ENDPOINT")
    except httpx.HTTPError:
        raise _DiscoveryError(prefix + "_NETWORK_ERROR")


# ---------------------------------------------------------------------------
# XXE-safe XML
# ---------------------------------------------------------------------------

def _safe_xml(data: Any, parse_code: str, invalid_spec_code: str) -> "ET.Element":
    """Parse XML (bytes or str) after rejecting DOCTYPE/ENTITY. bytes honor the
    document's own encoding declaration; str retries as UTF-8 bytes when it
    carries an encoding declaration (ElementTree rejects those on str)."""
    if isinstance(data, bytes):
        screen = data.decode("utf-8", "replace")
    elif isinstance(data, str):
        screen = data
    else:
        raise _DiscoveryError(invalid_spec_code)
    if _DOCTYPE_RE.search(screen):
        raise _DiscoveryError(invalid_spec_code)
    try:
        if isinstance(data, bytes):
            return ET.fromstring(data)
        try:
            return ET.fromstring(data)
        except ValueError:
            return ET.fromstring(data.encode("utf-8"))
    except ET.ParseError:
        raise _DiscoveryError(parse_code)


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1] if isinstance(tag, str) else tag


def _ns(tag: str) -> Optional[str]:
    if isinstance(tag, str) and tag.startswith("{"):
        return tag[1:].split("}", 1)[0]
    return None


def _iter_local(parent: "ET.Element", local: str, ns_set: Optional[set] = None):
    for child in list(parent):
        if _local(child.tag) != local:
            continue
        if ns_set is not None and _ns(child.tag) not in ns_set:
            continue
        yield child


def _last_segment(ref: Any) -> Optional[str]:
    if not isinstance(ref, str) or not ref:
        return None
    return ref.rsplit("/", 1)[-1].rsplit(".", 1)[-1] or None


def _qname_local(value: Any) -> Optional[str]:
    """Return the local part of an XML QName reference ('prefix:local' -> 'local',
    'local' -> 'local'). Used for WSDL message/element/type/binding references."""
    if not isinstance(value, str) or not value:
        return None
    return value.rsplit(":", 1)[-1] or None


def _sanitize_url(value: Any) -> Optional[str]:
    """Strip userinfo + query (which can hold credentials) from a URL string
    before echoing it in a summary; return None if unparseable."""
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parts = urlsplit(value.strip())
    except ValueError:
        return None
    if not parts.scheme and not parts.netloc:
        return value.strip()[:_TEXT_CLIP]
    host = parts.hostname or ""
    port = f":{parts.port}" if parts.port else ""
    return f"{parts.scheme}://{host}{port}{parts.path}"[:_TEXT_CLIP]


# ---------------------------------------------------------------------------
# OpenAPI parsing
# ---------------------------------------------------------------------------

_HTTP_METHODS = ("get", "put", "post", "delete", "options", "head", "patch", "trace")


def _openapi_schema_descriptor(schema: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(schema, dict):
        return None
    ref = schema.get("$ref")
    items = schema.get("items")
    items_ref = None
    items_type = None
    if isinstance(items, dict):
        items_ref = _last_segment(items.get("$ref"))
        items_type = items.get("type")
    return {
        "type": schema.get("type"),
        "format": schema.get("format"),
        "ref": _last_segment(ref),
        "items_type": items_type,
        "items_ref": items_ref,
    }


def _openapi_parameter(param: Any, trunc: _Truncation) -> Optional[Dict[str, Any]]:
    if not isinstance(param, dict):
        return None
    if "$ref" in param and len(param) == 1:
        name = _last_segment(param.get("$ref"))
        return {
            "name": name,
            "in": None,
            "required": False,
            "type": None,
            "format": None,
            "ref": name,
        }
    schema = param.get("schema") if isinstance(param.get("schema"), dict) else {}
    return {
        "name": param.get("name"),
        "in": param.get("in"),
        "required": bool(param.get("required", False)),
        "type": param.get("type") or schema.get("type"),
        "format": param.get("format") or schema.get("format"),
        "ref": _last_segment(schema.get("$ref")) if schema else None,
    }


def _openapi_request_schema(op: Dict[str, Any], version_major: int) -> Optional[Dict[str, Any]]:
    if version_major == 3:
        body = op.get("requestBody")
        if not isinstance(body, dict):
            return None
        content = body.get("content")
        if not isinstance(content, dict):
            return None
        for _mtype, media in content.items():
            if isinstance(media, dict) and isinstance(media.get("schema"), dict):
                return _openapi_schema_descriptor(media["schema"])
        return None
    # v2: a body parameter carries the schema
    for param in op.get("parameters", []) or []:
        if isinstance(param, dict) and param.get("in") == "body" and isinstance(
            param.get("schema"), dict
        ):
            return _openapi_schema_descriptor(param["schema"])
    return None


def _parse_openapi(doc: Any, limits: Dict[str, int], trunc: _Truncation):
    if not isinstance(doc, dict):
        raise _DiscoveryError("OPENAPI_INVALID_SPEC")

    swagger = doc.get("swagger")
    openapi = doc.get("openapi")
    if isinstance(openapi, str) and openapi.startswith("3."):
        version = openapi
        version_major = 3
    elif swagger == "2.0" or openapi == "2.0":
        version = "2.0"
        version_major = 2
    elif openapi is not None or swagger is not None:
        raise _DiscoveryError("OPENAPI_UNSUPPORTED_VERSION")
    else:
        raise _DiscoveryError("OPENAPI_INVALID_SPEC")

    paths = doc.get("paths")
    if not isinstance(paths, dict):
        raise _DiscoveryError("OPENAPI_INVALID_SPEC")

    info = doc.get("info") if isinstance(doc.get("info"), dict) else {}
    title = _clip(info.get("title"), trunc)

    servers: List[str] = []
    if version_major == 3 and isinstance(doc.get("servers"), list):
        for srv in doc["servers"]:
            if isinstance(srv, dict):
                url = _sanitize_url(srv.get("url"))
                if url:
                    servers.append(url)
    elif version_major == 2:
        host = doc.get("host")
        base_path = doc.get("basePath") or ""
        schemes = doc.get("schemes") if isinstance(doc.get("schemes"), list) else ["https"]
        if isinstance(host, str) and host:
            for scheme in schemes:
                if isinstance(scheme, str):
                    servers.append(_sanitize_url(f"{scheme}://{host}{base_path}"))
    servers = [s for s in servers if s]

    budget = _Budget(trunc, limits["max_nodes"], limits["max_fields"])

    operations: List[Dict[str, Any]] = []
    omitted_ops = 0
    for path in sorted(k for k in paths.keys() if isinstance(k, str)):
        item = paths.get(path)
        if not isinstance(item, dict):
            continue
        shared_params = item.get("parameters") if isinstance(item.get("parameters"), list) else []
        for method in _HTTP_METHODS:
            op = item.get(method)
            if not isinstance(op, dict):
                continue
            if not budget.take_node("operations"):
                omitted_ops += 1
                continue

            params_out: List[Dict[str, Any]] = []
            raw_params = list(shared_params) + (
                op.get("parameters") if isinstance(op.get("parameters"), list) else []
            )
            for p in raw_params:
                if not budget.take_field("parameters"):
                    break
                norm = _openapi_parameter(p, trunc)
                if norm is not None:
                    params_out.append(norm)

            responses_out: List[Dict[str, Any]] = []
            responses = op.get("responses") if isinstance(op.get("responses"), dict) else {}
            for status in sorted(str(s) for s in responses.keys()):
                if not budget.take_field("responses"):
                    break
                resp = responses.get(status) if isinstance(responses.get(status), dict) else {}
                schema_desc = None
                if version_major == 3:
                    content = resp.get("content")
                    if isinstance(content, dict):
                        for _mt, media in content.items():
                            if isinstance(media, dict) and isinstance(media.get("schema"), dict):
                                schema_desc = _openapi_schema_descriptor(media["schema"])
                                break
                else:
                    if isinstance(resp.get("schema"), dict):
                        schema_desc = _openapi_schema_descriptor(resp["schema"])
                responses_out.append(
                    {
                        "status_code": status,
                        "description": _clip(resp.get("description"), trunc),
                        "schema": schema_desc,
                    }
                )

            operations.append(
                {
                    "path": path,
                    "method": method.upper(),
                    "operation_id": op.get("operationId"),
                    "summary": _clip(op.get("summary"), trunc),
                    "parameters": params_out,
                    "request_schema": _openapi_request_schema(op, version_major),
                    "responses": responses_out,
                }
            )
    if omitted_ops:
        trunc.add("nodes:operations", limits["max_nodes"], budget._nodes + omitted_ops, omitted_ops)

    # Schemas / definitions
    if version_major == 3:
        components = doc.get("components") if isinstance(doc.get("components"), dict) else {}
        raw_schemas = components.get("schemas") if isinstance(components.get("schemas"), dict) else {}
    else:
        raw_schemas = doc.get("definitions") if isinstance(doc.get("definitions"), dict) else {}

    schemas_out: List[Dict[str, Any]] = []
    omitted_schemas = 0
    for name in sorted(k for k in raw_schemas.keys() if isinstance(k, str)):
        schema = raw_schemas.get(name)
        if not isinstance(schema, dict):
            continue
        if not budget.take_node("schemas"):
            omitted_schemas += 1
            continue
        required = schema.get("required") if isinstance(schema.get("required"), list) else []
        required_set = {r for r in required if isinstance(r, str)}
        props_out: List[Dict[str, Any]] = []
        props = schema.get("properties") if isinstance(schema.get("properties"), dict) else {}
        for pname in sorted(k for k in props.keys() if isinstance(k, str)):
            if not budget.take_field("properties"):
                break
            pschema = props.get(pname) if isinstance(props.get(pname), dict) else {}
            desc = _openapi_schema_descriptor(pschema) or {}
            props_out.append(
                {
                    "name": pname,
                    "type": desc.get("type"),
                    "format": desc.get("format"),
                    "required": pname in required_set,
                    "ref": desc.get("ref"),
                    "items_type": desc.get("items_type"),
                    "items_ref": desc.get("items_ref"),
                }
            )
        schemas_out.append(
            {
                "name": name,
                "type": schema.get("type"),
                "required_fields": sorted(required_set),
                "properties": props_out,
            }
        )
    if omitted_schemas:
        trunc.add("nodes:schemas", limits["max_nodes"], budget._nodes + omitted_schemas, omitted_schemas)

    operations.sort(key=lambda o: (o["path"], o["method"]))
    body = {
        "title": title,
        "servers": servers,
        "operations": operations,
        "schemas": schemas_out,
    }
    counts = {"operations": len(operations), "schemas": len(schemas_out)}
    return {"version": version, "counts": counts, "body": body}


def _decode_json_source(raw: bytes, prefix: str) -> Any:
    text = raw.decode("utf-8", "replace")
    return _loads_openapi_text(text, prefix)


def _loads_openapi_text(text: str, prefix: str) -> Any:
    try:
        return json.loads(text)
    except (ValueError, json.JSONDecodeError):
        stripped = text.lstrip()
        # Distinguish a YAML/other spec (unsupported) from malformed JSON.
        if not stripped.startswith("{") and not stripped.startswith("["):
            raise _DiscoveryError(prefix + "_UNSUPPORTED_FORMAT")
        raise _DiscoveryError(prefix + "_PARSE_ERROR")


# ---------------------------------------------------------------------------
# WSDL parsing
# ---------------------------------------------------------------------------

_WSDL_NS = {"http://schemas.xmlsoap.org/wsdl/"}
_SOAP11_NS = "http://schemas.xmlsoap.org/wsdl/soap/"
_SOAP12_NS = "http://schemas.xmlsoap.org/wsdl/soap12/"
_SOAP_BINDING_NS = {_SOAP11_NS, _SOAP12_NS}


def _soap_version_of(ns: Optional[str]) -> Optional[str]:
    if ns == _SOAP11_NS:
        return "1.1"
    if ns == _SOAP12_NS:
        return "1.2"
    return None


def _parse_wsdl(root: "ET.Element", limits: Dict[str, int], trunc: _Truncation):
    if _local(root.tag) != "definitions" or _ns(root.tag) not in _WSDL_NS:
        raise _DiscoveryError("WSDL_INVALID_SPEC")

    target_namespace = root.get("targetNamespace")
    budget = _Budget(trunc, limits["max_nodes"], limits["max_fields"])

    # portType operations: name -> {input, output, faults[]}
    port_type_ops: Dict[str, Dict[str, Any]] = {}
    for port_type in _iter_local(root, "portType", _WSDL_NS):
        for op in _iter_local(port_type, "operation", _WSDL_NS):
            op_name = op.get("name")
            if not op_name:
                continue
            input_msg = None
            output_msg = None
            faults: List[str] = []
            for child in list(op):
                lname = _local(child.tag)
                if lname == "input":
                    input_msg = _qname_local(child.get("message"))
                elif lname == "output":
                    output_msg = _qname_local(child.get("message"))
                elif lname == "fault":
                    fm = _qname_local(child.get("message")) or child.get("name")
                    if fm:
                        faults.append(fm)
            port_type_ops[op_name] = {
                "input": input_msg,
                "output": output_msg,
                "faults": faults,
            }

    # bindings
    bindings_out: List[Dict[str, Any]] = []
    binding_soap_version: Dict[str, Optional[str]] = {}
    for binding in _iter_local(root, "binding", _WSDL_NS):
        if not budget.take_node("bindings"):
            break
        bname = binding.get("name")
        port_type = _qname_local(binding.get("type"))
        soap_version = None
        style = None
        transport = None
        for child in list(binding):
            if _local(child.tag) == "binding" and _ns(child.tag) in _SOAP_BINDING_NS:
                soap_version = _soap_version_of(_ns(child.tag))
                style = child.get("style")
                transport = child.get("transport")
                break
        binding_soap_version[bname] = soap_version

        ops_out: List[Dict[str, Any]] = []
        for op in _iter_local(binding, "operation", _WSDL_NS):
            if not budget.take_field("operations"):
                break
            op_name = op.get("name")
            soap_action = None
            for child in list(op):
                if _local(child.tag) == "operation" and _ns(child.tag) in _SOAP_BINDING_NS:
                    soap_action = child.get("soapAction")
                    break
            pt = port_type_ops.get(op_name, {})
            ops_out.append(
                {
                    "name": op_name,
                    "soap_action": soap_action,
                    "input_message": pt.get("input"),
                    "output_message": pt.get("output"),
                    "fault_messages": list(pt.get("faults", [])),
                }
            )
        bindings_out.append(
            {
                "name": bname,
                "port_type": port_type,
                "soap_version": soap_version,
                "style": style,
                "transport": transport,
                "operations": ops_out,
            }
        )

    # services / ports
    services_out: List[Dict[str, Any]] = []
    for service in _iter_local(root, "service", _WSDL_NS):
        ports_out: List[Dict[str, Any]] = []
        for port in _iter_local(service, "port", _WSDL_NS):
            if not budget.take_field("ports"):
                break
            binding_ref = _qname_local(port.get("binding"))
            address = None
            soap_version = None
            for child in list(port):
                if _local(child.tag) == "address" and _ns(child.tag) in _SOAP_BINDING_NS:
                    address = _sanitize_url(child.get("location"))
                    soap_version = _soap_version_of(_ns(child.tag))
                    break
            if soap_version is None:
                soap_version = binding_soap_version.get(binding_ref)
            ports_out.append(
                {
                    "name": port.get("name"),
                    "binding": binding_ref,
                    "address": address,
                    "soap_version": soap_version,
                }
            )
        services_out.append({"name": service.get("name"), "ports": ports_out})

    # messages
    messages_out: List[Dict[str, Any]] = []
    for message in _iter_local(root, "message", _WSDL_NS):
        parts_out: List[Dict[str, Any]] = []
        for part in _iter_local(message, "part", _WSDL_NS):
            if not budget.take_field("message_parts"):
                break
            parts_out.append(
                {
                    "name": part.get("name"),
                    "element": _qname_local(part.get("element")),
                    "type": _qname_local(part.get("type")),
                }
            )
        messages_out.append({"name": message.get("name"), "parts": parts_out})

    # imports (reported, never fetched)
    imports_out: List[Dict[str, Any]] = []
    for imp in _iter_local(root, "import", _WSDL_NS):
        imports_out.append(
            {
                "namespace": imp.get("namespace"),
                "location": _sanitize_url(imp.get("location")),
                "fetched": False,
            }
        )

    body = {
        "target_namespace": target_namespace,
        "services": services_out,
        "bindings": bindings_out,
        "messages": messages_out,
        "imports": imports_out,
    }
    counts = {
        "services": len(services_out),
        "bindings": len(bindings_out),
        "operations": sum(len(b["operations"]) for b in bindings_out),
        "messages": len(messages_out),
    }
    return {"version": "1.1", "counts": counts, "body": body}


# ---------------------------------------------------------------------------
# OData EDMX parsing
# ---------------------------------------------------------------------------

_EDMX_V4_NS = {"http://docs.oasis-open.org/odata/ns/edmx"}
_EDMX_LEGACY_NS = {"http://schemas.microsoft.com/ado/2007/06/edmx"}
_EDM_V4_NS = {"http://docs.oasis-open.org/odata/ns/edm"}
_EDM_V2_NS = {"http://schemas.microsoft.com/ado/2008/09/edm"}


def _find_edm_schemas(root: "ET.Element") -> List["ET.Element"]:
    schemas: List["ET.Element"] = []
    for ds in _iter_local(root, "DataServices"):
        for schema in _iter_local(ds, "Schema"):
            schemas.append(schema)
    if not schemas:  # some documents omit DataServices wrapper defensively
        for schema in root.iter():
            if _local(schema.tag) == "Schema":
                schemas.append(schema)
    return schemas


def _odata_data_service_version(root: "ET.Element") -> Optional[str]:
    """Return the OData PROTOCOL version signalled on ``edmx:DataServices`` —
    ``MaxDataServiceVersion`` preferred over ``DataServiceVersion``. These are
    namespaced under the dataservices/metadata (``m:``) namespace, so match by
    the local attribute name. This is the authoritative discriminator between
    OData v2 and v3, which SHARE the CSDL 3.0 (``2009/11/edm``) schema namespace
    and the same ``2007/06/edmx`` wrapper — the schema namespace alone cannot
    tell them apart."""
    for ds in _iter_local(root, "DataServices"):
        max_ver = None
        data_ver = None
        for key, val in ds.attrib.items():
            local = _local(key)
            if local == "MaxDataServiceVersion":
                max_ver = val
            elif local == "DataServiceVersion":
                data_ver = val
        return max_ver or data_ver
    return None


def _detect_odata_version(root: "ET.Element", schemas: List["ET.Element"]) -> Optional[str]:
    edmx_ns = _ns(root.tag)
    version_attr = root.get("Version") or ""
    schema_ns = _ns(schemas[0].tag) if schemas else None
    if edmx_ns in _EDMX_V4_NS or version_attr.startswith("4") or schema_ns in _EDM_V4_NS:
        return "4.0"
    # Legacy (v1-v3) wrappers: the authoritative protocol signal is the
    # (Max)DataServiceVersion attribute, NOT the CSDL schema namespace — real v2
    # services (e.g. services.odata.org/V2/Northwind) use CSDL 3.0
    # (``2009/11/edm``) with MaxDataServiceVersion="2.0", which the schema
    # namespace alone would misclassify. v3 (MaxDataServiceVersion="3.0") stays
    # unsupported per the declared v2+v4 scope.
    ds_version = _odata_data_service_version(root)
    if ds_version:
        if ds_version.startswith("2"):
            return "2.0"
        # v1 (unsupported) or v3+ (out of scope) -> reject as invalid spec.
        return None
    # No protocol-version attribute present: fall back to legacy structural hints.
    if (edmx_ns in _EDMX_LEGACY_NS and version_attr.startswith("2")) or version_attr == "2.0" or schema_ns in _EDM_V2_NS:
        return "2.0"
    return None


def _odata_property(prop: "ET.Element") -> Dict[str, Any]:
    nullable_attr = prop.get("Nullable")
    return {
        "name": prop.get("Name"),
        "type": prop.get("Type"),
        "nullable": True if nullable_attr is None else nullable_attr.lower() == "true",
        "max_length": prop.get("MaxLength"),
        "precision": _to_int(prop.get("Precision")),
        "scale": prop.get("Scale"),
    }


def _to_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_odata(root: "ET.Element", limits: Dict[str, int], trunc: _Truncation):
    if _local(root.tag) != "Edmx":
        raise _DiscoveryError("ODATA_INVALID_SPEC")
    schemas = _find_edm_schemas(root)
    version = _detect_odata_version(root, schemas)
    if version is None:
        raise _DiscoveryError("ODATA_INVALID_SPEC")

    budget = _Budget(trunc, limits["max_nodes"], limits["max_fields"])

    # v2 associations: name -> {role_name: type}
    associations: Dict[str, Dict[str, str]] = {}
    if version == "2.0":
        for schema in schemas:
            sns = schema.get("Namespace") or ""
            for assoc in _iter_local(schema, "Association"):
                aname = assoc.get("Name")
                roles: Dict[str, str] = {}
                for end in _iter_local(assoc, "End"):
                    role = end.get("Role")
                    etype = end.get("Type")
                    if role and etype:
                        roles[role] = etype
                if aname:
                    associations[aname] = roles
                    associations[f"{sns}.{aname}"] = roles

    entity_types: List[Dict[str, Any]] = []
    schema_names: List[str] = []
    for schema in schemas:
        sns = schema.get("Namespace") or ""
        if sns and sns not in schema_names:
            schema_names.append(sns)
        for etype in _iter_local(schema, "EntityType"):
            if not budget.take_node("entity_types"):
                break
            keys: List[str] = []
            for key in _iter_local(etype, "Key"):
                for ref in _iter_local(key, "PropertyRef"):
                    if ref.get("Name"):
                        keys.append(ref.get("Name"))
            props: List[Dict[str, Any]] = []
            for prop in _iter_local(etype, "Property"):
                if not budget.take_field("properties"):
                    break
                props.append(_odata_property(prop))
            navs: List[Dict[str, Any]] = []
            for nav in _iter_local(etype, "NavigationProperty"):
                if not budget.take_field("navigation_properties"):
                    break
                navs.append(_odata_navigation(nav, version, associations))
            entity_types.append(
                {
                    "namespace": sns,
                    "name": etype.get("Name"),
                    "base_type": etype.get("BaseType"),
                    "keys": keys,
                    "properties": props,
                    "navigation_properties": navs,
                }
            )

    entity_sets: List[Dict[str, Any]] = []
    for schema in schemas:
        for container in _iter_local(schema, "EntityContainer"):
            cname = container.get("Name")
            for eset in _iter_local(container, "EntitySet"):
                if not budget.take_field("entity_sets"):
                    break
                bindings: List[Dict[str, Any]] = []
                for nb in _iter_local(eset, "NavigationPropertyBinding"):
                    bindings.append({"path": nb.get("Path"), "target": nb.get("Target")})
                entity_sets.append(
                    {
                        "container": cname,
                        "name": eset.get("Name"),
                        "entity_type": eset.get("EntityType"),
                        "navigation_bindings": bindings,
                    }
                )

    body = {
        "schemas": schema_names,
        "entity_types": entity_types,
        "entity_sets": entity_sets,
    }
    counts = {
        "entity_types": len(entity_types),
        "entity_sets": len(entity_sets),
    }
    return {"version": version, "counts": counts, "body": body}


def _odata_navigation(
    nav: "ET.Element", version: str, associations: Dict[str, Dict[str, str]]
) -> Dict[str, Any]:
    name = nav.get("Name")
    if version == "4.0":
        type_attr = nav.get("Type") or ""
        collection = type_attr.startswith("Collection(")
        target = type_attr
        if collection:
            target = type_attr[len("Collection(") : -1] if type_attr.endswith(")") else type_attr
        nullable_attr = nav.get("Nullable")
        return {
            "name": name,
            "target_type": target or None,
            "collection": collection,
            "nullable": None if nullable_attr is None else nullable_attr.lower() == "true",
            "partner": nav.get("Partner"),
            "relationship": None,
        }
    # v2
    relationship = nav.get("Relationship")
    to_role = nav.get("ToRole")
    target_type = None
    roles = associations.get(relationship) or associations.get(_last_segment(relationship) or "")
    if roles and to_role and to_role in roles:
        target_type = roles[to_role]
    return {
        "name": name,
        "target_type": target_type,
        "collection": None,
        "nullable": None,
        "partner": None,
        "relationship": _last_segment(relationship),
    }


# ---------------------------------------------------------------------------
# DB information-schema parsing
# ---------------------------------------------------------------------------

def _normalize_nullable(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        low = value.strip().lower()
        if low == "yes":
            return True
        if low == "no":
            return False
    return None


def _table_key(catalog: Any, schema: Any, name: Any) -> Tuple[Any, Any, Any]:
    return (catalog, schema, name)


def _parse_db_schema(doc: Any, limits: Dict[str, int], trunc: _Truncation):
    if not isinstance(doc, dict):
        raise _DiscoveryError("DB_SCHEMA_INVALID_INPUT")
    columns = doc.get("columns")
    if not isinstance(columns, list) or not columns:
        raise _DiscoveryError("DB_SCHEMA_INVALID_SPEC")

    budget = _Budget(trunc, limits["max_nodes"], limits["max_fields"])

    # Build table registry (declared tables first, then derived from columns).
    tables: Dict[Tuple[Any, Any, Any], Dict[str, Any]] = {}
    order: List[Tuple[Any, Any, Any]] = []

    def _ensure(catalog, schema, name, ttype=None):
        key = _table_key(catalog, schema, name)
        if key not in tables:
            tables[key] = {
                "catalog": catalog,
                "schema": schema,
                "name": name,
                "type": ttype,
                "columns": [],
                "constraints": [],
                "indexes": [],
            }
            order.append(key)
        elif ttype and not tables[key]["type"]:
            tables[key]["type"] = ttype
        return tables[key]

    declared = doc.get("tables")
    if isinstance(declared, list):
        for t in declared:
            if isinstance(t, dict) and t.get("table_name"):
                _ensure(
                    t.get("table_catalog"),
                    t.get("table_schema"),
                    t.get("table_name"),
                    t.get("table_type"),
                )

    for col in columns:
        if not isinstance(col, dict) or not col.get("table_name"):
            continue
        if not budget.take_field("columns"):
            break
        table = _ensure(col.get("table_catalog"), col.get("table_schema"), col.get("table_name"))
        table["columns"].append(
            {
                "name": col.get("column_name"),
                "ordinal_position": _to_int(col.get("ordinal_position")),
                "data_type": col.get("data_type"),
                "nullable": _normalize_nullable(col.get("is_nullable")),
                # NEVER echo the actual default value (may hold secrets/PII).
                "default_present": col.get("column_default") is not None,
                "max_length": _to_int(col.get("character_maximum_length")),
                "precision": _to_int(col.get("numeric_precision")),
                "scale": _to_int(col.get("numeric_scale")),
            }
        )

    constraints = doc.get("constraints")
    if isinstance(constraints, list):
        for c in constraints:
            if not isinstance(c, dict) or not c.get("table_name"):
                continue
            if not budget.take_field("constraints"):
                break
            table = _ensure(None, c.get("table_schema"), c.get("table_name"))
            table["constraints"].append(
                {
                    "name": c.get("constraint_name"),
                    "type": c.get("constraint_type"),
                    "columns": [x for x in (c.get("columns") or []) if isinstance(x, str)],
                    "referenced_schema": c.get("referenced_table_schema"),
                    "referenced_table": c.get("referenced_table_name"),
                    "referenced_columns": [
                        x for x in (c.get("referenced_columns") or []) if isinstance(x, str)
                    ],
                }
            )

    indexes = doc.get("indexes")
    if isinstance(indexes, list):
        for ix in indexes:
            if not isinstance(ix, dict) or not ix.get("table_name"):
                continue
            if not budget.take_field("indexes"):
                break
            table = _ensure(None, ix.get("table_schema"), ix.get("table_name"))
            table["indexes"].append(
                {
                    "name": ix.get("index_name"),
                    "unique": bool(ix.get("unique")),
                    "columns": [x for x in (ix.get("columns") or []) if isinstance(x, str)],
                }
            )

    # Node cap on tables (deterministic ordering by schema then name).
    ordered = sorted(
        (tables[k] for k in order),
        key=lambda t: (str(t["schema"] or ""), str(t["name"] or "")),
    )
    tables_out: List[Dict[str, Any]] = []
    omitted_tables = 0
    for t in ordered:
        if not budget.take_node("tables"):
            omitted_tables += 1
            continue
        t["columns"].sort(
            key=lambda c: (
                c["ordinal_position"] if c["ordinal_position"] is not None else 1 << 30,
                str(c["name"] or ""),
            )
        )
        tables_out.append(t)
    if omitted_tables:
        trunc.add("nodes:tables", limits["max_nodes"], len(ordered), omitted_tables)

    body = {
        "database_product": doc.get("database_product"),
        "catalog": doc.get("catalog"),
        "tables": tables_out,
    }
    counts = {
        "tables": len(tables_out),
        "columns": sum(len(t["columns"]) for t in tables_out),
    }
    return {"version": None, "counts": counts, "body": body}


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------

def _require_one_source(url: Any, artifact: Any, prefix: str) -> str:
    """Return 'url' or 'artifact' for an exactly-one-of contract."""
    has_url = isinstance(url, str) and url.strip() != ""
    has_artifact = artifact is not None
    if has_url == has_artifact:  # neither or both
        raise _DiscoveryError(prefix + "_INVALID_INPUT")
    return "url" if has_url else "artifact"


def discover_openapi_spec_action(
    spec_url: Optional[str] = None,
    artifact: Any = None,
    options: Any = None,
) -> Dict[str, Any]:
    """Discover a bounded summary of an OpenAPI/Swagger spec (JSON only)."""
    try:
        limits = _normalize_options(options, _LIMITS_API)
        trunc = _Truncation(limits)
        mode = _require_one_source(spec_url, artifact, "OPENAPI")
        if mode == "url":
            raw = _fetch(spec_url, limits["max_input_chars"], _ACCEPT_JSON, "OPENAPI")
            doc = _decode_json_source(raw, "OPENAPI")
        else:
            if isinstance(artifact, str):
                if len(artifact) > limits["max_input_chars"]:
                    raise _DiscoveryError("OPENAPI_SIZE_LIMIT_EXCEEDED")
                doc = _loads_openapi_text(artifact, "OPENAPI")
            elif isinstance(artifact, dict):
                doc = artifact
            else:
                raise _DiscoveryError("OPENAPI_INVALID_INPUT")
        result = _parse_openapi(doc, limits, trunc)
        return _success("openapi", result["version"], mode, result["counts"], result["body"], trunc)
    except _DiscoveryError as e:
        return _error(e.error_code, e.http_status)
    except Exception:
        return _error("OPENAPI_DISCOVERY_FAILED")


def discover_soap_wsdl_action(
    wsdl_url: Optional[str] = None,
    artifact: Any = None,
    options: Any = None,
) -> Dict[str, Any]:
    """Discover a bounded summary of a WSDL 1.1 document (SOAP 1.1/1.2)."""
    try:
        limits = _normalize_options(options, _LIMITS_API)
        trunc = _Truncation(limits)
        mode = _require_one_source(wsdl_url, artifact, "WSDL")
        if mode == "url":
            raw = _fetch(wsdl_url, limits["max_input_chars"], _ACCEPT_XML, "WSDL")
            root = _safe_xml(raw, "WSDL_PARSE_ERROR", "WSDL_INVALID_SPEC")
        else:
            if not isinstance(artifact, str):
                raise _DiscoveryError("WSDL_INVALID_INPUT")
            if len(artifact) > limits["max_input_chars"]:
                raise _DiscoveryError("WSDL_SIZE_LIMIT_EXCEEDED")
            root = _safe_xml(artifact, "WSDL_PARSE_ERROR", "WSDL_INVALID_SPEC")
        result = _parse_wsdl(root, limits, trunc)
        return _success("wsdl", result["version"], mode, result["counts"], result["body"], trunc)
    except _DiscoveryError as e:
        return _error(e.error_code, e.http_status)
    except Exception:
        return _error("WSDL_DISCOVERY_FAILED")


def discover_odata_metadata_action(
    metadata_url: str,
    options: Any = None,
) -> Dict[str, Any]:
    """Discover a bounded summary of an OData v2/v4 EDMX ``$metadata`` document.

    URL-only: fetches the EXACT metadata_url (no ``/$metadata`` inference)."""
    try:
        limits = _normalize_options(options, _LIMITS_API)
        trunc = _Truncation(limits)
        if not isinstance(metadata_url, str) or not metadata_url.strip():
            raise _DiscoveryError("ODATA_INVALID_INPUT")
        raw = _fetch(metadata_url, limits["max_input_chars"], _ACCEPT_XML, "ODATA")
        root = _safe_xml(raw, "ODATA_PARSE_ERROR", "ODATA_INVALID_SPEC")
        result = _parse_odata(root, limits, trunc)
        return _success(
            "odata_metadata", result["version"], "url", result["counts"], result["body"], trunc
        )
    except _DiscoveryError as e:
        return _error(e.error_code, e.http_status)
    except Exception:
        return _error("ODATA_DISCOVERY_FAILED")


def discover_db_schema_action(
    artifact: Any,
    options: Any = None,
) -> Dict[str, Any]:
    """Discover a bounded relational-topology summary from a normalized
    information-schema JSON artifact. Artifact-only: NEVER opens JDBC/network."""
    try:
        limits = _normalize_options(options, _LIMITS_DB)
        trunc = _Truncation(limits)
        if isinstance(artifact, str):
            if len(artifact) > limits["max_input_chars"]:
                raise _DiscoveryError("DB_SCHEMA_SIZE_LIMIT_EXCEEDED")
            try:
                doc = json.loads(artifact)
            except (ValueError, json.JSONDecodeError):
                raise _DiscoveryError("DB_SCHEMA_PARSE_ERROR")
        elif isinstance(artifact, dict):
            doc = artifact
        else:
            raise _DiscoveryError("DB_SCHEMA_INVALID_INPUT")
        result = _parse_db_schema(doc, limits, trunc)
        return _success(
            "information_schema_json",
            result["version"],
            "artifact",
            result["counts"],
            result["body"],
            trunc,
        )
    except _DiscoveryError as e:
        return _error(e.error_code, e.http_status)
    except Exception:
        return _error("DB_SCHEMA_DISCOVERY_FAILED")


__all__ = [
    "discover_openapi_spec_action",
    "discover_soap_wsdl_action",
    "discover_odata_metadata_action",
    "discover_db_schema_action",
]
