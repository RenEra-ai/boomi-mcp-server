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
from urllib.parse import parse_qsl, unquote, urlsplit
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

_TEXT_CLIP = 512  # individual description/summary/identifier strings bounded to this
_LIST_CAP = 1000  # max elements emitted per nested identifier list

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
    """Global node/field budgets shared across a parse. ``take_*`` returns True
    while under budget (and consumes one slot), else counts one omission for that
    kind and returns False. All truncation accounting flows through ``finalize``
    ONCE at the end — a single path, so omissions are never double-counted. To
    get accurate omitted counts, callers should keep iterating (``continue``) and
    calling ``take_*`` for every candidate rather than ``break``-ing early."""

    def __init__(self, node_limit: int, field_limit: int):
        self._node_limit = node_limit
        self._field_limit = field_limit
        self._nodes = 0
        self._fields = 0
        self._seen: Dict[str, int] = {}
        self._omitted: Dict[str, int] = {}

    def take_node(self, kind: str) -> bool:
        key = "nodes:" + kind
        self._seen[key] = self._seen.get(key, 0) + 1
        if self._nodes >= self._node_limit:
            self._omitted[key] = self._omitted.get(key, 0) + 1
            return False
        self._nodes += 1
        return True

    def take_field(self, kind: str) -> bool:
        key = "fields:" + kind
        self._seen[key] = self._seen.get(key, 0) + 1
        if self._fields >= self._field_limit:
            self._omitted[key] = self._omitted.get(key, 0) + 1
            return False
        self._fields += 1
        return True

    def finalize(self, trunc: "_Truncation") -> None:
        for key, omitted in self._omitted.items():
            if omitted <= 0:
                continue
            limit = self._node_limit if key.startswith("nodes:") else self._field_limit
            trunc.add(key, limit, self._seen.get(key, omitted), omitted)


def _clip(value: Any, trunc: _Truncation) -> Optional[str]:
    """Bound an individual emitted scalar. None passes through; a genuine string
    is length-capped to _TEXT_CLIP (registering truncation when clipped). ANY
    OTHER type (a list/dict/number smuggled into a string field by a malformed
    caller artifact) is DROPPED to None — never echoed unbounded — so a single
    artifact field can never inflate or corrupt the bounded string summary."""
    if value is None:
        return None
    if not isinstance(value, str):
        return None
    if len(value) <= _TEXT_CLIP:
        return value
    trunc.add("text", _TEXT_CLIP, len(value), len(value) - _TEXT_CLIP)
    return value[:_TEXT_CLIP]


def _clip_list(items: Any, trunc: _Truncation, kind: str) -> List[str]:
    """Bound a nested list of identifiers to _LIST_CAP string elements (each also
    clipped), registering truncation when either the count or an element is
    clipped — so one artifact list (e.g. a constraint's columns) can't inflate
    the bounded summary."""
    if not isinstance(items, list):
        return []
    out: List[str] = []
    for x in items:
        if not isinstance(x, str):
            continue
        if len(out) >= _LIST_CAP:
            trunc.add("list:" + kind, _LIST_CAP, len(items), len(items) - _LIST_CAP)
            break
        out.append(_clip(x, trunc))
    return out


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
        # Encoding-robust DOCTYPE screen: decoding as UTF-8 would leave NUL bytes
        # interleaved through the '<!DOCTYPE' of a UTF-16/UTF-32 document, so the
        # regex would MISS the declaration and ET.fromstring (which honors the
        # document's own encoding) could then expand internal entities. latin-1
        # maps every byte 1:1 (never raises), and stripping NULs collapses the
        # UTF-16/32 padding so an ASCII '<!DOCTYPE'/'<!ENTITY' is always exposed.
        screen = data.decode("latin-1").replace("\x00", "")
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
    """Last dotted/slashed segment of a namespace-qualified name (e.g. OData
    'NS.Order_Customer' -> 'Order_Customer'). Do NOT use for OpenAPI JSON Pointer
    refs, whose component names may legitimately contain dots — see
    ``_json_pointer_name``."""
    if not isinstance(ref, str) or not ref:
        return None
    # Return the FULL normalized name (no length cap): callers use it for
    # lookups/matching and clip the EMITTED copy via truncation-aware _clip.
    return ref.rsplit("/", 1)[-1].rsplit(".", 1)[-1] or None


def _json_pointer_name(ref: Any) -> Optional[str]:
    """Name of a LOCAL OpenAPI '$ref', or None for an external one. A ref is
    local only when the WHOLE value is a fragment (starts with '#'):
      - JSON Pointer '#/components/schemas/com.example.Pet' -> 'com.example.Pet'
        (no dot-splitting, so it matches the exact dotted component name);
      - fragment-only anchor '#Pet' (OpenAPI 3.1 / JSON Schema '$anchor') -> 'Pet'.
    An EXTERNAL / relative ref (e.g. 'https://user:SEKRET@host/x' or 'Pet.json#/X')
    returns None: the tool summarizes only same-document refs, and echoing a
    segment of an external URI could leak credential-like authority text."""
    if not isinstance(ref, str) or not ref.startswith("#"):
        return None
    # Percent-decode the WHOLE fragment first (RFC 6901 §6) — this must happen
    # BEFORE classifying pointer-vs-anchor, since a valid fragment may encode its
    # leading '/' as '%2F' (e.g. '#%2Fcomponents%2Fschemas%2FFoo'). Then take the
    # final pointer token (or the anchor) and JSON-Pointer-unescape it, so
    # '.../Foo%2DBar' -> 'Foo-Bar' matches the schema registered under that name.
    fragment = unquote(ref[1:])
    if fragment.startswith("/"):
        raw = fragment.rsplit("/", 1)[-1]
    else:
        raw = fragment  # fragment-only anchor
    name = raw.replace("~1", "/").replace("~0", "~")
    if not name:
        return None
    # Return the FULL name (no length cap) so it still matches its declaration in
    # lookups; the EMITTED ref is clipped via truncation-aware _clip at each site.
    return name


def _qname_local(value: Any) -> Optional[str]:
    """Return the local part of an XML QName reference ('prefix:local' -> 'local',
    'local' -> 'local'). Used for WSDL message/element/type/binding references.
    Leading/trailing whitespace is stripped first: XML Schema's QName datatype
    collapses it, but ElementTree preserves the lexical value, so a schema-valid
    'tns:Pt ' would otherwise miss its portType lookup."""
    if not isinstance(value, str):
        return None
    value = value.strip()
    if not value:
        return None
    local = value.rsplit(":", 1)[-1]
    if not local:
        return None
    # Return the FULL local part (no length cap): it is used as a lookup key
    # (portType/binding resolution), so capping here would collide distinct
    # long names. Emitted copies are clipped via truncation-aware _clip.
    return local


def _sanitize_url(value: Any) -> Optional[str]:
    """Strip userinfo + query + fragment (any of which can hold credentials) from
    a URL string before echoing it in a summary; return None if unparseable.
    Handles relative references (path only, no query) and templated OpenAPI
    authorities (e.g. 'https://host:{port}/v1', whose port is not an int)."""
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parts = urlsplit(value.strip())
    except ValueError:
        return None
    if not parts.scheme and not parts.netloc:
        # Relative reference: no authority was parsed, so an '@' in the path is a
        # legitimate RFC 3986 path character (e.g. '/@tenant/v1'). Emit the path
        # only — never the query/fragment, which could carry '/v1?token=SECRET'.
        # The caller clips this (full) value at emit via truncation-aware _clip.
        return parts.path or None
    # An authority WAS parsed. Treat ANY '@' in the path as a possible userinfo
    # spill and suppress the URL. An unescaped '/' in 'user:pw' truncates the
    # authority — sometimes to a still-PARSEABLE prefix like 'user:443' or 'user'
    # — and pushes the real 'SEKRET@realhost' into the path; echoing the
    # reconstructed path would then leak the credential. A genuine spill is
    # indistinguishable from a legitimate path '@' (e.g. '/users/a@b') from
    # urlsplit's output alone, so the tool's hard no-credential-echo invariant
    # (a P1 concern) requires suppressing the ambiguous case. The rare cost is
    # dropping a legitimate '@'-in-path endpoint from the summary.
    if "@" in parts.path:
        return None
    try:
        port = f":{parts.port}" if parts.port else ""
        authority = f"{parts.hostname or ''}{port}"
    except ValueError:
        # Unparseable/'{port}'-templated authority: omit the endpoint that cannot
        # be safely sanitized (host/path '{variable}' templates parse cleanly
        # above and are kept).
        return None
    # Return the FULL sanitized URL; the caller clips it at emit via _clip.
    return f"{parts.scheme}://{authority}{parts.path}" or None


def _sanitize_ns(value: Any) -> Optional[str]:
    """Sanitize a namespace/identifier URI-reference before echoing it. A
    WSDL/OData namespace can be any URI-reference that embeds credentials — an
    absolute URL, a scheme-relative/network-path reference ('//user:pw@host/x'),
    or one with a secret query — so parse it with urlsplit and, WHENEVER an
    authority is present, strip userinfo + query + fragment. A URN/opaque
    identifier (no authority) just has its query/fragment tail dropped. The
    caller clips the (full) sanitized value at emit."""
    if not isinstance(value, str) or not value.strip():
        return None
    v = value.strip()
    try:
        parts = urlsplit(v)
    except ValueError:
        return None
    if parts.netloc:
        try:
            host = parts.hostname or ""
            port = f":{parts.port}" if parts.port else ""
        except ValueError:
            return None
        if ":" in host:  # IPv6 literal — restore brackets
            host = f"[{host}]"
        scheme = f"{parts.scheme}:" if parts.scheme else ""
        return f"{scheme}//{host}{port}{parts.path}" or None
    return v.split("?", 1)[0].split("#", 1)[0] or None


# ---------------------------------------------------------------------------
# OpenAPI parsing
# ---------------------------------------------------------------------------

_HTTP_METHODS = ("get", "put", "post", "delete", "options", "head", "patch", "trace")


def _openapi_schema_descriptor(schema: Any, trunc: _Truncation) -> Optional[Dict[str, Any]]:
    if not isinstance(schema, dict):
        return None
    ref = schema.get("$ref")
    items = schema.get("items")
    items_ref = None
    items_type = None
    if isinstance(items, dict):
        items_ref = _clip(_json_pointer_name(items.get("$ref")), trunc)
        items_type = _clip(items.get("type"), trunc)
    return {
        "type": _clip(schema.get("type"), trunc),
        "format": _clip(schema.get("format"), trunc),
        "ref": _clip(_json_pointer_name(ref), trunc),
        "items_type": items_type,
        "items_ref": items_ref,
    }


def _openapi_parameter(param: Any, trunc: _Truncation) -> Optional[Dict[str, Any]]:
    if not isinstance(param, dict):
        return None
    if "$ref" in param and len(param) == 1:
        name = _clip(_json_pointer_name(param.get("$ref")), trunc)
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
        "name": _clip(param.get("name"), trunc),
        "in": _clip(param.get("in"), trunc),
        "required": bool(param.get("required", False)),
        "type": _clip(param.get("type") or schema.get("type"), trunc),
        "format": _clip(param.get("format") or schema.get("format"), trunc),
        "ref": _clip(_json_pointer_name(schema.get("$ref")), trunc) if schema else None,
    }


def _resolve_pointer(doc: Any, ref: Any) -> Optional[Dict[str, Any]]:
    """Resolve ONE same-document JSON Pointer '$ref' (e.g. '#/parameters/Foo' or
    '#/components/parameters/Foo') to its target dict, or None. Never follows an
    external ref."""
    if not isinstance(ref, str) or not ref.startswith("#") or not isinstance(doc, dict):
        return None
    # Per RFC 6901 §6, percent-decode the WHOLE fragment BEFORE classifying and
    # tokenizing: an encoded leading slash '%2F...' still denotes a pointer, an
    # encoded separator '%2F' becomes a '/' delimiter, and an encoded token char
    # like '%2D' becomes part of the token. THEN split and apply '~1'/'~0'.
    pointer = unquote(ref[1:])  # drop leading '#'
    if not pointer.startswith("/"):
        return None  # anchor / empty fragment — not a JSON Pointer
    node: Any = doc
    for raw in pointer[1:].split("/"):
        token = raw.replace("~1", "/").replace("~0", "~")
        if isinstance(node, dict) and token in node:
            node = node[token]
        elif isinstance(node, list):
            try:
                idx = int(token)
            except (TypeError, ValueError):
                return None
            if idx < 0 or idx >= len(node):
                return None
            node = node[idx]
        else:
            return None
    return node if isinstance(node, dict) else None


def _resolve_ref_chain(doc: Any, ref: Any, max_hops: int = 10) -> Optional[Dict[str, Any]]:
    """Follow a chain of local '$ref' aliases (A -> A0 -> ...) to the final
    non-ref target dict, with cycle detection and a hop cap. Returns None if any
    hop is external/unresolvable or a cycle is detected."""
    seen: set = set()
    current: Any = ref
    for _ in range(max_hops):
        if not isinstance(current, str) or current in seen:
            return None
        seen.add(current)
        target = _resolve_pointer(doc, current)
        if not isinstance(target, dict):
            return None
        nxt = target.get("$ref")
        if isinstance(nxt, str):
            current = nxt
            continue
        return target
    return None


def _openapi_effective_parameters(
    shared_params: Any, op_params: Any, doc: Any
) -> List[Dict[str, Any]]:
    """Merge path-level and operation-level parameters per the OpenAPI rule: an
    operation-level parameter OVERRIDES a shared one with the same (name, in);
    a same-target duplicate is not emitted twice. A same-document parameter
    '$ref' is resolved to its target so it keys (and de-dupes / overrides) by the
    resolved (name, in) — a ref and an inline param for the same target collapse
    correctly. Unresolvable/external refs key on the ref string. Order is stable
    (path-level first, then operation-level extras)."""
    shared = shared_params if isinstance(shared_params, list) else []
    ops = op_params if isinstance(op_params, list) else []
    order: List[Any] = []
    by_key: Dict[Any, Dict[str, Any]] = {}
    for source in (shared, ops):
        for p in source:
            if not isinstance(p, dict):
                continue
            entry = p
            if "$ref" in p:
                target = _resolve_ref_chain(doc, p.get("$ref"))
                if isinstance(target, dict):
                    entry = target
            if entry is p and "$ref" in p:  # unresolved / external ref
                key: Any = ("$ref", p.get("$ref"))
            else:
                key = (entry.get("name"), entry.get("in"))
            if key not in by_key:
                order.append(key)
            by_key[key] = entry  # later (operation-level) wins
    return [by_key[k] for k in order]


def _openapi_request_schema(
    op: Dict[str, Any],
    effective_params: List[Dict[str, Any]],
    version_major: int,
    trunc: _Truncation,
) -> Optional[Dict[str, Any]]:
    if version_major == 3:
        body = op.get("requestBody")
        if not isinstance(body, dict):
            return None
        content = body.get("content")
        if not isinstance(content, dict):
            return None
        for _mtype, media in content.items():
            if isinstance(media, dict) and isinstance(media.get("schema"), dict):
                return _openapi_schema_descriptor(media["schema"], trunc)
        return None
    # v2: a body parameter carries the schema — check the EFFECTIVE (merged)
    # parameters so a path-level `in: body` is not missed.
    for param in effective_params:
        if isinstance(param, dict) and param.get("in") == "body" and isinstance(
            param.get("schema"), dict
        ):
            return _openapi_schema_descriptor(param["schema"], trunc)
    return None


def _parse_openapi(doc: Any, limits: Dict[str, int], trunc: _Truncation):
    if not isinstance(doc, dict):
        raise _DiscoveryError("OPENAPI_INVALID_SPEC")

    swagger = doc.get("swagger")
    openapi = doc.get("openapi")
    if isinstance(openapi, str) and openapi.startswith("3."):
        version = _clip(openapi, trunc)  # bound the emitted version string
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

    budget = _Budget(limits["max_nodes"], limits["max_fields"])

    # servers[] is charged against the shared field budget (and each URL clipped
    # at emit), so a caller artifact with thousands of servers cannot defeat the
    # bounded-summary contract.
    servers: List[str] = []
    if version_major == 3 and isinstance(doc.get("servers"), list):
        for srv in doc["servers"]:
            if not isinstance(srv, dict):
                continue
            if not budget.take_field("servers"):
                continue
            url = _clip(_sanitize_url(srv.get("url")), trunc)
            if url:
                servers.append(url)
    elif version_major == 2:
        host = doc.get("host")
        base_path = doc.get("basePath") or ""
        schemes = doc.get("schemes") if isinstance(doc.get("schemes"), list) else ["https"]
        if isinstance(host, str) and host:
            for scheme in schemes:
                if not isinstance(scheme, str):
                    continue
                if not budget.take_field("servers"):
                    continue
                url = _clip(_sanitize_url(f"{scheme}://{host}{base_path}"), trunc)
                if url:
                    servers.append(url)

    operations: List[Dict[str, Any]] = []
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
                continue

            effective_params = _openapi_effective_parameters(shared_params, op.get("parameters"), doc)
            params_out: List[Dict[str, Any]] = []
            for p in effective_params:
                if not budget.take_field("parameters"):
                    continue
                norm = _openapi_parameter(p, trunc)
                if norm is not None:
                    params_out.append(norm)

            responses_out: List[Dict[str, Any]] = []
            responses = op.get("responses") if isinstance(op.get("responses"), dict) else {}
            for status in sorted(str(s) for s in responses.keys()):
                if not budget.take_field("responses"):
                    continue
                resp = responses.get(status) if isinstance(responses.get(status), dict) else {}
                schema_desc = None
                if version_major == 3:
                    content = resp.get("content")
                    if isinstance(content, dict):
                        for _mt, media in content.items():
                            if isinstance(media, dict) and isinstance(media.get("schema"), dict):
                                schema_desc = _openapi_schema_descriptor(media["schema"], trunc)
                                break
                else:
                    if isinstance(resp.get("schema"), dict):
                        schema_desc = _openapi_schema_descriptor(resp["schema"], trunc)
                responses_out.append(
                    {
                        "status_code": _clip(status, trunc),
                        "description": _clip(resp.get("description"), trunc),
                        "schema": schema_desc,
                    }
                )

            operations.append(
                {
                    "path": _clip(path, trunc),
                    "method": method.upper(),
                    "operation_id": _clip(op.get("operationId"), trunc),
                    "summary": _clip(op.get("summary"), trunc),
                    "parameters": params_out,
                    "request_schema": _openapi_request_schema(op, effective_params, version_major, trunc),
                    "responses": responses_out,
                }
            )

    # Schemas / definitions
    if version_major == 3:
        components = doc.get("components") if isinstance(doc.get("components"), dict) else {}
        raw_schemas = components.get("schemas") if isinstance(components.get("schemas"), dict) else {}
    else:
        raw_schemas = doc.get("definitions") if isinstance(doc.get("definitions"), dict) else {}

    schemas_out: List[Dict[str, Any]] = []
    for name in sorted(k for k in raw_schemas.keys() if isinstance(k, str)):
        schema = raw_schemas.get(name)
        if not isinstance(schema, dict):
            continue
        if not budget.take_node("schemas"):
            continue
        required = schema.get("required") if isinstance(schema.get("required"), list) else []
        required_set = {r for r in required if isinstance(r, str)}
        props_out: List[Dict[str, Any]] = []
        props = schema.get("properties") if isinstance(schema.get("properties"), dict) else {}
        for pname in sorted(k for k in props.keys() if isinstance(k, str)):
            if not budget.take_field("properties"):
                continue
            pschema = props.get(pname) if isinstance(props.get(pname), dict) else {}
            desc = _openapi_schema_descriptor(pschema, trunc) or {}
            props_out.append(
                {
                    "name": _clip(pname, trunc),
                    "type": _clip(desc.get("type"), trunc),
                    "format": _clip(desc.get("format"), trunc),
                    "required": pname in required_set,
                    "ref": desc.get("ref"),
                    "items_type": _clip(desc.get("items_type"), trunc),
                    "items_ref": desc.get("items_ref"),
                }
            )
        schemas_out.append(
            {
                "name": _clip(name, trunc),
                "type": _clip(schema.get("type"), trunc),
                "required_fields": _clip_list(sorted(required_set), trunc, "required_fields"),
                "properties": props_out,
            }
        )

    budget.finalize(trunc)
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

    target_namespace = _clip(root.get("targetNamespace"), trunc)
    budget = _Budget(limits["max_nodes"], limits["max_fields"])

    # Abstract portType operations, keyed by (portType name, operation name) so
    # two portTypes that define the same operation name never collide. Both key
    # components are whitespace-normalized via _qname_local — the reference side
    # (binding/@type) and the declaration side (portType/@name, operation/@name)
    # must normalize identically (NCName/QName whitespace facet is 'collapse'),
    # or a schema-valid ' Pt ' declaration would miss its 'tns:Pt' reference.
    port_type_ops: Dict[Tuple[Optional[str], Optional[str]], Dict[str, Any]] = {}
    for port_type in _iter_local(root, "portType", _WSDL_NS):
        pt_name = _qname_local(port_type.get("name"))
        for op in _iter_local(port_type, "operation", _WSDL_NS):
            op_name = _qname_local(op.get("name"))
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
            port_type_ops[(pt_name, op_name)] = {
                "input": input_msg,
                "output": output_msg,
                "faults": faults,
            }

    # bindings
    bindings_out: List[Dict[str, Any]] = []
    binding_soap_version: Dict[Optional[str], Optional[str]] = {}
    for binding in _iter_local(root, "binding", _WSDL_NS):
        if not budget.take_node("bindings"):
            continue
        # Normalize declaration NCNames identically to the QName references that
        # look them up (whitespace facet is 'collapse'), so ports[].binding
        # matches bindings[].name and the soap-version fallback resolves.
        bname = _qname_local(binding.get("name"))
        port_type = _qname_local(binding.get("type"))
        soap_version = None
        style = None
        transport = None
        for child in list(binding):
            if _local(child.tag) == "binding" and _ns(child.tag) in _SOAP_BINDING_NS:
                soap_version = _soap_version_of(_ns(child.tag))
                style = _clip(child.get("style"), trunc)
                transport = _clip(child.get("transport"), trunc)
                break
        binding_soap_version[bname] = soap_version

        ops_out: List[Dict[str, Any]] = []
        for op in _iter_local(binding, "operation", _WSDL_NS):
            if not budget.take_field("operations"):
                continue
            op_name = _qname_local(op.get("name"))  # normalize to match the key
            soap_action = None
            for child in list(op):
                if _local(child.tag) == "operation" and _ns(child.tag) in _SOAP_BINDING_NS:
                    soap_action = _clip(child.get("soapAction"), trunc)
                    break
            # Resolve the abstract operation within THIS binding's port type
            # using the FULL (uncapped) key; emit clipped copies.
            pt = port_type_ops.get((port_type, op_name), {})
            ops_out.append(
                {
                    "name": _clip(op_name, trunc),
                    "soap_action": soap_action,
                    "input_message": _clip(pt.get("input"), trunc),
                    "output_message": _clip(pt.get("output"), trunc),
                    "fault_messages": _clip_list(pt.get("faults", []), trunc, "fault_messages"),
                }
            )
        bindings_out.append(
            {
                "name": _clip(bname, trunc),
                "port_type": _clip(port_type, trunc),
                "soap_version": soap_version,
                "style": style,
                "transport": transport,
                "operations": ops_out,
            }
        )

    # services / ports
    services_out: List[Dict[str, Any]] = []
    for service in _iter_local(root, "service", _WSDL_NS):
        if not budget.take_node("services"):
            continue
        ports_out: List[Dict[str, Any]] = []
        for port in _iter_local(service, "port", _WSDL_NS):
            if not budget.take_field("ports"):
                continue
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
                    "name": _clip(_qname_local(port.get("name")), trunc),
                    "binding": _clip(binding_ref, trunc),  # full binding_ref used for lookup above
                    "address": _clip(address, trunc),
                    "soap_version": soap_version,
                }
            )
        services_out.append(
            {"name": _clip(_qname_local(service.get("name")), trunc), "ports": ports_out}
        )

    # messages
    messages_out: List[Dict[str, Any]] = []
    for message in _iter_local(root, "message", _WSDL_NS):
        if not budget.take_node("messages"):
            continue
        parts_out: List[Dict[str, Any]] = []
        for part in _iter_local(message, "part", _WSDL_NS):
            if not budget.take_field("message_parts"):
                continue
            parts_out.append(
                {
                    "name": _clip(_qname_local(part.get("name")), trunc),
                    "element": _clip(_qname_local(part.get("element")), trunc),
                    "type": _clip(_qname_local(part.get("type")), trunc),
                }
            )
        messages_out.append(
            {"name": _clip(_qname_local(message.get("name")), trunc), "parts": parts_out}
        )

    # imports (reported, never fetched)
    imports_out: List[Dict[str, Any]] = []
    for imp in _iter_local(root, "import", _WSDL_NS):
        if not budget.take_node("imports"):
            continue
        imports_out.append(
            {
                "namespace": _clip(_sanitize_ns(imp.get("namespace")), trunc),
                "location": _clip(_sanitize_url(imp.get("location")), trunc),
                "fetched": False,
            }
        )

    budget.finalize(trunc)
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


def _odata_property(prop: "ET.Element", trunc: _Truncation) -> Dict[str, Any]:
    nullable_attr = prop.get("Nullable")
    return {
        "name": _clip(prop.get("Name"), trunc),
        "type": _clip(prop.get("Type"), trunc),
        "nullable": True if nullable_attr is None else nullable_attr.lower() == "true",
        "max_length": _clip(prop.get("MaxLength"), trunc),
        "precision": _to_int(prop.get("Precision")),
        "scale": _clip(prop.get("Scale"), trunc),
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

    budget = _Budget(limits["max_nodes"], limits["max_fields"])

    # v2 associations, keyed by FULLY-QUALIFIED name (Namespace.Name AND, when
    # the schema declares one, Alias.Name). A bare short name is tracked
    # separately and only used when UNAMBIGUOUS across schemas, so a navigation
    # that references an association via an alias resolves to the right one and a
    # duplicated short name never silently reads the wrong association.
    associations: Dict[str, Dict[str, Dict[str, Any]]] = {}
    short_assoc: Dict[str, Optional[Dict[str, Dict[str, Any]]]] = {}
    if version == "2.0":
        # Collect CSDL <Using Namespace=".." Alias=".."> mappings (alias -> ns).
        # A '{using-alias}.Name' reference is equivalent to '{namespace}.Name'.
        using_aliases: Dict[str, str] = {}
        for schema in schemas:
            for using in _iter_local(schema, "Using"):
                u_alias = using.get("Alias")
                u_ns = using.get("Namespace")
                if u_alias and u_ns:
                    using_aliases[u_alias] = u_ns
        for schema in schemas:
            sns = schema.get("Namespace") or ""
            alias = schema.get("Alias")
            for assoc in _iter_local(schema, "Association"):
                aname = assoc.get("Name")
                roles: Dict[str, Dict[str, Any]] = {}
                for end in _iter_local(assoc, "End"):
                    role = end.get("Role")
                    etype = end.get("Type")
                    if role and etype:
                        roles[role] = {"type": etype, "multiplicity": end.get("Multiplicity")}
                if aname:
                    if sns:
                        associations[f"{sns}.{aname}"] = roles
                    if alias:
                        associations[f"{alias}.{aname}"] = roles
                    # index under any <Using> alias that maps to this namespace
                    for u_alias, u_ns in using_aliases.items():
                        if u_ns == sns:
                            associations[f"{u_alias}.{aname}"] = roles
                    if aname in short_assoc and short_assoc[aname] is not roles:
                        short_assoc[aname] = None  # ambiguous -> leave unresolved
                    else:
                        short_assoc[aname] = roles

    entity_types: List[Dict[str, Any]] = []
    schema_names: List[str] = []
    for schema in schemas:
        sns = schema.get("Namespace") or ""
        if sns and sns not in schema_names:
            schema_names.append(sns)
        for etype in _iter_local(schema, "EntityType"):
            if not budget.take_node("entity_types"):
                continue
            keys: List[str] = []
            for key in _iter_local(etype, "Key"):
                for ref in _iter_local(key, "PropertyRef"):
                    if ref.get("Name"):
                        keys.append(ref.get("Name"))
            props: List[Dict[str, Any]] = []
            for prop in _iter_local(etype, "Property"):
                if not budget.take_field("properties"):
                    continue
                props.append(_odata_property(prop, trunc))
            navs: List[Dict[str, Any]] = []
            for nav in _iter_local(etype, "NavigationProperty"):
                if not budget.take_field("navigation_properties"):
                    continue
                navs.append(_odata_navigation(nav, version, associations, short_assoc, trunc))
            entity_types.append(
                {
                    "namespace": _clip(sns, trunc),
                    "name": _clip(etype.get("Name"), trunc),
                    "base_type": _clip(etype.get("BaseType"), trunc),
                    "keys": _clip_list(keys, trunc, "keys"),
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
                    continue
                bindings: List[Dict[str, Any]] = []
                for nb in _iter_local(eset, "NavigationPropertyBinding"):
                    if not budget.take_field("navigation_bindings"):
                        continue
                    bindings.append(
                        {"path": _clip(nb.get("Path"), trunc), "target": _clip(nb.get("Target"), trunc)}
                    )
                entity_sets.append(
                    {
                        "container": _clip(cname, trunc),
                        "name": _clip(eset.get("Name"), trunc),
                        "entity_type": _clip(eset.get("EntityType"), trunc),
                        "navigation_bindings": bindings,
                    }
                )

    budget.finalize(trunc)
    body = {
        "schemas": _clip_list(schema_names, trunc, "schemas"),
        "entity_types": entity_types,
        "entity_sets": entity_sets,
    }
    counts = {
        "entity_types": len(entity_types),
        "entity_sets": len(entity_sets),
    }
    return {"version": version, "counts": counts, "body": body}


def _odata_navigation(
    nav: "ET.Element",
    version: str,
    associations: Dict[str, Dict[str, Dict[str, Any]]],
    short_assoc: Dict[str, Optional[Dict[str, Dict[str, Any]]]],
    trunc: _Truncation,
) -> Dict[str, Any]:
    name = _clip(nav.get("Name"), trunc)
    if version == "4.0":
        type_attr = nav.get("Type") or ""
        collection = type_attr.startswith("Collection(")
        target = type_attr
        if collection:
            target = type_attr[len("Collection(") : -1] if type_attr.endswith(")") else type_attr
        nullable_attr = nav.get("Nullable")
        return {
            "name": name,
            "target_type": _clip(target or None, trunc),
            "collection": collection,
            "nullable": None if nullable_attr is None else nullable_attr.lower() == "true",
            "partner": _clip(nav.get("Partner"), trunc),
            "relationship": None,
        }
    # v2: resolve the target role's type AND multiplicity from the Association, so
    # `collection` is a boolean ('*' -> many) per the response contract.
    relationship = nav.get("Relationship")
    to_role = nav.get("ToRole")
    target_type = None
    collection: Optional[bool] = None
    # Prefer the fully-qualified (Namespace/Alias) key; fall back to the bare
    # short name ONLY when it is unambiguous (short_assoc holds None otherwise).
    roles = associations.get(relationship)
    if roles is None:
        roles = short_assoc.get(_last_segment(relationship) or "")
    if roles and to_role and to_role in roles:
        target_type = roles[to_role].get("type")
        mult = roles[to_role].get("multiplicity")
        if mult is not None:
            collection = mult.strip() == "*"
    return {
        "name": name,
        "target_type": _clip(target_type, trunc),
        "collection": collection,
        "nullable": None,
        "partner": None,
        "relationship": _clip(_last_segment(relationship), trunc),
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

    # Fail closed on structurally invalid column records: every column MUST be an
    # object carrying the required non-empty string fields (table_name,
    # column_name, data_type); the qualifier fields used to key/echo tables
    # (table_schema, table_catalog) MUST be a string or absent (never a
    # list/dict/number, which would corrupt the table key or the summary). A
    # malformed artifact (e.g. {"columns":[{}]}) is INVALID_SPEC, not a success
    # with nulls or a generic DISCOVERY_FAILED crash.
    def _bad_required(v):
        return not isinstance(v, str) or not v

    def _bad_optional_str(v):
        return v is not None and not isinstance(v, str)

    for col in columns:
        if (
            not isinstance(col, dict)
            or _bad_required(col.get("table_name"))
            or _bad_required(col.get("column_name"))
            or _bad_required(col.get("data_type"))
            or _bad_optional_str(col.get("table_schema"))
            or _bad_optional_str(col.get("table_catalog"))
        ):
            raise _DiscoveryError("DB_SCHEMA_INVALID_SPEC")

    budget = _Budget(limits["max_nodes"], limits["max_fields"])
    top_catalog = doc.get("catalog")

    # Build table registry (declared tables first, then derived from columns).
    tables: Dict[Tuple[Any, Any, Any], Dict[str, Any]] = {}
    order: List[Tuple[Any, Any, Any]] = []
    # O(1) index: (schema, name) -> list of full keys, for catalog-less lookups.
    by_schema_name: Dict[Tuple[Any, Any], List[Tuple[Any, Any, Any]]] = {}

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
            by_schema_name.setdefault((schema, name), []).append(key)
        elif ttype and not tables[key]["type"]:
            tables[key]["type"] = ttype
        return tables[key]

    def _locate(schema, name):
        """Attach constraints/indexes (which carry no catalog) to the right
        table without splitting a catalog-qualified DB into duplicates. Prefer a
        COLUMN-BEARING candidate (so a catalog-less columns[] table wins over an
        empty catalog-qualified tables[] declaration of the same (schema, name)),
        then within that pool the exact top-level-catalog match, then a unique
        candidate; else create under the top-level catalog. O(1) via the
        (schema, name) index."""
        candidates = [tables[k] for k in by_schema_name.get((schema, name), [])]
        if not candidates:
            return _ensure(top_catalog, schema, name)
        with_cols = [t for t in candidates if t["columns"]]
        pool = with_cols if with_cols else candidates
        for t in pool:
            if t["catalog"] == top_catalog:
                return t
        if len(pool) == 1:
            return pool[0]
        return _ensure(top_catalog, schema, name)

    declared = doc.get("tables")
    if isinstance(declared, list):
        for t in declared:
            if not isinstance(t, dict) or not isinstance(t.get("table_name"), str) or not t.get("table_name"):
                continue
            # Skip a declared table whose key fields are the wrong type (a
            # list/dict schema/catalog would be an unhashable table key).
            if _bad_optional_str(t.get("table_schema")) or _bad_optional_str(t.get("table_catalog")):
                continue
            _ensure(
                t.get("table_catalog"),
                t.get("table_schema"),
                t.get("table_name"),
                t.get("table_type"),
            )

    for col in columns:
        if not budget.take_field("columns"):
            continue
        table = _ensure(col.get("table_catalog"), col.get("table_schema"), col.get("table_name"))
        table["columns"].append(
            {
                "name": _clip(col.get("column_name"), trunc),
                "ordinal_position": _to_int(col.get("ordinal_position")),
                "data_type": _clip(col.get("data_type"), trunc),
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
            if _bad_optional_str(c.get("table_schema")):
                continue  # unhashable/invalid lookup key
            if not budget.take_field("constraints"):
                continue
            table = _locate(c.get("table_schema"), c.get("table_name"))
            table["constraints"].append(
                {
                    "name": _clip(c.get("constraint_name"), trunc),
                    "type": _clip(c.get("constraint_type"), trunc),
                    "columns": _clip_list(c.get("columns"), trunc, "constraint_columns"),
                    "referenced_schema": _clip(c.get("referenced_table_schema"), trunc),
                    "referenced_table": _clip(c.get("referenced_table_name"), trunc),
                    "referenced_columns": _clip_list(
                        c.get("referenced_columns"), trunc, "referenced_columns"
                    ),
                }
            )

    indexes = doc.get("indexes")
    if isinstance(indexes, list):
        for ix in indexes:
            if not isinstance(ix, dict) or not ix.get("table_name"):
                continue
            if _bad_optional_str(ix.get("table_schema")):
                continue  # unhashable/invalid lookup key
            if not budget.take_field("indexes"):
                continue
            table = _locate(ix.get("table_schema"), ix.get("table_name"))
            table["indexes"].append(
                {
                    "name": _clip(ix.get("index_name"), trunc),
                    "unique": bool(ix.get("unique")),
                    "columns": _clip_list(ix.get("columns"), trunc, "index_columns"),
                }
            )

    # Node cap on tables (deterministic ordering by schema then name).
    ordered = sorted(
        (tables[k] for k in order),
        key=lambda t: (str(t["schema"] or ""), str(t["name"] or "")),
    )
    tables_out: List[Dict[str, Any]] = []
    for t in ordered:
        if not budget.take_node("tables"):
            continue
        t["columns"].sort(
            key=lambda c: (
                c["ordinal_position"] if c["ordinal_position"] is not None else 1 << 30,
                str(c["name"] or ""),
            )
        )
        t["catalog"] = _clip(t["catalog"], trunc)
        t["schema"] = _clip(t["schema"], trunc)
        t["name"] = _clip(t["name"], trunc)
        t["type"] = _clip(t["type"], trunc)
        tables_out.append(t)

    budget.finalize(trunc)
    body = {
        "database_product": _clip(doc.get("database_product"), trunc),
        "catalog": _clip(top_catalog, trunc),
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
