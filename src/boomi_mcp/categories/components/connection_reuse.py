"""suggest_connection_reuse — read-only connection-reuse discovery (M7.3, issue #83).

Ranks EXISTING ``connector-settings`` components for safe reuse so an agent can
wire a reused connection (keeping credentials out of the conversation) instead of
authoring a new one. Read-only: queries component metadata and reads component XML
only to extract non-secret endpoint context. Never mutates Boomi, never echoes
credential material.

Every response carries ``read_only=True`` / ``boomi_mutation=False`` /
``raw_xml_exposed=False`` (mirrors ``_IMPORT_FLAGS`` in ``integration_import.py``)
so the advertised contract holds on success AND error.

Candidates are returned with IntegrationSpecV1-compatible reuse bindings:
``reference_only=True`` connections (resolved by component_id) plus an exact-name
fallback paired with ``conflict_policy='reuse'`` — the two reuse surfaces the
build path already understands (see ``integration_builder`` reference_only
resolution). Only whitelisted, non-secret endpoint fields are ever echoed, and the
response is scanned with the existing redaction/secret-shape helpers before return.
"""

from __future__ import annotations

import difflib
import re
import time
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, urlsplit, urlunsplit

from boomi import Boomi
from boomi.net.transport.api_error import ApiError
from boomi.models import (
    ComponentMetadataQueryConfig,
    ComponentMetadataQueryConfigQueryFilter,
    ComponentMetadataSimpleExpression,
    ComponentMetadataSimpleExpressionOperator,
    ComponentMetadataSimpleExpressionProperty,
    ComponentMetadataGroupingExpression,
    ComponentMetadataGroupingExpressionOperator,
)

from ._shared import (
    component_get_xml,
    paginate_metadata,
    _component_get_deadline_seconds,
    ComponentGetDeadlineExceeded,
)
from .builders.connector_builder import (
    _resolve_rest_connector_type,
    _resolve_soap_client_connector_type,
    REST_CLIENT_SUBTYPE,
    SOAP_CLIENT_SUBTYPE,
    DatabaseConnectorBuilder,
)
from ...patterns.primitives._helpers import value_looks_secret, _key_looks_secret

# ---------------------------------------------------------------------------
# Contract constants
# ---------------------------------------------------------------------------

_REUSE_FLAGS = {"read_only": True, "boomi_mutation": False, "raw_xml_exposed": False}

CONNECTION_REUSE_QUERY_FAILED = "CONNECTION_REUSE_QUERY_FAILED"

# top_k clamp — a discovery tool never needs to return the whole account.
_TOP_K_MIN = 1
_TOP_K_MAX = 25

# GenericConnectionConfig field ids that are safe URL context (REST base URL,
# SOAP WSDL/endpoint URL). Everything else (username/password/oauth/token/…) is
# never read. Values are reduced to a credential-free skeleton before echoing.
_URL_SAFE_FIELD_IDS = ("url", "endpoint")

# Cap connector-action metadata attached per candidate.
_MAX_PAIRED_ACTIONS = 3

_TOKEN_RE = re.compile(r"[a-z0-9]+")
_SEGMENT_RE = re.compile(r"[\\/]+")
_SHARED_FOLDER_SEGMENTS = frozenset({"common", "shared", "library", "lib"})

# Credential material can be embedded IN an endpoint value: URL userinfo
# (scheme://user:pass@host), query/fragment params (?api_key=… / #token=…), or a
# JDBC connection string's properties — which use a bewildering set of
# driver-specific grammars (';' matrix, '?' query, MySQL '//user:pass@', Oracle
# thin 'user/pass@//host', …). Rather than parse each grammar to redact in place
# (an unwinnable game), the tool echoes only a credential-FREE skeleton:
#   * a standard URL is reduced (via urlsplit) to scheme://host[:port][/path] —
#     userinfo, query, and fragment (the only credential-bearing parts) dropped;
#   * anything else (JDBC strings, schemeless values) is reduced to its bare
#     host, so no connection-string credential grammar can ever be echoed.
# DB identity is still conveyed by the host/port/dbname/driverId scalar attrs,
# and endpoint MATCHING uses the host (via _host_of), so ranking is unaffected.
_JDBC_PREFIX_RE = re.compile(r"^\s*jdbc:", re.IGNORECASE)
# A fully-safe echoable skeleton is EXACTLY scheme://host[:port] — nothing else.
# The standard-URL skeleton is validated against this before echoing so any host
# junk (matrix/backslash/path/credential suffix) that slipped through yields an
# omit (None) instead of an echo. Host = an [IPv6] literal or hostname chars.
_SAFE_SKELETON_RE = re.compile(
    r"^[a-z][a-z0-9+.\-]*://(?:\[[0-9A-Fa-f:]+\]|[A-Za-z0-9.\-_]+)(?::\d+)?$",
    re.IGNORECASE,
)
# Schemeless echo path (no scheme): a clean bare hostname, and a pure Boomi
# placeholder (letters/digits/spaces only, e.g. "SET IN EXTENSION"). Anything
# else — percent-encoded delimiters, params, junk — is omitted, never echoed.
_HOSTNAME_RE = re.compile(r"^[A-Za-z0-9._\-]+$")
_IPV6_RE = re.compile(r"^[0-9A-Fa-f:]+$")  # hex + ':' only (loose IPv6 literal)
# A non-numeric port is echoable ONLY when it is an externalized-value
# placeholder (`${PORT}`, `{{port}}`, `$PORT`) — NOT arbitrary text (which could
# be a password from a malformed `user:pass` value with no scheme/'@').
_PORT_PLACEHOLDER_RE = re.compile(r"[${}]")
# Known Boomi externalization sentinels — the ONLY non-host phrases echoed
# verbatim. Anything else (arbitrary text that could contain a secret) is omitted.
_KNOWN_PLACEHOLDERS = frozenset({"set in extension"})
# Best-effort host from a JDBC authority, for MATCHING ONLY — never echoed.
# Property-based host form (SQL Server `;serverName=host`, plus server/host/
# hostName variants) where the host is a connection property, not in //authority.
_JDBC_HOST_PROP_RE = re.compile(
    r"[;?&](?:servername|server|host|hostname|databaseserver)=([^;?&#/\\\s]+)",
    re.IGNORECASE,
)
# A colon-separated JDBC PORT segment: pure digits, optionally followed by a
# /path or ;props. A dotted IPv4 host (10.0.0.1) does NOT match, so it isn't
# mistaken for the port.
_JDBC_PORT_SEG_RE = re.compile(r"^\d+(?:[/;?#].*)?$")


# ---------------------------------------------------------------------------
# Small text/host helpers
# ---------------------------------------------------------------------------

def _tokens(text: Optional[str]) -> set:
    """Lowercase alphanumeric tokens of length >= 3 (drops noise words)."""
    if not text:
        return set()
    return {t for t in _TOKEN_RE.findall(text.lower()) if len(t) >= 3}


def _host_of(value: Optional[str]) -> str:
    """Extract a bare lowercase host from a URL, host:port, IPv6, or bare host."""
    if not value:
        return ""
    v = value.strip()
    if "://" in v:
        return (urlparse(v).hostname or "").lower()
    v = v.split("/", 1)[0]  # drop any path
    if v.startswith("["):  # bracketed IPv6 literal [::1][:port]
        return v[1:].split("]", 1)[0].lower()
    if v.count(":") >= 2:  # bare IPv6 (multiple colons) — no host:port to split
        return v.lower()
    return v.split(":", 1)[0].lower()  # host[:port]


def _localname(tag: str) -> str:
    """Strip an ``{namespace}`` prefix from an ElementTree tag."""
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def _jdbc_host(url_format: Optional[str]) -> str:
    """Best-effort host from a JDBC connection string, for MATCHING ONLY.

    Never echoed (the connection string itself is never returned) — this only
    feeds endpoint ranking so that ``custom_url`` DB connections (Snowflake etc.)
    whose host lives solely in ``urlFormat`` can still be distinguished by an
    ``endpoint_hint``. Returns '' when no host is discernible.

    Covers the four JDBC host-location grammars: (1) ``//authority`` (mysql,
    postgres, sqlserver, jtds, db2, snowflake), (2) Oracle ``@host`` / ``@//host``,
    (3) a property-based host (SQL Server ``;serverName=host``, plus server/host/
    hostName/databaseServer variants), and (4) colon-separated
    ``jdbc:<driver>[:<subname>]:host:port[/db]`` (Sybase jConnect, Informix,
    Oracle SID-without-``@``) — the host is the segment right before the numeric
    port. The authority forms strip userinfo to the LAST ``@``."""
    if not isinstance(url_format, str):
        return ""
    # Drop braced property values first (SQL Server allows a ';'-containing value
    # in {braces}) so host-looking text inside a password — e.g.
    # `password={p;serverName=secret}` — can't be mistaken for the real host.
    url_format = re.sub(r"\{[^}]*\}", "", url_format)
    # The FIRST '//' is the authority marker (mysql/postgres/sqlserver/… and
    # Oracle `@//host`); a later '//' inside a property value (`;password=p//x`)
    # is ignored. If that first '//' is immediately followed by ';' (or a path/
    # query delimiter), the authority is EMPTY — a property-based form
    # (`//;serverName=host`) — so fall through to property parsing.
    idx = url_format.find("//")
    if idx != -1:
        after_slashes = url_format[idx + 2:]
        if after_slashes[:1] not in (";", "/", "?", "#", ""):
            # Authority form. Split off any ';property' block first — but ONLY a
            # real `;key=value` property; a ';' inside a userinfo password (NOT
            # followed by `key=`, e.g. `//user:pa;ss@host`) stays in the authority.
            # This disambiguates `//host:1433;password=p@ss` (host before ';') from
            # `//user:pa;ss@host` (host after the userinfo '@').
            prop = re.search(r";[^;/?#@=\s]*=", after_slashes)
            authority = after_slashes[: prop.start()] if prop else after_slashes
            # Userinfo (if any) ends at the LAST '@' — regardless of how many
            # ';' '/' ':' '@' the password holds — so the host follows it, never a
            # pre-'@' username fragment.
            if "@" in authority:
                authority = authority.rsplit("@", 1)[-1].lstrip("/")
            match = re.match(r"(\[[0-9A-Fa-f:]+\]|[^/:;?@\\\s]+)", authority)
            if match:
                return match.group(1).strip("[]").lower()
    match = _JDBC_HOST_PROP_RE.search(url_format)  # SQL Server serverName= etc.
    if match:
        return match.group(1).lower()
    # Colon-separated host descriptor (no '//'): the host is the segment
    # immediately before the PORT segment. The port is purely numeric (optionally
    # followed by a /path or ;props) — so an IPv4 host like `10.0.0.1` (dotted,
    # not pure digits) is NOT mistaken for the port. Userinfo before the host
    # (Oracle SID `user/pass@host`) is stripped to the last '@'.
    segments = url_format.split(":")
    for i in range(1, len(segments)):
        if _JDBC_PORT_SEG_RE.match(segments[i]):
            host = segments[i - 1].strip()
            host = host.rsplit("@", 1)[-1].split("/", 1)[0].split(";", 1)[0]
            return host.lower()
    return ""


def _safe_url_skeleton(value: Optional[str]) -> Optional[str]:
    """Return a credential-FREE display skeleton of an endpoint value, or None.

    A standard URL is reduced (via urlsplit) to ``scheme://host[:port]``:
    userinfo, PATH, query, and fragment are ALL dropped — the path is dropped
    because it can carry credentials (webhook tokens, ``;jsessionid=…``), so only
    scheme+host+port (which cannot hold a credential) is echoed. Anything that is
    NOT a parseable standard URL (a JDBC connection string, a schemeless value)
    is reduced to its bare host. Returns None when no host can be determined.
    Echo-only — endpoint MATCHING uses ``_host_of``/``_jdbc_host``, so ranking is
    unaffected."""
    if not isinstance(value, str) or not value:
        return None
    raw = value.strip()
    # A standard URL (NOT a jdbc: connection string, which urlsplit misparses).
    if "://" in raw and not _JDBC_PREFIX_RE.match(raw):
        # ANY '@' makes host extraction ambiguous: userinfo can contain
        # ':' '/' '?' '#' '%' ';' (`user:p?ss@host`, `svc:pa/ss@host`), and even a
        # ';'/'\'-matrix '@' or an '@' spanning a delimiter yields the wrong host.
        # A real base URL never carries an '@' (Boomi keeps credentials in
        # separate encrypted fields), so omit rather than risk echoing a username.
        if "@" in raw:
            return None
        # Whitespace in a scheme'd authority is also malformed.
        m = re.match(r"[A-Za-z][A-Za-z0-9+.\-]*://([^/?#]*)", raw)
        if m and re.search(r"\s", m.group(1)):
            return None
        try:
            parts = urlsplit(raw)
        except ValueError:
            parts = None
        # .hostname parses without validating the port; only .port int-converts,
        # so a templated/externalized port (…:${PORT}) must not discard the host.
        host = parts.hostname if parts is not None else None
        if host and "%" in host:
            # Percent-encoding in the host means encoded delimiters (a `%40`
            # encoded '@' hides userinfo, so the pre-'%' text is the username).
            # A real hostname never contains '%' — omit rather than truncate.
            return None
        if host:
            # urlsplit's .hostname keeps a ';jsessionid=…' matrix suffix when it
            # precedes the first '/', so truncate at the first char that cannot
            # appear in a real host (letters/digits/'.'/'-'/'_' and ':' for IPv6).
            valid = re.match(r"[A-Za-z0-9.:_\-]+", host)
            host = valid.group(0) if valid else None
        port = None
        if host:
            try:
                port = parts.port
            except ValueError:
                # A non-numeric port is acceptable ONLY as an externalized
                # placeholder (`${PORT}`) — otherwise the value is a malformed
                # `user:password` (no scheme/'@'), so the "host" is the username.
                raw_port = parts.netloc.rsplit(":", 1)[-1] if parts is not None else ""
                if not _PORT_PLACEHOLDER_RE.search(raw_port):
                    return None
                port = None
        if parts is not None and parts.scheme and host:
            # scheme://host[:port] only — drop userinfo, PATH, query, fragment.
            # The path is dropped because it can carry credentials (webhook
            # tokens like /services/T00/B00/SECRET, ;jsessionid=… matrix params).
            host_disp = f"[{host}]" if ":" in host else host  # bracket IPv6
            netloc = f"{host_disp}:{port}" if port else host_disp
            skeleton = urlunsplit((parts.scheme, netloc, "", "", ""))
            # Belt-and-suspenders: only echo a value that is EXACTLY
            # scheme://host[:port]; anything else (residual junk) is omitted.
            return skeleton if _SAFE_SKELETON_RE.match(skeleton) else None
    # A JDBC connection string is never echoed — its host is carried by the
    # `host` scalar attr, and the driver-specific credential grammars are too
    # varied to reduce safely.
    if _JDBC_PREFIX_RE.match(raw):
        return None
    # A URL-shaped value that produced no host → omit (never echo a bare
    # "scheme:" fragment or a still-credential-bearing raw URL).
    if "://" in raw:
        return None
    # ANY '@' makes host extraction ambiguous (same reasoning as the standard-URL
    # branch) — omit rather than risk echoing a username. A real host value has no
    # '@'; a ';' matrix WITHOUT '@' still reduces to the host below.
    if "@" in raw:
        return None
    # Schemeless value → bare host[:port]: the authority ends at the first
    # '/', '\', '?' or '#'; a ';' after the host is a matrix param — drop it.
    authority = re.split(r"[/\\?#]", raw, 1)[0]
    authority = authority.split(";", 1)[0].strip()
    # Bracketed IPv6 literal [addr][:port].
    if authority.startswith("["):
        inner, _, after = authority[1:].partition("]")
        if _IPV6_RE.match(inner):
            port = after[1:] if after.startswith(":") else ""
            return f"[{inner}]:{port}" if port.isdigit() else f"[{inner}]"
        return None
    # Bare IPv6 (2+ colons, hex only) — echo whole; there is no host:port to split.
    if authority.count(":") >= 2 and _IPV6_RE.match(authority):
        return authority.lower()
    # authority is host[:port]. Echo a valid hostname; keep a NUMERIC port, keep a
    # ${PORT}-style placeholder port, but OMIT any other non-numeric port (a
    # malformed `user:password` value would otherwise leak the username).
    host_part, sep, port_part = authority.partition(":")
    if _HOSTNAME_RE.match(host_part):
        if not sep:
            return host_part
        if port_part.isdigit():
            return f"{host_part}:{port_part}"
        if _PORT_PLACEHOLDER_RE.search(port_part):
            return host_part
        return None  # arbitrary non-numeric "port" (likely a password) → omit
    # Not host-shaped: echo ONLY a KNOWN Boomi externalization sentinel (e.g.
    # "SET IN EXTENSION"). Any other non-host phrase (which could contain a
    # secret, e.g. "MY PASSWORD IS hunter2") is omitted, not echoed.
    if raw.strip().lower() in _KNOWN_PLACEHOLDERS:
        return raw.strip()
    return None


# ---------------------------------------------------------------------------
# Subtype resolution
# ---------------------------------------------------------------------------

def _resolve_subtype(connector_type: str) -> str:
    """Resolve caller connector_type to the canonical Boomi subType stored on
    components. REST/SOAP aliases resolve to their canonical subtypes; anything
    else (``database``, ``sftp``, …) is treated as an exact raw subtype."""
    rest = _resolve_rest_connector_type(connector_type)
    if rest is not None:
        return rest
    soap = _resolve_soap_client_connector_type(connector_type)
    if soap is not None:
        return soap
    return connector_type


def _connector_family(resolved_subtype: str) -> str:
    """Map a resolved subtype to the reference_only ``connector_type`` family
    label the build path uses (matches the primitives' reuse configs)."""
    if resolved_subtype == REST_CLIENT_SUBTYPE:
        return "rest"
    if resolved_subtype == SOAP_CLIENT_SUBTYPE:
        return "soap_client"
    if resolved_subtype.lower() == "database":
        return "database"
    # Unknown family → the documented "raw" marker; the actual subtype is still
    # carried in each candidate's `subtype` field.
    return "raw"


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def _hint_host(endpoint_hint: Optional[str]) -> str:
    """Extract the comparable host from an endpoint hint the SAME way candidate
    endpoint values are reduced (JDBC hints via ``_jdbc_host``; others via
    ``_safe_url_skeleton`` then ``_host_of``), so hint and candidate hosts compare
    symmetrically. Returns '' when no host is discernible."""
    if not endpoint_hint:
        return ""
    hint_raw = endpoint_hint.strip()
    if _JDBC_PREFIX_RE.match(hint_raw):
        return _jdbc_host(hint_raw)
    return _host_of(_safe_url_skeleton(hint_raw) or hint_raw)


def _endpoint_prefilter_bonus(
    folder: Optional[str], endpoint_hint: Optional[str]
) -> int:
    """A cheap, metadata-only endpoint signal — folder-segment token overlap with
    the hint host — used ONLY to order the bounded XML-enrichment window, NEVER
    added to the final score (whose endpoint signal is the dedicated
    ``_endpoint_score`` bucket; folding it into the score would double-count).

    Without it, in a large account (> ``working_cap`` matching components) a
    candidate whose FOLDER names the hinted host but whose name/purpose do not
    would sit at the baseline cheap score, sort out of the enrichment window, never
    receive its real endpoint score, and be dropped despite being the exact match.
    (Name overlap with the hint is already carried by ``_name_score``.)"""
    host = _hint_host(endpoint_hint)
    if not host:
        return 0
    hint_tokens = _tokens(host)
    if not hint_tokens:
        return 0
    folder_tokens: set = set()
    for s in _SEGMENT_RE.split(folder or ""):
        folder_tokens |= _tokens(s)
    return 5 if (hint_tokens & folder_tokens) else 0


def _folder_score(
    folder: Optional[str], purpose: Optional[str]
) -> Tuple[int, List[str]]:
    """Up to 15: shared/common placement + purpose/folder token overlap."""
    if not folder:
        return 0, []
    reasons: List[str] = []
    score = 0
    seg_lower = [s.lower() for s in _SEGMENT_RE.split(folder) if s]
    if any(s == "#common" for s in seg_lower):
        score = 15
        reasons.append("in #Common shared folder")
    elif any(s in _SHARED_FOLDER_SEGMENTS for s in seg_lower):
        score = 10
        reasons.append("in a shared/common folder")
    # Folder locality credits the PURPOSE dimension only — endpoint matching has
    # its own dedicated score bucket, so folding endpoint_hint tokens in here
    # would double-count the endpoint signal.
    wanted = _tokens(purpose)
    folder_tokens: set = set()
    for s in seg_lower:
        folder_tokens |= _tokens(s)
    overlap = wanted & folder_tokens
    if overlap:
        score = min(15, score + 5)
        reasons.append("folder matches purpose (" + ", ".join(sorted(overlap)) + ")")
    return score, reasons


def _name_score(
    name: Optional[str], purpose: Optional[str], endpoint_hint: Optional[str]
) -> Tuple[int, List[str]]:
    """Up to 15: max(SequenceMatcher ratio, token-overlap) vs purpose+hint."""
    target = " ".join(x for x in (purpose, endpoint_hint) if x).strip()
    if not name or not target:
        return 0, []
    ratio = difflib.SequenceMatcher(None, name.lower(), target.lower()).ratio()
    name_tokens = _tokens(name)
    target_tokens = _tokens(target)
    token_overlap = (
        len(name_tokens & target_tokens) / len(target_tokens)
        if target_tokens
        else 0.0
    )
    score = round(max(ratio, token_overlap) * 15)
    if score <= 0:
        return 0, []
    common = name_tokens & target_tokens
    if common:
        return score, ["name matches purpose (" + ", ".join(sorted(common)) + ")"]
    return score, ["name is similar to the requested purpose"]


def _endpoint_score(
    endpoint_hint: Optional[str], endpoint_values: List[str]
) -> Tuple[int, List[str]]:
    """Up to 30 for host match against extracted endpoint context.

    30 exact host, 20 subdomain/suffix host, 10 normalized substring, else 0.
    """
    if not endpoint_hint:
        return 0, []
    hint_raw = endpoint_hint.strip()
    # Extract the hint host the SAME way candidate endpoint values are extracted so
    # they compare symmetrically (shared with the enrichment prefilter via
    # _hint_host): a query/matrix/scheme in the hint doesn't block an exact match.
    hint_host = _hint_host(hint_raw)
    hint_norm = hint_raw.lower()
    best = 0
    reason: Optional[str] = None
    for raw in endpoint_values:
        if not raw:
            continue
        cand_host = _host_of(raw)
        raw_low = raw.strip().lower()
        if hint_host and cand_host:
            if hint_host == cand_host:
                if best < 30:
                    best, reason = 30, f"exact host match ({cand_host})"
                continue
            if cand_host.endswith("." + hint_host) or hint_host.endswith("." + cand_host):
                if best < 20:
                    best, reason = 20, f"subdomain/suffix host match ({cand_host})"
                continue
        if best < 10 and hint_norm and (
            hint_norm in raw_low or (hint_host and hint_host in raw_low)
        ):
            best, reason = 10, "endpoint substring match"
    return best, ([reason] if reason else [])


# ---------------------------------------------------------------------------
# Safe endpoint-context extraction (XML → non-secret fields only)
# ---------------------------------------------------------------------------

def _extract_safe_context(raw_xml: str) -> Tuple[Dict[str, Any], List[str]]:
    """Parse connector-settings XML and return ONLY whitelisted endpoint fields
    plus the list of endpoint strings usable for host matching.

    Reads DatabaseConnectionSettings safe attrs and GenericConnectionConfig
    ``url``/``endpoint`` fields. Never reads username/password/oauth/token/
    encrypted values (they are simply not in the whitelist)."""
    context: Dict[str, Any] = {}
    endpoint_values: List[str] = []
    try:
        root = ET.fromstring(raw_xml)
    except ET.ParseError:
        return context, endpoint_values

    for el in root.iter():
        local = _localname(el.tag)
        if local == "DatabaseConnectionSettings":
            # host/port/dbname/driverId are safe scalar attrs (no credentials).
            for attr in ("host", "port", "dbname", "driverId"):
                val = el.get(attr)
                if val:
                    context[attr] = val
            if context.get("host"):
                endpoint_values.append(context["host"])
            # urlFormat is a JDBC connection string — NEVER echoed. Extract a
            # best-effort host from it for MATCHING ONLY (custom_url connections
            # put the host solely here, with empty host/port/dbname attrs).
            url_format = el.get("urlFormat")
            if url_format:
                jdbc_host = _jdbc_host(url_format)
                if jdbc_host:
                    endpoint_values.append(jdbc_host)
        elif local == "field":
            field_id = el.get("id")
            if field_id in _URL_SAFE_FIELD_IDS:
                val = el.get("value")
                if val:
                    skeleton = _safe_url_skeleton(val)
                    if skeleton:
                        context[field_id] = skeleton
                        endpoint_values.append(skeleton)
                    elif _JDBC_PREFIX_RE.match(val.strip()):
                        # A JDBC url field (Database V2 GenericConnectionConfig) is
                        # never echoed, but keep its host for MATCHING.
                        jdbc_host = _jdbc_host(val)
                        if jdbc_host:
                            endpoint_values.append(jdbc_host)
    return context, endpoint_values


# ---------------------------------------------------------------------------
# Secret backstop
# ---------------------------------------------------------------------------

# NOTE: there is deliberately NO error-message sanitizer here. Exception text is
# an unbounded surface — a driver/SDK/platform message can embed a credential in
# any format — so the two error branches in the handler never echo it; they
# return only leak-proof bounded signals (exception type name + numeric HTTP
# status). The `_scrub_secrets` backstop below guards the SUCCESS payload, whose
# shape we fully control.


def _scrub_secrets(node: Any) -> None:
    """Defensive in-place scrub: redact forbidden-keyed values, then drop any
    secret-shaped key or secret-looking value that slipped through. We only ever
    populate whitelisted fields, so this should be a no-op — it exists so a
    future field-whitelist change can never leak credential material."""
    DatabaseConnectorBuilder.redact_forbidden_secret_fields_in_place(node)
    if isinstance(node, dict):
        for key in list(node.keys()):
            value = node[key]
            if isinstance(key, str) and _key_looks_secret(key):
                node.pop(key, None)
                continue
            if value_looks_secret(value):
                node[key] = "[REDACTED]"
            else:
                _scrub_secrets(value)
    elif isinstance(node, list):
        for item in node:
            _scrub_secrets(item)


# ---------------------------------------------------------------------------
# Reference bindings
# ---------------------------------------------------------------------------

def _build_reference(component_id: str, name: str, family: str) -> Dict[str, Any]:
    """Emit IntegrationSpecV1-compatible reuse bindings for a candidate."""
    key = "reused_connection"
    return {
        # Archetype/build binding keyed on the stable component id.
        "archetype_binding": {"mode": "reuse", "component_id": component_id},
        # config shape the build path resolves as reference_only (by id).
        "reference_only_config": {
            "reference_only": True,
            "connector_type": family,
            "component_id": component_id,
        },
        # A ready-to-drop IntegrationSpecV1 component (reference_only by id).
        "integration_spec_component_example": {
            "key": key,
            "type": "connector-settings",
            "action": "create",
            "name": name,
            "config": {
                "reference_only": True,
                "connector_type": family,
                "component_id": component_id,
            },
        },
        # Exact-name fallback: no id, resolved by name under conflict_policy=reuse.
        "exact_name_fallback": {
            "conflict_policy": "reuse",
            "component": {
                "key": key,
                "type": "connector-settings",
                "action": "create",
                "name": name,
                "config": {
                    "reference_only": True,
                    "connector_type": family,
                    "component_name": name,
                },
            },
        },
    }


# ---------------------------------------------------------------------------
# Paired connector-action metadata (optional, best-effort, metadata-only)
# ---------------------------------------------------------------------------

def _query_subtype_metadata(
    boomi_client: Boomi, component_type: str, subtype: str
) -> List[Dict[str, Any]]:
    """Metadata query for TYPE == component_type AND SUBTYPE == subtype."""
    type_expr = ComponentMetadataSimpleExpression(
        operator=ComponentMetadataSimpleExpressionOperator.EQUALS,
        property=ComponentMetadataSimpleExpressionProperty.TYPE,
        argument=[component_type],
    )
    subtype_expr = ComponentMetadataSimpleExpression(
        operator=ComponentMetadataSimpleExpressionOperator.EQUALS,
        property=ComponentMetadataSimpleExpressionProperty.SUBTYPE,
        argument=[subtype],
    )
    root_expr = ComponentMetadataGroupingExpression(
        operator=ComponentMetadataGroupingExpressionOperator.AND,
        nested_expression=[type_expr, subtype_expr],
    )
    query_filter = ComponentMetadataQueryConfigQueryFilter(expression=root_expr)
    query_config = ComponentMetadataQueryConfig(query_filter=query_filter)
    return paginate_metadata(boomi_client, query_config)


def _pair_actions(
    candidate_folder: str,
    candidate_name: str,
    actions: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Attach up to _MAX_PAIRED_ACTIONS same-subtype actions, preferring same
    folder then name-token overlap. Metadata only, marked non-authoritative."""
    if not actions:
        return []
    name_tokens = _tokens(candidate_name)

    def _rank(action: Dict[str, Any]) -> Tuple[int, int]:
        same_folder = 1 if action.get("folder_name") == candidate_folder and candidate_folder else 0
        overlap = len(name_tokens & _tokens(action.get("name")))
        return (same_folder, overlap)

    ranked = sorted(actions, key=_rank, reverse=True)
    paired: List[Dict[str, Any]] = []
    for action in ranked:
        score = _rank(action)
        if score == (0, 0):
            break  # no locality signal at all — don't pad with noise
        paired.append({
            "component_id": action.get("component_id"),
            "name": action.get("name"),
            "folder": action.get("folder_name"),
            "component_type": "connector-action",
            "authoritative": False,
        })
        if len(paired) >= _MAX_PAIRED_ACTIONS:
            break
    return paired


# ---------------------------------------------------------------------------
# Main handler
# ---------------------------------------------------------------------------

def suggest_connection_reuse_action(
    boomi_client: Boomi,
    profile: str,
    connector_type: str,
    purpose: Optional[str] = None,
    endpoint_hint: Optional[str] = None,
    top_k: int = 5,
) -> Dict[str, Any]:
    """Rank existing connector-settings components for safe reuse.

    Read-only: queries component metadata and reads candidate XML only to pull
    non-secret endpoint context. Returns reference_only / conflict_policy='reuse'
    bindings and never echoes credential material.
    """
    try:
        if not connector_type or not str(connector_type).strip():
            return {
                "_success": False,
                "error": "connector_type is required",
                "error_code": CONNECTION_REUSE_QUERY_FAILED,
                "profile": profile,
                **_REUSE_FLAGS,
            }

        try:
            top_k_int = int(top_k)
        except (TypeError, ValueError):
            top_k_int = 5
        top_k_int = min(max(top_k_int, _TOP_K_MIN), _TOP_K_MAX)

        resolved_subtype = _resolve_subtype(connector_type.strip())
        family = _connector_family(resolved_subtype)

        # --- Query connector-settings of this subtype ---
        settings = _query_subtype_metadata(
            boomi_client, "connector-settings", resolved_subtype
        )

        base = {
            "_success": True,
            "profile": profile,
            "connector_type": connector_type,
            "resolved_subtype": resolved_subtype,
            "connector_family": family,
            "top_k": top_k_int,
            **_REUSE_FLAGS,
        }

        if not settings:
            return {**base, "total_matched": 0, "candidates": []}

        # --- Cheap scoring from metadata (folder + name) ---
        scored: List[Dict[str, Any]] = []
        for comp in settings:
            folder = comp.get("folder_name", "")
            name = comp.get("name", "")
            folder_pts, folder_reasons = _folder_score(folder, purpose)
            name_pts, name_reasons = _name_score(name, purpose, endpoint_hint)
            cheap = 40 + folder_pts + name_pts  # subtype match is always 40
            scored.append({
                "component_id": comp.get("component_id", ""),
                "name": name,
                "folder": folder,
                "_cheap": cheap,
                # Ordering key for the bounded enrichment window: the cheap score
                # PLUS a metadata-only endpoint prefilter signal (kept OUT of
                # `_cheap`, so it never reaches the final score — no double-count).
                "_prefilter": cheap + _endpoint_prefilter_bonus(folder, endpoint_hint),
                "_folder_pts": folder_pts,
                "_name_pts": name_pts,
                "_reasons": folder_reasons + name_reasons,
            })

        # --- Bounded XML enrichment (endpoint context) for the top window ---
        # Order by `_prefilter` so a folder-names-host candidate still reaches
        # enrichment in a large account; the final score below uses `_cheap` only.
        scored.sort(key=lambda c: (c["_prefilter"], c["_cheap"], c["name"]), reverse=True)
        working_cap = min(len(scored), max(top_k_int * 4, 20))
        enrichment_capped = working_cap < len(scored)

        # Aggregate wall-clock budget: a window of stalled component reads must
        # not sum past the platform request timeout (mirrors the query_components
        # bulk-get pattern). Each GET gets at most the remaining budget; once it
        # is spent the rest are left un-enriched (endpoint score 0) rather than
        # starting another possibly-stalling request.
        budget = float(_component_get_deadline_seconds())
        enrichment_budget_exhausted = False
        for cand in scored[:working_cap]:
            endpoint_pts = 0
            endpoint_reasons: List[str] = []
            safe_context: Dict[str, Any] = {}
            if budget < 1:
                enrichment_budget_exhausted = True
                endpoint_reasons = ["endpoint context skipped (read budget exhausted)"]
            else:
                started = time.monotonic()
                try:
                    comp_xml = component_get_xml(
                        boomi_client, cand["component_id"], deadline_seconds=int(budget)
                    )
                    safe_context, endpoint_values = _extract_safe_context(
                        comp_xml.get("xml", "")
                    )
                    endpoint_pts, endpoint_reasons = _endpoint_score(
                        endpoint_hint, endpoint_values
                    )
                except ComponentGetDeadlineExceeded:
                    endpoint_reasons = ["endpoint context unavailable (component read timed out)"]
                except Exception:
                    endpoint_reasons = ["endpoint context unavailable (component read failed)"]
                finally:
                    budget -= time.monotonic() - started
            cand["_endpoint_pts"] = endpoint_pts
            cand["_reasons"] = cand["_reasons"] + endpoint_reasons
            cand["_safe_context"] = safe_context

        # --- Optional paired connector-action metadata (best-effort) ---
        actions: List[Dict[str, Any]] = []
        try:
            actions = _query_subtype_metadata(
                boomi_client, "connector-action", resolved_subtype
            )
        except Exception:
            actions = []

        # --- Assemble final candidates ---
        assembled: List[Dict[str, Any]] = []
        for cand in scored[:working_cap]:
            total = cand["_cheap"] + cand.get("_endpoint_pts", 0)
            why = ["connector subtype match (" + resolved_subtype + ")"] + cand["_reasons"]
            safe_context = cand.get("_safe_context", {})
            _scrub_secrets(safe_context)
            candidate = {
                "component_id": cand["component_id"],
                "name": cand["name"],
                "folder": cand["folder"],
                "component_type": "connector-settings",
                "subtype": resolved_subtype,
                "score": total,
                "why_matched": why,
                "safe_context": safe_context,
                "paired_actions": _pair_actions(cand["folder"], cand["name"], actions),
                "reference": _build_reference(
                    cand["component_id"], cand["name"], family
                ),
            }
            assembled.append(candidate)

        assembled.sort(key=lambda c: (c["score"], c["name"]), reverse=True)
        result_candidates = assembled[:top_k_int]

        # --- Final belt-and-suspenders scrub over the whole payload ---
        _scrub_secrets(result_candidates)

        return {
            **base,
            "total_matched": len(settings),
            "candidates_scanned": working_cap,
            "enrichment_capped": enrichment_capped,
            "enrichment_budget_exhausted": enrichment_budget_exhausted,
            "candidates": result_candidates,
        }

    except ApiError as e:
        # No error path echoes the platform message. An exception message is an
        # UNBOUNDED text surface — a driver/SDK/platform error can embed a
        # credential in any format (quoted, escaped, JSON, JDBC, header) — and
        # sanitizing the unknowable is a losing game. Instead surface only
        # leak-proof BOUNDED signals: the exception type name and the numeric HTTP
        # status (401 auth · 403 perms · 400 query · 429 rate-limit · 5xx platform),
        # which is the actionable part anyway. The contract holds by construction.
        status = getattr(e, "status", None)
        status = status if isinstance(status, int) else None
        suffix = f" (HTTP {status})" if status is not None else ""
        return {
            "_success": False,
            "error": f"Failed to query reusable connections{suffix}.",
            "error_code": CONNECTION_REUSE_QUERY_FAILED,
            "exception_type": type(e).__name__,
            "http_status": status,
            "profile": profile,
            **_REUSE_FLAGS,
        }
    except Exception as e:
        # Same rule for any other exception — echo only the type name, never str(e).
        return {
            "_success": False,
            "error": f"Failed to query reusable connections (unexpected {type(e).__name__}).",
            "error_code": CONNECTION_REUSE_QUERY_FAILED,
            "exception_type": type(e).__name__,
            "profile": profile,
            **_REUSE_FLAGS,
        }


__all__ = ["suggest_connection_reuse_action"]
