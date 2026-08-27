"""Stable digests over the parts of a connector component that decide a route.

Two digests, each with its own domain separator so a value computed for one can
never be mistaken for the other even if the underlying bytes coincide:

* :func:`route_digest_v1` — where a call GOES: the connection's base path plus the
  operation's path template.
* :func:`component_config_digest_v1` — what a component IS configured to do, over a
  CLOSED projection of its XML.

Both are ALLOWLIST projections, and that is the load-bearing decision. The obvious
alternative — hash the whole component and subtract the secret fields — requires an
enumeration of every field that might carry credential material, maintained by hand,
forever, against a platform that adds connector fields on its own schedule. That
list is wrong the moment a new secret-bearing field ships, and it fails OPEN: the
unknown field lands in the digest, and the digest is a value this registry publishes.

So nothing is included unless it is named here. A connector field this module has
never heard of contributes nothing, which is a fail-closed default: the worst case
is two genuinely different configurations sharing a digest, which the registry
detects as a collision. Under a denylist the worst case is a published secret.

The captured connection XML that motivated this shows why the denylist is not merely
inconvenient: a single REST connection carries `password`, `customAuthCredentials`,
`awsSecretKey`, `awsPrivateKey`, `privateCertificate` and a nested OAuth2 block with
its own `credentials` element — eleven distinct places for secret material in ONE
connector type, before counting the ones other connectors introduce.
"""

from __future__ import annotations

import hashlib
import json
import re
import xml.etree.ElementTree as ET
from typing import Final
from urllib.parse import urlsplit

from boomi_mcp.errors import (
    CONNECTOR_REPLAY_CONFIGURATION_DIGEST_REFUSED,
    CONNECTOR_REPLAY_ROUTE_DIGEST_REFUSED,
)

__all__ = [
    "DigestRefused",
    "RouteDigestRefused",
    "ConfigDigestRefused",
    "ROUTE_DIGEST_DOMAIN",
    "CONFIG_DIGEST_DOMAIN",
    "route_digest_v1",
    "component_config_digest_v1",
]

#: Domain separators. A digest is over ``domain + payload``, so two algorithms that
#: happened to serialize identical bytes still produce different values. Without
#: this, adding a second digest later silently makes the first one ambiguous.
ROUTE_DIGEST_DOMAIN: Final[bytes] = b"RouteDigestV1\0"
CONFIG_DIGEST_DOMAIN: Final[bytes] = b"ComponentConfigDigestV1\0"

#: The closed projection. Nothing outside this contributes to a config digest.
#:
#: Operation: the verb and the path template decide what the call does and where it
#: goes. Query-parameter and request-header NAMES are included because adding a
#: parameter changes the request; their VALUES are excluded because a static header
#: value is exactly where an API key gets parked.
#: FALLBACK ONLY. The authoritative projection specs are registry DATA — see
#: `_projection_spec` — so extending what a digest covers is a data change backed by
#: a capture rather than a code edit. These constants remain as the shape the code
#: expects and as the default when a registry is not supplied.
#:
#: MEASURED against every operation component in the archive, not assumed. The
#: platform serves exactly four operation fields, and all four are `<field id=...>`
#: entries — including the two property containers. An earlier version of this
#: module looked for `queryParameters` and `requestHeaders` as ELEMENT TAGS, which
#: no component carries, so that branch was dead: parameter and header names never
#: reached a digest at all, and `followRedirects` was not in the allowlist either.
#: Two operations differing only in redirect behaviour digested identically.
_OPERATION_FIELDS: Final[frozenset[str]] = frozenset({"path", "followRedirects"})
_OPERATION_ATTRS: Final[tuple[str, ...]] = ("customOperationType",)
#: Field ids whose value is a customProperties block. Their NAMES are digested;
#: their VALUES are not — a static header value is where an API key gets parked.
_OPERATION_PROPERTY_FIELDS: Final[frozenset[str]] = frozenset(
    {"queryParameters", "requestHeaders"}
)

#: Connection: the base URL only. Not the username, not the auth mode's operands —
#: the route is what this digest is for, and everything else on a connection is
#: either irrelevant to routing or a place secrets live.
_CONNECTION_FIELDS: Final[frozenset[str]] = frozenset({"url"})


class DigestRefused(Exception):
    """A digest could not be computed from the supplied bytes.

    Each subclass carries the stable taxonomy code a caller reports. The code is
    an attribute rather than something the raiser passes, so every raise site
    reports the same code and none can invent one.
    """

    code: str = ""


class RouteDigestRefused(DigestRefused):
    """The route could not be determined."""

    code = CONNECTOR_REPLAY_ROUTE_DIGEST_REFUSED


class ConfigDigestRefused(DigestRefused):
    """The component configuration could not be projected."""

    code = CONNECTOR_REPLAY_CONFIGURATION_DIGEST_REFUSED


def _canonical(payload: object) -> bytes:
    """Serialize a digest payload so distinct inputs cannot collide.

    Concatenating values with separators is NOT injective, and the failure is
    quiet: joining property keys with a comma made ``["a,b"]`` and ``["a", "b"]``
    identical, and an empty key indistinguishable from no keys at all. The builder
    accepts arbitrary non-secret strings as keys, so those are reachable inputs,
    not pathological ones — and the same hazard sat one level up, where a field
    value containing a newline could have impersonated an adjacent line.

    JSON with sorted keys and no whitespace encodes structure rather than eliding
    it: strings are quoted and escaped, so a separator inside a value can no longer
    be read as a separator between values. It is also auditable — a reader can see
    exactly what went into a published digest.
    """
    return json.dumps(payload, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False).encode("utf-8")


def _localname(tag: str) -> str:
    """Strip any namespace. The platform's XML mixes prefixed and bare elements."""
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def _parse(xml: str, refusal: type[DigestRefused], what: str) -> ET.Element:
    if not isinstance(xml, str) or not xml.strip():
        raise refusal(f"{what}: expected non-empty XML text")
    try:
        return ET.fromstring(xml)
    except ET.ParseError as exc:
        raise refusal(f"{what}: not well-formed XML ({exc})") from exc


def _field_values(
    root: ET.Element,
    wanted: frozenset[str],
    refusal: type[DigestRefused] = ConfigDigestRefused,
) -> dict[str, str]:
    """Collect ``<field id=... value=...>`` for ids in ``wanted``.

    ``refusal`` is the caller's own exception type. This helper is shared by both
    digests, and raising the config refusal from a route derivation would report a
    registered code the caller never advertises.

    A repeated id is a refusal rather than a last-one-wins: two fields claiming the
    same id means the caller's assumption about which one decides the route is
    already broken, and quietly picking one would bake that ambiguity into a
    published value.
    """
    found: dict[str, str] = {}
    for el in root.iter():
        if _localname(el.tag) != "field":
            continue
        fid = el.get("id")
        if fid not in wanted:
            continue
        if fid in found:
            raise refusal(
                f"field id {fid!r} appears more than once; the route it implies is "
                "ambiguous and will not be digested"
            )
        found[fid] = el.get("value", "")
    return found


#: Characters RFC 3986 calls unreserved: percent-encoding them carries no meaning,
#: so an encoded form and a literal form denote the same path and must digest the
#: same. Everything else keeps its encoding, with the hex digits upper-cased.
_UNRESERVED: Final[frozenset[str]] = frozenset(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"
)

_PCT = re.compile(r"%([0-9A-Fa-f]{2})")


def _normalize_percent_encoding(path: str) -> str:
    """Decode unreserved octets; upper-case the hex of everything else.

    Both halves of RFC 3986's percent-encoding normalization. Doing only one would
    leave two spellings of one path digesting differently, which for a digest that
    identifies captured route coverage means evidence that cannot be matched to the
    route it was captured for.
    """

    def replace(match: re.Match[str]) -> str:
        octet = int(match.group(1), 16)
        char = chr(octet)
        return char if char in _UNRESERVED else "%" + match.group(1).upper()

    return _PCT.sub(replace, path)


def _remove_dot_segments(path: str) -> str:
    """RFC 3986 §5.2.4, applied literally.

    Written out rather than approximated with a library call: `posixpath.normpath`
    collapses duplicate separators, and internal empty segments are significant
    here — two paths that differ only by one may route differently.
    """
    output: list[str] = []
    while path:
        if path.startswith("../"):
            path = path[3:]
        elif path.startswith("./"):
            path = path[2:]
        elif path.startswith("/./"):
            path = "/" + path[3:]
        elif path == "/.":
            path = "/"
        elif path.startswith("/../"):
            path = "/" + path[4:]
            if output:
                output.pop()
        elif path == "/..":
            path = "/"
            if output:
                output.pop()
        elif path in (".", ".."):
            path = ""
        else:
            end = path.find("/", 1)
            if end == -1:
                end = len(path)
            output.append(path[:end])
            path = path[end:]
    return "".join(output)


def _effective_path(base_url: str, template: str) -> str:
    """The single path a call actually addresses.

    ONE path, not two values hashed side by side: a connection base and an
    operation template are joined by the runtime before the request is made, so a
    digest over the pair identifies something the wire never sees — and two
    different splits of the same effective path would digest differently.

    Trailing slash and internal empty segments are PRESERVED; both are
    route-significant, and a normalizer that removed them would let evidence
    captured for one resource satisfy a claim about another.
    """
    base = urlsplit(base_url.strip()).path if "://" in base_url else base_url.strip()
    tail = template.strip()
    # Exactly one slash at the boundary — neither swallowed nor doubled.
    joined = base.rstrip("/") + "/" + tail.lstrip("/") if tail else base
    if not joined.startswith("/"):
        joined = "/" + joined
    return _remove_dot_segments(_normalize_percent_encoding(joined)) or "/"


def route_digest_v1(connection_xml: str, operation_xml: str) -> str:
    """Digest of where a connector call goes.

    The connection supplies the base URL's path component; the operation supplies
    its own path template. Neither alone identifies a route: two operations with
    the same template against different connections hit different services, and one
    connection serves many operations.
    """
    conn = _parse(connection_xml, RouteDigestRefused, "connection")
    oper = _parse(operation_xml, RouteDigestRefused, "operation")

    conn_fields = _field_values(conn, _CONNECTION_FIELDS, RouteDigestRefused)
    if "url" not in conn_fields:
        raise RouteDigestRefused(
            "the connection declares no url field; a route cannot be derived from a "
            "connection whose base is unknown"
        )
    oper_fields = _field_values(oper, frozenset({"path"}), RouteDigestRefused)
    # A BLANK operation path is meaningful and must not be confused with a missing
    # one: blank is how a dynamically-bound path is authored, and it is exactly the
    # case this issue exists to support.
    if "path" not in oper_fields:
        raise RouteDigestRefused(
            "the operation declares no path field; a blank path is authored as an "
            "empty value, not by omitting the field, so this component is not the "
            "shape this digest reads"
        )

    template = oper_fields["path"].strip()
    if not template:
        # A blank path is how a DYNAMICALLY BOUND route is authored — the actual
        # path is composed per document at runtime. Every such operation would
        # otherwise digest to the same base-only value, so one captured document's
        # route could satisfy coverage for every other. There is no static route
        # here to identify, and inventing one is worse than refusing.
        raise RouteDigestRefused(
            "the operation's path is blank, which is how a dynamically bound route "
            "is authored: the path is composed per document at runtime, so no "
            "static route digest identifies it. Service-wide evidence is required "
            "for such an operation, not a route digest"
        )
    path = _effective_path(conn_fields["url"], template)
    # Stored WITH its version prefix. A bare hex string cannot say which algorithm
    # produced it, so two algorithms' outputs would be interchangeable in storage
    # even though the domain separator kept them distinct in the hash.
    return "RouteDigestV1:" + hashlib.sha256(
        ROUTE_DIGEST_DOMAIN + path.encode("utf-8")
    ).hexdigest()


def _projection_spec(kind: str, registry=None) -> dict:
    """The projection for ``kind``, taken from registry data.

    The plan makes these specs registry data on purpose: the allowlist binds an
    unbounded XML space, so what it covers must be extensible without a code change
    and must be visible in the artifact the registry publishes.
    """
    if registry is None:
        from .registry import load_registry

        try:
            registry = load_registry()
        except Exception:  # a registry that cannot load must not silently widen this
            registry = None
    typed = registry.projection_for(kind) if registry is not None else None
    if typed is not None:
        return {
            "attributes": list(typed.included_attributes),
            "value_fields": list(typed.included_value_fields),
            "property_fields": list(typed.included_property_fields),
            "excluded_fields": list(typed.excluded_fields),
        }
    return {
        "attributes": list(_OPERATION_ATTRS) if kind == "operation" else [],
        "value_fields": sorted(_OPERATION_FIELDS if kind == "operation" else _CONNECTION_FIELDS),
        "property_fields": sorted(_OPERATION_PROPERTY_FIELDS) if kind == "operation" else [],
        "excluded_fields": [],
    }


def _refuse_unknown_fields(root: ET.Element, spec: dict, kind: str) -> None:
    """Unknown content BLOCKS; it does not get ignored.

    The stopping rule this projection was designed under is explicit: the allowlist
    binds an unbounded space by design, and unknown content refuses. Silently
    skipping an unrecognised field is the fail-OPEN reading of the same allowlist —
    a connector field added by the platform tomorrow, or a credential-bearing field
    this module has never seen, would leave the digest unchanged and let two
    behaviourally different components share one published identity.
    """
    # THREE categories, not two. A field is included in the digest, or explicitly
    # excluded from it, or UNKNOWN — and only the third refuses. Conflating the
    # second and third would have refused every real connection, because a
    # connection deliberately contributes only its base URL and carries some thirty
    # other fields that are secrets or irrelevant to routing. Naming them as
    # excluded forces a decision per field instead of silent omission, which is the
    # point: a field nobody has classified is exactly the one that might matter.
    known = (set(spec["value_fields"]) | set(spec["property_fields"])
             | set(spec.get("excluded_fields", ())))
    seen = {
        el.get("id") for el in root.iter()
        if _localname(el.tag) == "field" and el.get("id") is not None
    }
    unknown = sorted(seen - known)
    if unknown:
        raise ConfigDigestRefused(
            "{0} component carries field(s) this projection does not cover: {1}. "
            "Refusing rather than ignoring them — an uncovered field may change what "
            "the component does, and a digest that omits it would let two different "
            "components share one identity. Extend the registry's projection "
            "allowlist, with a capture behind it.".format(kind, unknown)
        )


def _projected_operation(root: ET.Element) -> list[str]:
    lines: list[tuple[str, object]] = []
    for attr in _OPERATION_ATTRS:
        # The attribute may sit on the root or on a nested config element; search
        # rather than assume a depth, but take it only once.
        seen = [el.get(attr) for el in root.iter() if el.get(attr) is not None]
        if len(set(seen)) > 1:
            raise ConfigDigestRefused(
                f"attribute {attr!r} carries conflicting values {sorted(set(seen))!r}"
            )
        lines.append((attr, seen[0] if seen else ""))
    for fid, value in sorted(_field_values(root, _OPERATION_FIELDS).items()):
        lines.append(("field:" + fid, value))
    for el in root.iter():
        if _localname(el.tag) != "field":
            continue
        fid = el.get("id")
        if fid not in _OPERATION_PROPERTY_FIELDS:
            continue
        # KEYS only. A static header's value is a classic place for an API key,
        # and this digest is published.
        #
        # The attribute is `key` on a `<properties>` element — the shape the
        # component builder emits, which its own docstring records as verified
        # against two live components. Two earlier versions of this read the wrong
        # thing: first an element tag no component carries, then a `name`
        # attribute that does not exist on this element. Both produced an empty
        # list for every real component, so populated parameters and headers
        # contributed nothing to the digest while the code looked correct.
        names = sorted(
            child.get("key", "")
            for child in el.iter()
            if _localname(child.tag) == "properties" and child.get("key") is not None
        )
        lines.append((fid + ".keys", names))
    return sorted(lines)


def _projected_connection(root: ET.Element) -> list[tuple[str, object]]:
    fields = _field_values(root, _CONNECTION_FIELDS)
    return [("field:" + fid, fields.get(fid, "")) for fid in sorted(_CONNECTION_FIELDS)]


def component_config_digest_v1(component_xml: str, kind: str) -> str:
    """Digest a component's routing-relevant configuration.

    ``kind`` is ``"operation"`` or ``"connection"``. It is required rather than
    sniffed: guessing from the XML would mean a component that looks like neither
    silently gets one projection or the other, and the two projections are not
    comparable.
    """
    if kind not in ("operation", "connection"):
        raise ConfigDigestRefused(
            f"kind must be 'operation' or 'connection', not {kind!r}"
        )
    root = _parse(component_xml, ConfigDigestRefused, kind)
    _refuse_unknown_fields(root, _projection_spec(kind), kind)
    lines = _projected_operation(root) if kind == "operation" else _projected_connection(root)
    return "ComponentConfigDigestV1:" + hashlib.sha256(
        CONFIG_DIGEST_DOMAIN + _canonical({"kind": kind, "fields": lines})
    ).hexdigest()
