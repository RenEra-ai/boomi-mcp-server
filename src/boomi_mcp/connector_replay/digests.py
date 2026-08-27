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
import xml.etree.ElementTree as ET
from typing import Final
from urllib.parse import urlsplit

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
_OPERATION_FIELDS: Final[frozenset[str]] = frozenset({"path"})
_OPERATION_ATTRS: Final[tuple[str, ...]] = ("customOperationType",)
_OPERATION_PROPERTY_CONTAINERS: Final[frozenset[str]] = frozenset(
    {"queryParameters", "requestHeaders"}
)

#: Connection: the base URL only. Not the username, not the auth mode's operands —
#: the route is what this digest is for, and everything else on a connection is
#: either irrelevant to routing or a place secrets live.
_CONNECTION_FIELDS: Final[frozenset[str]] = frozenset({"url"})


class DigestRefused(Exception):
    """A digest could not be computed from the supplied bytes."""


class RouteDigestRefused(DigestRefused):
    """The route could not be determined."""


class ConfigDigestRefused(DigestRefused):
    """The component configuration could not be projected."""


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


def _field_values(root: ET.Element, wanted: frozenset[str]) -> dict[str, str]:
    """Collect ``<field id=... value=...>`` for ids in ``wanted``.

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
            raise ConfigDigestRefused(
                f"field id {fid!r} appears more than once; the route it implies is "
                "ambiguous and will not be digested"
            )
        found[fid] = el.get("value", "")
    return found


def _normalized_path(raw: str) -> str:
    """RFC 3986 syntax-based normalization, restricted to what a path needs.

    Deliberately NOT a general URL normalizer. Case-folding a host would be wrong
    here because the host is not part of what this returns, and resolving dot
    segments against a base we do not have would invent a path.
    """
    path = raw.strip()
    if "://" in path:
        path = urlsplit(path).path
    if not path.startswith("/"):
        path = "/" + path if path else "/"
    # Collapse duplicate separators; a trailing separator is not significant for a
    # route, but the root itself must survive as "/".
    parts = [p for p in path.split("/") if p]
    return "/" + "/".join(parts) if parts else "/"


def route_digest_v1(connection_xml: str, operation_xml: str) -> str:
    """Digest of where a connector call goes.

    The connection supplies the base URL's path component; the operation supplies
    its own path template. Neither alone identifies a route: two operations with
    the same template against different connections hit different services, and one
    connection serves many operations.
    """
    conn = _parse(connection_xml, RouteDigestRefused, "connection")
    oper = _parse(operation_xml, RouteDigestRefused, "operation")

    conn_fields = _field_values(conn, _CONNECTION_FIELDS)
    if "url" not in conn_fields:
        raise RouteDigestRefused(
            "the connection declares no url field; a route cannot be derived from a "
            "connection whose base is unknown"
        )
    oper_fields = _field_values(oper, _OPERATION_FIELDS)
    # A BLANK operation path is meaningful and must not be confused with a missing
    # one: blank is how a dynamically-bound path is authored, and it is exactly the
    # case this issue exists to support.
    if "path" not in oper_fields:
        raise RouteDigestRefused(
            "the operation declares no path field; a blank path is authored as an "
            "empty value, not by omitting the field, so this component is not the "
            "shape this digest reads"
        )

    base = _normalized_path(urlsplit(conn_fields["url"].strip()).path)
    template = oper_fields["path"].strip()
    payload = "\n".join(("base=" + base, "template=" + template)).encode("utf-8")
    return hashlib.sha256(ROUTE_DIGEST_DOMAIN + payload).hexdigest()


def _projected_operation(root: ET.Element) -> list[str]:
    lines: list[str] = []
    for attr in _OPERATION_ATTRS:
        # The attribute may sit on the root or on a nested config element; search
        # rather than assume a depth, but take it only once.
        seen = [el.get(attr) for el in root.iter() if el.get(attr) is not None]
        if len(set(seen)) > 1:
            raise ConfigDigestRefused(
                f"attribute {attr!r} carries conflicting values {sorted(set(seen))!r}"
            )
        lines.append(f"{attr}={seen[0] if seen else ''}")
    for fid, value in sorted(_field_values(root, _OPERATION_FIELDS).items()):
        lines.append(f"field:{fid}={value}")
    for el in root.iter():
        if _localname(el.tag) not in _OPERATION_PROPERTY_CONTAINERS:
            continue
        container = _localname(el.tag)
        # NAMES only. A static header's value is a classic place for an API key,
        # and this digest is published.
        names = sorted(
            child.get("name", "")
            for child in el.iter()
            if child.get("name") is not None
        )
        lines.append(f"{container}.names={','.join(names)}")
    return lines


def _projected_connection(root: ET.Element) -> list[str]:
    fields = _field_values(root, _CONNECTION_FIELDS)
    return [f"field:{fid}={fields.get(fid, '')}" for fid in sorted(_CONNECTION_FIELDS)]


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
    lines = _projected_operation(root) if kind == "operation" else _projected_connection(root)
    payload = ("kind=" + kind + "\n" + "\n".join(lines)).encode("utf-8")
    return hashlib.sha256(CONFIG_DIGEST_DOMAIN + payload).hexdigest()
