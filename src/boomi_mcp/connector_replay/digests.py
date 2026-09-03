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
    "RECORD_DIGEST_DOMAIN",
    "operation_record_digest_v1",
    "comparable_path",
    "PATH_EQUIVALENCE_PROBES",
    "path_equivalence_behaviour",
]

#: Domain separators. A digest is over ``domain + payload``, so two algorithms that
#: happened to serialize identical bytes still produce different values. Without
#: this, adding a second digest later silently makes the first one ambiguous.
ROUTE_DIGEST_DOMAIN: Final[bytes] = b"RouteDigestV1\0"
CONFIG_DIGEST_DOMAIN: Final[bytes] = b"ComponentConfigDigestV1\0"
RECORD_DIGEST_DOMAIN: Final[bytes] = b"OperationContractRecordV1\0"

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


def _require_absolute_http_url(raw: str) -> str:
    """Refuse a connection base that is not an absolute HTTP(S) URL with authority.

    Anything looser silently produced a route identity for a base that addresses
    nothing — and worse, DIFFERENT malformed bases collided: `ftp://h/base` and
    `http:///base` both reduced to the same path and therefore the same digest.
    A digest exists to distinguish routes; one that maps two unrelated malformed
    inputs onto one value is worse than a refusal.
    """
    parsed = urlsplit(raw.strip())
    if parsed.scheme.lower() not in ("http", "https"):
        raise RouteDigestRefused(
            f"connection url {raw!r} is not http(s); a route digest identifies an "
            "HTTP route and cannot describe another scheme"
        )
    if not parsed.netloc:
        raise RouteDigestRefused(
            f"connection url {raw!r} has no authority, so it addresses no host and "
            "the route it implies does not exist"
        )
    if parsed.query or parsed.fragment:
        raise RouteDigestRefused(
            f"connection url {raw!r} carries a query or fragment; neither is part of "
            "a route and including one would make the same route digest differently"
        )
    return parsed.path


def _reject_malformed_percent(path: str) -> None:
    """A stray `%` that is not a valid escape makes the path undecodable."""
    for index, char in enumerate(path):
        if char != "%":
            continue
        escape = path[index + 1:index + 3]
        if len(escape) != 2 or any(c not in "0123456789abcdefABCDEF" for c in escape):
            raise RouteDigestRefused(
                f"path {path!r} contains a malformed percent-escape at offset "
                f"{index}; it decodes to nothing and cannot identify a route"
            )


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
    base = _require_absolute_http_url(base_url)
    tail = template.strip()
    _reject_malformed_percent(base)
    _reject_malformed_percent(tail)
    # The operation resource is a PATH. Query and fragment are refused on the
    # connection url already; leaving them accepted here meant `/x?query=1` and
    # `/x#fragment` still received versioned digests through the other door.
    for marker, what in (("?", "query"), ("#", "fragment")):
        if marker in tail:
            raise RouteDigestRefused(
                f"operation path {tail!r} carries a {what}; neither is part of a "
                "route, and including one would digest the same route differently"
            )
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


def _projection_spec(kind: str, registry=None, family: str = "rest") -> dict:
    """The projection for ``kind``, taken from registry data.

    The plan makes these specs registry data on purpose: the allowlist binds an
    unbounded XML space, so what it covers must be extensible without a code change
    and must be visible in the artifact the registry publishes.
    """
    if registry is None:
        from .registry import load_registry

        # NO silent fallback. A registry that cannot load used to drop this to a
        # hard-coded projection, which is the fail-open reading of a failure: the
        # digest would still be produced, under a projection nobody published, and
        # would silently disagree with every digest computed when the registry did
        # load. A digest that cannot state its projection is not an identity.
        registry = load_registry()
    typed = registry.projection_for(kind, family) if registry is not None else None
    if typed is None:
        raise ConfigDigestRefused(
            f"the registry publishes no projection for a {kind!r} component, so "
            "there is no defined set of facts to digest. A projection is registry "
            "data; add one with a capture behind it"
        )
    if typed is not None:
        return {
            "attributes": list(typed.included_attributes),
            "value_fields": list(typed.included_value_fields),
            "property_fields": list(typed.included_property_fields),
            "excluded_fields": list(typed.excluded_fields),
            "elements": list(typed.included_elements),
            "scope_attributes": list(typed.included_scope_attributes),
            "excluded_scope_attributes": list(typed.excluded_scope_attributes),
            "projection_version": typed.projection_version,
            "qname_aware_tags": list(typed.qname_aware_tags),
            "qname_aware_attrs": list(typed.qname_aware_attrs),
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
    # ELEMENTS as well as fields. Checking only field ids left every unknown
    # element and wrapper outside the closed set: a behaviour-bearing element added
    # by the platform would leave the digest unchanged, so a changed live component
    # could still match captured configuration evidence.
    allowed_elements = set(spec.get("elements", ()))
    if allowed_elements:
        elements = {_qname(el)[1] for el in root.iter()}
        # A prefixed name is compared on its local part; the namespace URI is
        # carried into the projection tree separately.
        stray = sorted(e for e in elements
                       if e not in allowed_elements
                       and not any(a.endswith(":" + e) or a == e for a in allowed_elements))
        if stray:
            raise ConfigDigestRefused(
                "{0} component carries element(s) this projection does not cover: "
                "{1}. Refusing rather than ignoring them — an uncovered element may "
                "change what the component does, and a digest that omits it would "
                "let a changed component match captured evidence.".format(kind, stray)
            )

    unknown = sorted(seen - known)
    if unknown:
        raise ConfigDigestRefused(
            "{0} component carries field(s) this projection does not cover: {1}. "
            "Refusing rather than ignoring them — an uncovered field may change what "
            "the component does, and a digest that omits it would let two different "
            "components share one identity. Extend the registry's projection "
            "allowlist, with a capture behind it.".format(kind, unknown)
        )


#: Attributes the projection carries STRUCTURALLY rather than by allowlist, keyed
#: by the element that may carry them: a field's ``id`` becomes the projected node's
#: identity, its ``value`` the node text, and a property's ``key`` its name. Known by
#: construction, not by configuration — and element-QUALIFIED, because a flat set of
#: bare names accepted ``key`` on a field, where nothing reads it.
_STRUCTURAL_PAIRS = ("field/id", "field/value", "properties/key")


def _attr_class(spec, element: str, uri: str, attribute: str):
    """How this projection classifies one attribute ON ONE element.

    Element-qualified on purpose. A flat set of attribute names is not a
    classification: it accepted a property's ``key`` on a field and a connection's
    excluded ``url`` on a field, in both cases passing an attribute that reaches
    nothing — the fail-open the per-element allowlists existed to prevent, restored
    one level up while generalising them.
    """
    if uri:
        # A namespaced attribute is never the unqualified one of the same name.
        return None
    key = "{0}/{1}".format(element, attribute)
    if key in _STRUCTURAL_PAIRS:
        return "structural"
    if key in set(spec.get("scope_attributes", ())):
        return "carried"
    if key in set(spec.get("excluded_scope_attributes", ())):
        return "excluded"
    return None


def _admitted(root: ET.Element, spec: dict):
    """Every element the projection ADMITS, in document order.

    This set is the projection's own scope — the elements it already refuses to
    digest a component without. Deriving the attribute rule from it, rather than
    naming the shapes that carry attributes, is what stops the enumeration: three
    consecutive reviews each found an uncovered shape (a field, then a property,
    then a configuration element), because each fix named one more place instead of
    the space.
    """
    allowed = set(spec.get("elements", ()))
    if not allowed:
        return
    for el in root.iter():
        if _qname(el)[1] in allowed:
            yield el


def _attr_qname(name: str) -> tuple[str, str]:
    """(namespace URI, local name) for an attribute, as ElementTree spells it."""
    if name.startswith("{"):
        uri, _, local = name[1:].partition("}")
        return uri, local
    return "", name


def _refuse_unknown_attributes(root: ET.Element, spec: dict, kind: str) -> None:
    """Unknown ATTRIBUTES on any admitted element block, as unknown fields do.

    Compared by QNAME, not by local name. A prefixed ``x:type`` is a different
    attribute from ``type``: it would pass a local-name allowlist while the
    projection reads the unqualified name and finds nothing, so a field carrying a
    namespaced type digested identically to a field carrying none.
    """
    for el in _admitted(root, spec):
        element = _qname(el)[1]
        stray = sorted(
            name for name in el.attrib
            if _attr_class(spec, element, *_attr_qname(name)) is None
        )
        if stray:
            raise ConfigDigestRefused(
                "{0} component carries attribute(s) {1} on a projected <{2}> that "
                "this projection does not cover. Refusing rather than ignoring them "
                "— an uncovered attribute may change what the component does, and a "
                "digest that omits it would let a changed component match captured "
                "evidence. A namespaced attribute is never the unqualified one of "
                "the same name. Classify it in the registry projection, with a "
                "capture behind it.".format(kind, stray, _qname(el)[1]))


def _qname(el: ET.Element) -> tuple[str, str]:
    """(namespace URI, local name) — the identity XML actually assigns a name.

    Matching on local name alone treats two elements from different namespaces as
    the same element, which is how an unknown element from a namespace this
    projection has never seen would be read as a known one.
    """
    tag = el.tag
    if tag.startswith("{"):
        uri, _, local = tag[1:].partition("}")
        return uri, local
    return "", tag


def _project_tree(root: ET.Element, spec: dict, kind: str) -> ET.Element:
    """Build a NEW tree holding only what the projection names.

    Structural rather than value-scraping. The previous form flattened the
    component into `key=value` pairs and hashed sorted JSON, which discarded child
    ORDER and every namespace — so a reordered component, or one whose fields came
    from a different namespace, digested identically to the original.

    Comments and processing instructions are dropped structurally rather than
    stripped textually; whitespace-only text is dropped, real text preserved.
    """
    # The projection REVISION is digested, not merely recorded beside it. Two
    # digests from different projections are declared incomparable by the registry
    # model; without the revision inside the payload, a component whose projected
    # facts happen not to differ produces the same value under both, and a stored
    # identity can be read under the wrong revision.
    out = ET.Element("projection", {
        "kind": kind, "projection_version": str(spec.get("projection_version", 0))})
    for attr in spec.get("attributes", ()):
        seen = {el.get(attr) for el in root.iter() if el.get(attr) is not None}
        if len(seen) > 1:
            raise ConfigDigestRefused(
                f"attribute {attr!r} carries conflicting values {sorted(seen)!r}")
        node = ET.SubElement(out, "attribute", {"name": attr})
        node.text = next(iter(seen)) if seen else ""

    # Every classified attribute on every admitted element, in document order and
    # qualified by the element that carries it. A change to any of them moves the
    # digest; previously only a document-unique scan reached them, which could not
    # represent the same attribute appearing on two different elements.
    # ONE record per admitted element, in document order, emitted whether or not it
    # carries anything. Position is the record's identity: keyed only by element NAME,
    # two fields were indistinguishable, so moving the sole `type` from one to the
    # other left the payload identical. Emitting every admitted element also makes the
    # presence of an extra admitted element a change in its own right.
    for index, el in enumerate(_admitted(root, spec)):
        uri, local = _qname(el)
        # Bound to the element's STRUCTURAL identity, not only to its position among
        # admitted elements. Position alone collided: with a projected field and an
        # excluded one both carrying `type`, swapping which owned which produced the
        # same ordered sequence of records, so a routing field's type could change
        # and still match captured evidence.
        identity = {"ns": uri, "el": local, "at": str(index)}
        if el.get("id") is not None:
            identity["of"] = el.get("id")
        carried = {
            name.split("}")[-1]: value for name, value in sorted(el.attrib.items())
            if _attr_class(spec, local, *_attr_qname(name)) == "carried"
        }
        ET.SubElement(out, "on", {**identity, **carried})

    included = set(spec.get("value_fields", ())) | set(spec.get("property_fields", ()))
    # A repeated id is a REFUSAL, not last-one-wins. Two fields claiming the same id
    # means the caller's assumption about which one applies is already broken, and
    # quietly picking one bakes that ambiguity into a published value. This check
    # was lost when the projection changed shape and is restored here — replacing a
    # mechanism must carry its invariants across, not just its output.
    seen_ids: set[str] = set()
    for el in root.iter():
        uri, local = _qname(el)
        if local != "field":
            continue
        fid = el.get("id")
        if fid not in included:
            continue
        if fid in seen_ids:
            raise ConfigDigestRefused(
                f"field id {fid!r} appears more than once; which one applies is "
                "ambiguous and will not be digested")
        seen_ids.add(fid)
        node = ET.SubElement(out, "field", {"ns": uri, "id": fid})
        if fid in set(spec.get("value_fields", ())):
            node.text = el.get("value", "")
        else:
            # A property container: KEYS only, in document order. A static header's
            # value is a classic place for an API key, and this digest is published.
            for child in el.iter():
                if _qname(child)[1] == "properties" and child.get("key") is not None:
                    ET.SubElement(node, "key", {"name": child.get("key")})
    return out


def component_config_digest_v1(component_xml: str, kind: str, family: str = "rest") -> str:
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
    spec = _projection_spec(kind, family=family)
    _refuse_unknown_fields(root, spec, kind)
    _refuse_unknown_attributes(root, spec, kind)
    projected = _project_tree(root, spec, kind)
    # The SPECIFIED canonicalization, with its options pinned. Two components that
    # differ only in prefix spelling or in insignificant whitespace must digest the
    # same; two that differ in child order or namespace must not.
    canonical = ET.canonicalize(
        ET.tostring(projected, encoding="unicode"),
        with_comments=False,
        rewrite_prefixes=True,
        strip_text=False,
        qname_aware_tags=tuple(spec.get("qname_aware_tags", ())) or None,
        qname_aware_attrs=tuple(spec.get("qname_aware_attrs", ())) or None,
    )
    return "ComponentConfigDigestV1:" + hashlib.sha256(
        CONFIG_DIGEST_DOMAIN + canonical.encode("utf-8")
    ).hexdigest()


def account_scope_hash(account_id: str) -> str:
    """The account an artifact belongs to, hashed. REFUSES when unknown.

    Exported because two callers need it — ingestion, which stamps a record, and
    the compiler's corroboration, which checks one — and a second private copy is
    how two sides of the same comparison come to disagree.

    Hashing the empty string produces the well-known digest of nothing, and every
    accountless artifact would then carry the SAME scope, so evidence from two
    different unknown accounts would satisfy the very check this hash exists to
    enforce. A scope nobody established is not a scope.
    """
    import hashlib

    if not account_id:
        raise ValueError("no account id: the scope an artifact belongs to is unknown")
    return hashlib.sha256(account_id.encode("utf-8")).hexdigest()



def operation_record_digest_v1(record: object) -> str:
    """The digest of an operation record, over the record MINUS the digest field.

    PUBLISHED HERE because the loader has to be able to recompute it. This value
    is the evidence IDENTITY — grants carry it, the apply-boundary recheck matches
    on it, and the durable mutation attestation records it — and until now the
    only thing that computed it was the out-of-repo capture harness. So the
    packaged registry shipped an identifier nothing in the repository could
    check: alter a record's content while keeping its internal cross-checks
    consistent, and the stale digest was accepted as the identity of evidence
    that no longer described what it authorised.

    Domain-separated like its siblings, and over the model's own JSON dump so the
    serialization is the one the record is published in. The digest field is
    removed rather than blanked: a placeholder would make the digest depend on
    the placeholder's spelling.
    """
    import hashlib

    body = record.model_dump(mode="json") if hasattr(record, "model_dump") else dict(record)
    body.pop("record_digest", None)
    return hashlib.sha256(RECORD_DIGEST_DOMAIN + _canonical(body)).hexdigest()


def comparable_path(path: str) -> str:
    """A stored path reduced ONLY by what is safe to reduce out of context.

    Percent-escape hex is case-insensitive under RFC 3986 wherever the path ends
    up, so folding it is context-free and correct: `%2f` and `%2F` are the same
    octet against any base. That is the whole of what this does.

    DOT SEGMENTS ARE DELIBERATELY NOT RESOLVED, and the first version of this
    function resolved them — which invented an equivalence the composed route
    does not have. `route_digest_v1` removes them only AFTER joining the
    operation path to the connection base, and that order is load-bearing:
    against base `https://h/api`, `/../admin` addresses `/admin` while `/admin`
    addresses `/api/admin`. Folded in isolation the two compare equal, so the
    declared-versus-live check would approve a reuse whose live route is not the
    one the caller asserted — and their route digests, computed the correct way,
    differ. The same folding also swallowed `/secret?/../x` into `/x`, hiding a
    query the digest refuses outright.

    Two paths differing only by a dot segment are therefore reported as a
    mismatch. That is a refusal a caller can fix by declaring the path the account
    stores, and the alternative was accepting a route nobody checked.

    Deliberately NOT a lower-casing either: `/Orders` and `/orders` are different
    resources on a case-sensitive upstream. Only what the standard declares
    case-insensitive is folded.
    """
    return _normalize_percent_encoding(path.strip())


#: Every branch `rest_route_decision` has, named by the input that reaches it.
#: Derived from the function's own structure rather than from the two cases this
#: issue happened to change.
_ROUTE_DECISION_CASES: Final[dict] = {
    "unmodelled-family": ((("/a", "/b"), False, True)),
    "unresolved-identity": ((("/a",), True, False)),
    "no-path-field": (((), True, True)),
    "blank-only": ((("",), True, True)),
    "blank-plus-one-route": ((("", "/a"), True, True)),
    "blank-plus-two-routes": ((("", "/a", "/b"), True, True)),
    "blank-plus-two-spellings": ((("", "/a", "/%61"), True, True)),
    "one-route": ((("/a",), True, True)),
    "one-route-two-spellings": ((("/a", "/%61"), True, True)),
    "two-distinct-routes": ((("/a", "/b"), True, True)),
    "three-routes": ((("/a", "/b", "/c"), True, True)),
}


def _percent_probe_domain() -> tuple:
    """One probe per octet, in both hex spellings — DERIVED, never sampled.

    The normalizer makes a per-octet decision: an unreserved byte is decoded to
    its character, everything else keeps its escape with the hex upper-cased.
    Sampling one escape covers ONE of those arms. A release that dropped the
    decoding while keeping the upper-casing would leave every sampled output
    unchanged — and with it both published revisions — while a stored `/A` and a
    declared `/%41` flipped from a match to a refusal. That is the same
    stale-provenance defect this projection was added to close, reachable again
    through the half it did not look at.

    So the domain is every byte rather than a chosen few, in upper and lower hex
    so the case-folding arm is covered per octet too. It is 512 short strings:
    the cost of the honest version is nothing, and the sampled version was only
    ever cheaper to write.
    """
    # PER NIBBLE, not per escape. Generating an upper and a lower spelling treats
    # the case as a property of the pair, and it is a property of each digit: an
    # octet whose two nibbles are both alphabetic has FOUR valid spellings, and a
    # normalization that regressed to accepting only uniformly-cased pairs would
    # leave every uniformly-cased probe — and the revision with them — unchanged
    # while a stored `/%AF` and a declared `/%Af` stopped matching. This is the
    # same defect as the sampled vocabulary it replaced, one dimension in: the
    # domain was derived over the octets and then sampled over the casings.
    seen, probes = set(), []
    for byte in range(256):
        for spelling in (
            "{0:02X}".format(byte),
            "{0:02x}".format(byte),
            "{0:02X}".format(byte)[0] + "{0:02x}".format(byte)[1],
            "{0:02x}".format(byte)[0] + "{0:02X}".format(byte)[1],
        ):
            if spelling in seen:
                # A digit with no case — every octet whose nibbles are both
                # numeric — collapses to one spelling rather than four.
                continue
            seen.add(spelling)
            probes.append("/%" + spelling)
    # ...AND THE OTHER BRANCH. The escape pattern matches a percent followed by
    # exactly two hex digits; everything else falls through untouched, and that
    # fall-through is a decision this function makes just as much as the rewrite
    # is. A change that started rejecting or repairing a malformed escape, or that
    # stopped trimming, would move behaviour with every well-formed probe
    # unchanged — which is the shape of the last three findings against this
    # oracle, each one a dimension derived on one axis and sampled on the next.
    # THE READER'S OWN DECISION, not just the normalizer's. The oracle
    # fingerprinted what `comparable_path` returns and nothing about whether the
    # route reader USES it — so changing that reader from rejecting two spellings
    # of one route to accepting them moved acceptance while every probe output,
    # and both served revisions, stood still. A behaviour authority that projects
    # its helper and not its own decision is projecting the wrong thing.
    # THE AUTHORITY'S FULL CASE SET, not two samples of it. Two probes covered
    # the pair whose answer changed and left the blank, missing, unmodelled and
    # unresolved branches invisible — the same sample-instead-of-derive defect
    # this oracle has now produced twice, one axis in each time.
    probes.extend("\x00route-decision:" + case for case in _ROUTE_DECISION_CASES)
    probes.extend((
        "/%",        # a percent with nothing after it
        "/%2",       # one hex digit — a truncated escape
        "/%ZZ",      # two non-hex digits
        "/%2G",      # one hex digit and one not
        "/%%41",     # an escape whose percent is itself escaped
        "  /a  ",    # the surrounding whitespace this function strips
        "/a b",      # an unencoded space, which it does not touch
        "",          # the empty path
    ))
    return tuple(probes)


#: The distinctions `comparable_path` is required to draw. The structural cases are
#: named because each stands for a rule; the percent-escape cases are DERIVED over
#: the whole octet domain, because that rule is decided per byte.
PATH_EQUIVALENCE_PROBES: Final[tuple] = (
    "/Orders/42",        # literal characters — case-sensitive, must NOT fold
    "/orders/42",
    "/../admin",         # dot segments — context-dependent, must NOT fold
    "/admin",
    "/a/./b",
    "/a/b",
    "/secret?/../x",     # a query the route digest refuses outright
    "/x",
) + _percent_probe_domain()


def path_equivalence_behaviour() -> tuple:
    """What `comparable_path` reduces each probe to, as fingerprintable data.

    The OUTPUTS rather than pairwise verdicts: an output carries strictly more
    than an equal/unequal answer, so a change that alters a normalization without
    flipping any pair is still visible.

    This exists because a served revision that does not move when validator
    behaviour moves is the failure the revision manifest is built to detect —
    and this function decides what the declared-versus-live path check accepts.
    Changing it altered acceptance while both published revisions stood still,
    measured, which is the same defect its sibling grammar row was added to close.
    """
    results = []
    #: The route reader's verdict on the two cases whose answer this issue
    #: changed, carried as data beside the normalizer's outputs so a change to
    #: EITHER moves the revision. Derived by running the reader, never restated.
    def _reader_verdict(case):
        # THE DECISION ITSELF, called, and serialized WHOLE. An earlier version
        # built a component document to ask this question and the reader refused
        # it — the third hand-built fixture in this issue to prove only that the
        # shape had been guessed wrong. A later one recorded merely whether a
        # path came back, so changing WHICH route is retained moved declaration
        # matching and left the revision still.
        from ..authoring.connector_resolution_snapshot import rest_route_decision

        fields, modelled, resolved = _ROUTE_DECISION_CASES[case]
        state, conflicting, path = rest_route_decision(
            list(fields), modelled=modelled, resolved_enough=resolved
        )
        return "%s/%s/%s" % (state, bool(conflicting), path)

    for probe in PATH_EQUIVALENCE_PROBES:
        if probe.startswith("\x00route-decision:"):
            try:
                results.append((probe, _reader_verdict(probe.split(":", 1)[1])))
            except Exception as refusal:  # noqa: BLE001
                results.append((probe, "refused:" + type(refusal).__name__))
            continue
        try:
            results.append((probe, comparable_path(probe)))
        except Exception as refusal:  # noqa: BLE001
            # A REFUSAL IS A RESULT, recorded per probe. The revision builder
            # wraps each authority in a try/except that substitutes the constant
            # "unavailable", so a single raising probe would collapse this whole
            # row to that constant — and every later behaviour change would then
            # produce the identical revision, because the row no longer varies.
            # An oracle that stops distinguishing after the first refusal is worse
            # than one that never existed: it reports stability it is no longer
            # measuring. The malformed probes above exist precisely to fingerprint
            # a hardening that would raise, so this is the case they were added
            # for, not a hypothetical.
            results.append((probe, "refused:" + type(refusal).__name__))
    return tuple(results)
