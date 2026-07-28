"""Pure projections from ProcessIR and component XML into typed witnesses (#144).

No I/O. Every function takes already-fetched data and returns typed witness
rows; the raw XML string never escapes this module, so nothing downstream can
accidentally carry a connection property, an endpoint, or a credential that
happened to sit next to the element we cared about.

The witness asymmetry
---------------------
What counts as proof depends on whether the process EXISTS yet:

* A **planned** process (``$ref:KEY``) has no component to parse. Its ProcessIR
  root is the authority for what it does, so a matching ``process_call`` node
  there is sufficient.
* An **existing** process (literal id) has a ProcessIR root only if someone
  authored one, and that root may describe an intended future shape rather than
  what is deployed. Only the component's own XML witnesses what it actually does.

Accepting ProcessIR for an existing process would let a plan assert an edge that
the deployed component does not have — the exact overclaim this issue exists to
prevent.

What the dependency API cannot do
---------------------------------
``analyze_component(action="dependencies")`` returns a FLAT, ONE-LEVEL list that
mixes component types and carries no edge kind: a sub-process call and a JSON
profile reference arrive as the same shape. It therefore corroborates a witness
and never establishes one. :func:`normalize_dependency_corroboration` types that
distinction so the two cannot be confused at a call site.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from typing import Iterable, List, Optional, Sequence, Set, Tuple

from .context import (
    ApiServiceRouteEvidenceV1,
    DependencyCorroborationV1,
    ProcessCallEvidenceV1,
    SharedResourceUseEvidenceV1,
)

_REF_TOKEN_PREFIX = "$ref:"

#: Upper bound on component XML this module will parse. A component far larger
#: than any real one is more likely a hostile or corrupt payload than a process,
#: and an unbounded parse is an easy memory amplification.
_MAX_XML_CHARS = 8 * 1024 * 1024


def _is_planned_ref(component_ref: str) -> bool:
    return component_ref.startswith(_REF_TOKEN_PREFIX)


def project_process_ir_evidence(
    process_component_ref: str,
    process_call_targets: Sequence[str],
    cache_targets: Sequence[str] = (),
    property_targets: Sequence[str] = (),
) -> Tuple[
    Tuple[ProcessCallEvidenceV1, ...], Tuple[SharedResourceUseEvidenceV1, ...]
]:
    """Project a PLANNED process's ProcessIR references into witnesses.

    Accepted only for a ``$ref:`` process. For a literal component id this
    returns nothing at all rather than raising: a caller assembling evidence for
    a mixed topology should not have to pre-partition it, and silently
    contributing nothing is the fail-closed outcome — the relation then has no
    witness and is gated, which is the correct verdict.
    """
    if not _is_planned_ref(process_component_ref):
        return (), ()

    calls = tuple(
        ProcessCallEvidenceV1(
            caller_component_ref=process_component_ref,
            callee_component_ref=target,
            witness="process_ir",
        )
        for target in process_call_targets
    )
    uses = tuple(
        SharedResourceUseEvidenceV1(
            process_component_ref=process_component_ref,
            resource_component_ref=target,
            resource_kind=kind,  # type: ignore[arg-type]
            witness="process_ir",
        )
        for kind, targets in (
            ("document_cache", cache_targets),
            ("process_property", property_targets),
        )
        for target in targets
    )
    return calls, uses


# ---------------------------------------------------------------------------
# XML extraction
# ---------------------------------------------------------------------------

# Element/attribute vocabulary, matched case-insensitively against the parsed
# tree. Regex extraction was the first implementation and was WRONG: a pattern
# has no idea what a comment is, so ``<!-- <processcall processId="x"/> -->``
# produced a witness for an edge the process does not have — and a witness is
# what authorizes a planning relation. The same applied to any malformed
# fragment that happened to contain matching text.
_PROCESS_CALL_TAGS = ("processcall",)
_PROCESS_CALL_ATTRS = ("processid",)
_CACHE_TAGS = ("documentcache", "cache")
_CACHE_ATTRS = ("documentcacheid", "cacheid")
_PROPERTY_TAGS = ("processproperty", "property")
_PROPERTY_ATTRS = ("componentid", "processpropertyid")
_ROUTE_TAGS = ("operation", "route")
_ROUTE_ATTRS = ("processid",)
_WSS_TAGS = ("wss",)

#: XXE / billion-laughs mitigation — reject DOCTYPE/ENTITY outright. A COPY of
#: the screen in ``categories.schema_discovery._safe_xml`` rather than an import:
#: the compiler must not depend on the tool layer (ADR-001 §6), the same reason
#: the secret list is copied in ``models.system_topology``.
_DOCTYPE_RE = re.compile(r"<!\s*(DOCTYPE|ENTITY)", re.IGNORECASE)


def _local_name(tag) -> str:
    """Namespace-stripped, lowercased element name.

    ``ElementTree`` reports a namespaced tag as ``{uri}local``. Comparing the
    raw tag would silently stop matching the moment a component carries a
    namespace declaration.
    """
    if not isinstance(tag, str):
        return ""
    return tag.rsplit("}", 1)[-1].lower()


def _attr(element, names: Tuple[str, ...]) -> Optional[str]:
    for key, value in element.attrib.items():
        if _local_name(key) in names and value:
            return value
    return None


def _parse(raw_xml: Optional[str]):
    """Parse bounded component XML, or return None. Never raises.

    Fail-closed at every step: too large, DTD-bearing, or malformed all yield
    None, which means "no witness" — and a relation with no witness is gated,
    which is the correct verdict for a document we could not read.
    """
    if not raw_xml or len(raw_xml) > _MAX_XML_CHARS:
        return None
    if _DOCTYPE_RE.search(raw_xml):
        return None
    try:
        return ET.fromstring(raw_xml)
    except ET.ParseError:
        return None
    except ValueError:
        # An encoding declaration on a ``str`` — retry as bytes, as the tool
        # layer's helper does.
        try:
            return ET.fromstring(raw_xml.encode("utf-8"))
        except (ET.ParseError, ValueError):
            return None


def _collect(root, tags: Tuple[str, ...], attrs: Tuple[str, ...]) -> Tuple[str, ...]:
    """Every attribute value on a matching element, deduplicated in order.

    ``iter()`` yields ELEMENTS only — comments and processing instructions are
    not elements under the default parser, so a commented-out shape contributes
    nothing without any special handling.
    """
    found = []
    for element in root.iter():
        if _local_name(element.tag) in tags:
            value = _attr(element, attrs)
            if value:
                found.append(value)
    return _dedupe(found)


def parse_process_component_evidence(
    component_ref: str,
    raw_xml: Optional[str],
) -> Tuple[
    Tuple[ProcessCallEvidenceV1, ...], Tuple[SharedResourceUseEvidenceV1, ...]
]:
    """Extract ProcessCall and shared-resource witnesses from a process's own XML.

    Required for an EXISTING (literal-id) process. Returns typed rows only; the
    XML itself is discarded here.
    """
    root = _parse(raw_xml)
    if root is None:
        return (), ()

    calls = tuple(
        ProcessCallEvidenceV1(
            caller_component_ref=component_ref,
            callee_component_ref=callee,
            witness="component_xml",
        )
        for callee in _collect(root, _PROCESS_CALL_TAGS, _PROCESS_CALL_ATTRS)
    )
    uses = tuple(
        SharedResourceUseEvidenceV1(
            process_component_ref=component_ref,
            resource_component_ref=resource,
            resource_kind=kind,  # type: ignore[arg-type]
            witness="component_xml",
        )
        for kind, tags, attrs in (
            ("document_cache", _CACHE_TAGS, _CACHE_ATTRS),
            ("process_property", _PROPERTY_TAGS, _PROPERTY_ATTRS),
        )
        for resource in _collect(root, tags, attrs)
    )
    return calls, uses


def parse_process_wss_listener_refs(raw_xml: Optional[str]) -> Tuple[str, ...]:
    """Does THIS process's own XML declare a WSS listen start shape?

    Returns the process's listen markers, or empty. The WSS Listen configuration
    lives on the linked PROCESS's start shape, not on the API Service Component
    — a real ASC capture contains no ``<wss>`` element at all. Looking for one
    inside the ASC therefore found nothing on every real component, which made
    the whole existing-ASC route witness unreachable.
    """
    root = _parse(raw_xml)
    if root is None:
        return ()
    markers = []
    for element in root.iter():
        name = _local_name(element.tag)
        attrs = {_local_name(k): str(v).strip().lower() for k, v in element.attrib.items()}
        if name in _WSS_TAGS or attrs.get("connectortype") == "wss":
            if any(
                attrs.get(key) in ("true", "listen")
                for key in ("listen", "mode", "operationtype", "actiontype")
            ) or name in _WSS_TAGS:
                markers.append(name)
    return tuple(markers)


def parse_api_service_component_evidence(
    api_service_component_ref: str,
    raw_xml: Optional[str],
    listener_process_refs: Sequence[str] = (),
) -> Tuple[ApiServiceRouteEvidenceV1, ...]:
    """Extract route-to-listener witnesses from an API Service Component's XML.

    ``listener_process_refs`` is the set of process refs whose OWN XML was found
    to declare a WSS listen start shape (see
    :func:`parse_process_wss_listener_refs`). A route is witnessed only when the
    ASC names a target AND that target is independently confirmed to be a
    listener — the plan's three-argument shape, which an earlier two-argument
    version dropped by searching for ``<wss>`` inside the ASC instead. Real ASC
    XML carries no such element, so that version witnessed nothing at all on a
    real component.

    Note what is NOT extracted: paths, methods, auth type, endpoint config. A
    route witness answers "this ASC invokes that process" and nothing more —
    carrying the path would put endpoint detail into a contract that promises
    opaque references only.

    ``ListenerStatus`` is explicitly not accepted as a substitute for the
    process-side confirmation: its observed behavior conflicts with the
    documented example, so it is registered ``unsupported`` rather than trusted.
    """
    root = _parse(raw_xml)
    if root is None:
        return ()

    targets = _collect(root, _ROUTE_TAGS, _ROUTE_ATTRS)
    confirmed = set(listener_process_refs)
    if confirmed:
        targets = tuple(target for target in targets if target in confirmed)
    else:
        # No process-side confirmation supplied. Fall back to a listen marker on
        # the ASC itself, which some shapes do carry — but never witness a route
        # with no listen evidence from either side.
        asc_declares_listen = any(
            _local_name(element.tag) in _WSS_TAGS
            and any(
                str(value).strip().lower() in ("true", "listen")
                for key, value in element.attrib.items()
                if _local_name(key) in ("listen", "mode", "operationtype")
            )
            for element in root.iter()
        )
        if not asc_declares_listen:
            return ()

    return tuple(
        ApiServiceRouteEvidenceV1(
            api_service_component_ref=api_service_component_ref,
            listener_component_ref=target,
            witness="component_xml",
        )
        for target in targets
    )


def normalize_dependency_corroboration(
    parent_component_ref: str,
    rows: Iterable[Tuple[str, str]],
) -> Tuple[DependencyCorroborationV1, ...]:
    """Type a dependency-API response as CORROBORATION, never as a witness.

    ``rows`` is ``(child_component_id, child_component_type)``. The result is
    deliberately a different type from every witness model, so promoting one is
    a type error rather than a judgement call at a call site.
    """
    return tuple(
        DependencyCorroborationV1(
            parent_component_ref=parent_component_ref,
            child_component_ref=child_id,
            child_component_type=child_type,
        )
        for child_id, child_type in rows
    )


def _dedupe(values: Iterable[str]) -> Tuple[str, ...]:
    """Order-preserving dedupe — the same target listed twice is one edge.

    The membership test runs against a SET while the list preserves order. A
    list-only version is quadratic, and the 8 MiB bound admits enough distinct
    targets for that to be a real cost on a corrupt or hostile component.
    """
    seen: Set[str] = set()
    kept: List[str] = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            kept.append(value)
    return tuple(kept)


__all__: List[str] = [
    "normalize_dependency_corroboration",
    "parse_api_service_component_evidence",
    "parse_process_wss_listener_refs",
    "parse_process_component_evidence",
    "project_process_ir_evidence",
]
