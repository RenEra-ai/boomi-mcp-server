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
from typing import Iterable, List, Optional, Sequence, Tuple

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


# ``<processcall>`` shapes carry the callee id in a ``processId`` attribute.
# Matched with a bounded, attribute-scoped pattern rather than a full parse
# because we want exactly one fact out of the document and nothing else: a
# targeted extraction cannot accidentally surface a sibling element's secret.
_PROCESS_CALL_ID = re.compile(
    r"<processcall\b[^>]*\bprocessId\s*=\s*[\"']([^\"'<>&]{1,128})[\"']",
    re.IGNORECASE,
)
_CACHE_ID = re.compile(
    r"<(?:documentcache|cache)\b[^>]*\b(?:documentCacheId|cacheId)\s*=\s*[\"']([^\"'<>&]{1,128})[\"']",
    re.IGNORECASE,
)
_PROPERTY_ID = re.compile(
    r"<(?:processproperty|property)\b[^>]*\b(?:componentId|processPropertyId)\s*=\s*[\"']([^\"'<>&]{1,128})[\"']",
    re.IGNORECASE,
)
_WSS_LISTEN = re.compile(r"\bwss\b[^>]*\blisten\b", re.IGNORECASE)
_ROUTE_PROCESS_ID = re.compile(
    r"<(?:operation|route)\b[^>]*\bprocessId\s*=\s*[\"']([^\"'<>&]{1,128})[\"']",
    re.IGNORECASE,
)


def _bounded(raw_xml: Optional[str]) -> str:
    if not raw_xml:
        return ""
    if len(raw_xml) > _MAX_XML_CHARS:
        # Refuse rather than truncate: a half-parsed document produces
        # confidently wrong witnesses, which is worse than no witness.
        return ""
    return raw_xml


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
    text = _bounded(raw_xml)
    if not text:
        return (), ()

    calls = tuple(
        ProcessCallEvidenceV1(
            caller_component_ref=component_ref,
            callee_component_ref=callee,
            witness="component_xml",
        )
        for callee in _dedupe(_PROCESS_CALL_ID.findall(text))
    )
    uses = tuple(
        SharedResourceUseEvidenceV1(
            process_component_ref=component_ref,
            resource_component_ref=resource,
            resource_kind=kind,  # type: ignore[arg-type]
            witness="component_xml",
        )
        for kind, pattern in (
            ("document_cache", _CACHE_ID),
            ("process_property", _PROPERTY_ID),
        )
        for resource in _dedupe(pattern.findall(text))
    )
    return calls, uses


def parse_api_service_component_evidence(
    api_service_component_ref: str,
    raw_xml: Optional[str],
) -> Tuple[ApiServiceRouteEvidenceV1, ...]:
    """Extract route-to-listener witnesses from an API Service Component's XML.

    Note what is NOT extracted: paths, methods, auth type, endpoint config. A
    route witness answers "this ASC invokes that process" and nothing more —
    carrying the path would put endpoint detail into a contract that promises
    opaque references only.

    The document must actually look like a WSS listen surface. An ASC whose
    operations are not listen-shaped does not make its targets listeners, and
    ``ListenerStatus`` is explicitly not accepted as a substitute (its observed
    behavior conflicts with the documented example, so it is registered
    ``unsupported`` rather than trusted).
    """
    text = _bounded(raw_xml)
    if not text or not _WSS_LISTEN.search(text):
        return ()
    return tuple(
        ApiServiceRouteEvidenceV1(
            api_service_component_ref=api_service_component_ref,
            listener_component_ref=target,
            witness="component_xml",
        )
        for target in _dedupe(_ROUTE_PROCESS_ID.findall(text))
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
    """Order-preserving dedupe — the same target listed twice is one edge."""
    seen: List[str] = []
    for value in values:
        if value and value not in seen:
            seen.append(value)
    return tuple(seen)


__all__: List[str] = [
    "normalize_dependency_corroboration",
    "parse_api_service_component_evidence",
    "parse_process_component_evidence",
    "project_process_ir_evidence",
]
