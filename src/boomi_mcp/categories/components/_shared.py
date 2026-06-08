"""
Shared helpers for component tools.

Provides XML-based component retrieval and parsing used across
query_components, manage_component, and analyze_component modules.
"""

from typing import Dict, Any, List, Optional
import os
import threading
import time
import xml.etree.ElementTree as ET

from boomi import Boomi
from boomi.models import (
    ComponentMetadataQueryConfig,
    ComponentMetadataQueryConfigQueryFilter,
    ComponentMetadataSimpleExpression,
    ComponentMetadataSimpleExpressionOperator,
    ComponentMetadataSimpleExpressionProperty,
)
from boomi.net.transport.api_error import ApiError
from boomi.net.transport.serializer import Serializer
from boomi.net.environment.environment import Environment


# ============================================================================
# Component XML GET wall-clock deadline
# ============================================================================
#
# component_get_xml() is synchronous and FastMCP runs sync @mcp.tool functions
# inside an anyio worker thread, so there is no usable asyncio loop here — the
# deadline must be thread-based (not asyncio.wait_for). The SDK's send_request
# uses a per-socket-read timeout (60s) plus retries, so a stalled backend fetch
# is bounded (~minutes) but can exceed Cloud Run's 300s request timeout and any
# client's patience. This wraps the blocking call in a wall-clock deadline that
# raises a structured error instead of hanging.

_DEADLINE_ENV = "BOOMI_COMPONENT_GET_DEADLINE_SECONDS"
_DEADLINE_DEFAULT = 90
_DEADLINE_MIN = 1
_DEADLINE_MAX = 240


class ComponentGetDeadlineExceeded(Exception):
    """A synchronous component XML GET exceeded its wall-clock deadline.

    Carries the structured fields needed to build the error envelope so call
    sites can surface a retryable failure instead of hanging. Subclasses
    ``Exception``, so a bare ``except Exception`` still catches it (bounded, not
    a hang) — sites that want the rich envelope must catch this BEFORE the
    generic handler.
    """

    def __init__(self, component_id: str, deadline_seconds: int, elapsed_seconds: float):
        self.component_id = component_id
        self.deadline_seconds = deadline_seconds
        self.elapsed_seconds = round(elapsed_seconds, 3)
        super().__init__(
            f"Component GET for {component_id!r} exceeded {deadline_seconds}s "
            f"deadline (elapsed {self.elapsed_seconds}s)"
        )


def _component_get_deadline_seconds() -> int:
    """Read + clamp the component GET deadline from the environment.

    Default 90s, clamped inclusive to [1, 240]; empty/invalid -> default.
    Mirrors the kb/service ``_env_int`` style and the ``min(max(...))`` clamp
    idiom used elsewhere in the repo.
    """
    raw = os.getenv(_DEADLINE_ENV)
    if raw is None or raw.strip() == "":
        return _DEADLINE_DEFAULT
    try:
        value = int(raw.strip())
    except ValueError:
        print(
            f"[WARNING] {_DEADLINE_ENV}={raw!r} is not an integer; "
            f"using default {_DEADLINE_DEFAULT}"
        )
        return _DEADLINE_DEFAULT
    return min(max(value, _DEADLINE_MIN), _DEADLINE_MAX)


def _run_with_deadline(fn, component_id: str, deadline_seconds: int):
    """Run ``fn()`` on a daemon worker thread with a wall-clock deadline.

    Returns ``fn()``'s result; re-raises ``fn()``'s ORIGINAL exception (so the
    existing ``except ApiError`` / ``except Exception`` handlers behave exactly
    as before); or raises :class:`ComponentGetDeadlineExceeded` on timeout.

    The worker is a ``daemon`` thread spawned per call (no shared pool/queue).
    On timeout it is abandoned (never joined or killed) and keeps draining the
    request to its own bounded self-termination (the SDK's per-read timeout +
    retries). Being a daemon, it can never block interpreter shutdown / Cloud
    Run deploy replacement — unlike ``ThreadPoolExecutor`` workers, which are
    NOT daemon threads (verified on 3.12) and are joined at interpreter exit.
    Spawning per call (no queue) also means a timed-out GET can't leave a stale
    request queued to run later. During a sustained backend outage abandoned
    daemon threads can accumulate, but each self-terminates within the SDK
    timeout and is reaped at process exit.
    """
    box: Dict[str, Any] = {}
    done = threading.Event()

    def _worker():
        try:
            box["value"] = fn()
        except BaseException as exc:  # noqa: BLE001 — marshalled to the caller
            box["exc"] = exc
        finally:
            done.set()

    started = time.monotonic()
    threading.Thread(target=_worker, name="component-get", daemon=True).start()
    if not done.wait(timeout=deadline_seconds):
        raise ComponentGetDeadlineExceeded(
            component_id=component_id,
            deadline_seconds=deadline_seconds,
            elapsed_seconds=time.monotonic() - started,
        )
    if "exc" in box:
        raise box["exc"]
    return box["value"]


def component_get_deadline_envelope(exc: "ComponentGetDeadlineExceeded") -> Dict[str, Any]:
    """Structured failure envelope for a component-GET wall-clock timeout.

    Single-result call sites return this directly.
    """
    return {
        "_success": False,
        "error_code": "COMPONENT_GET_DEADLINE_EXCEEDED",
        "exception_type": "ComponentGetDeadlineExceeded",
        "component_id": exc.component_id,
        "deadline_seconds": exc.deadline_seconds,
        "elapsed_seconds": exc.elapsed_seconds,
        "retryable": True,
        "hint": (
            "Retry the read or narrow the request. If it repeats for this "
            "component, use metadata search/list first and inspect Cloud Run "
            "logs."
        ),
    }


def component_get_deadline_item(exc: "ComponentGetDeadlineExceeded") -> Dict[str, Any]:
    """Per-item deadline error for bulk/loop accumulators (no ``_success``)."""
    return {
        "component_id": exc.component_id,
        "error": str(exc),
        "error_code": "COMPONENT_GET_DEADLINE_EXCEEDED",
        "exception_type": "ComponentGetDeadlineExceeded",
        "deadline_seconds": exc.deadline_seconds,
        "elapsed_seconds": exc.elapsed_seconds,
        "retryable": True,
    }


def _extract_description(root) -> str:
    """Extract description from component XML child element."""
    ns = {'bns': 'http://api.platform.boomi.com/'}
    desc_elem = root.find('bns:description', ns)
    if desc_elem is not None and desc_elem.text:
        return desc_elem.text
    # Also check without namespace
    desc_elem = root.find('description')
    if desc_elem is not None and desc_elem.text:
        return desc_elem.text
    return ''


def set_description_element(root, text: str) -> None:
    """Set description as a child element (Boomi ignores description attributes)."""
    ns_uri = 'http://api.platform.boomi.com/'
    desc_elem = root.find(f'{{{ns_uri}}}description')
    if desc_elem is None:
        desc_elem = root.find('description')
    if desc_elem is None:
        # Insert after <bns:encryptedValues> if present, otherwise append
        ev = root.find(f'{{{ns_uri}}}encryptedValues')
        if ev is not None:
            idx = list(root).index(ev) + 1
            desc_elem = ET.Element(f'{{{ns_uri}}}description')
            root.insert(idx, desc_elem)
        else:
            desc_elem = ET.SubElement(root, f'{{{ns_uri}}}description')
    desc_elem.text = text


def component_get_xml(
    boomi_client: Boomi,
    component_id: str,
    deadline_seconds: Optional[int] = None,
) -> Dict[str, Any]:
    """GET component as raw XML + parsed metadata dict.

    The SDK's get_component_raw() auto-sets Accept: application/json, but Boomi's
    Component GET endpoint only supports application/xml (returns 406 otherwise).
    We use the Serializer directly with an explicit Accept header.

    ``deadline_seconds`` overrides the per-call wall-clock deadline. Multi-get
    callers (bulk/enrichment loops) pass the remaining slice of a shared
    aggregate budget so the loop can't sum past the platform request timeout;
    ``None`` reads ``BOOMI_COMPONENT_GET_DEADLINE_SECONDS``.
    """
    svc = boomi_client.component
    serialized_request = (
        Serializer(
            f"{svc.base_url or Environment.DEFAULT.url}/Component/{component_id}",
            [svc.get_access_token(), svc.get_basic_auth()],
        )
        .add_header("Accept", "application/xml")
        .serialize()
        .set_method("GET")
    )
    if deadline_seconds is None:
        deadline_seconds = _component_get_deadline_seconds()
    try:
        response, status, content = _run_with_deadline(
            lambda: svc.send_request(serialized_request),
            component_id,
            deadline_seconds,
        )
    except ComponentGetDeadlineExceeded:
        # Bounded wall-clock abort — propagate the rich exception unchanged so
        # call sites can build the COMPONENT_GET_DEADLINE_EXCEEDED envelope.
        raise
    except Exception as exc:
        raise Exception(f"GET failed: {_extract_api_error_msg(exc)}") from exc
    if status >= 400:
        # Extract a clean message from the error response body.
        # send_request() returns response.body which is a parsed dict for
        # JSON responses, raw str/bytes for XML/text.
        body_msg = ""
        if isinstance(response, dict):
            body_msg = response.get("message", "")
        elif isinstance(response, (str, bytes)):
            raw = response if isinstance(response, str) else response.decode("utf-8", errors="replace")
            try:
                import json as _json
                body_msg = _json.loads(raw).get("message", "")
            except Exception:
                body_msg = raw.split("\n")[0][:200] if raw else ""
        raise Exception(f"GET failed (HTTP {status}): {body_msg}" if body_msg else f"GET failed: HTTP {status}")

    raw_xml = response if isinstance(response, str) else response.decode('utf-8')
    root = ET.fromstring(raw_xml)

    return {
        'component_id': root.attrib.get('componentId', component_id),
        'id': root.attrib.get('componentId', ''),
        'name': root.attrib.get('name', ''),
        'folder_name': root.attrib.get('folderName', ''),
        'folder_id': root.attrib.get('folderId', ''),
        'folder_full_path': root.attrib.get('folderFullPath', ''),
        'type': root.attrib.get('type', ''),
        'version': int(root.attrib.get('version', 0)),
        'description': _extract_description(root),
        'xml': raw_xml,
    }


def parse_component_xml(raw_xml: str, fallback_id: str = '') -> Dict[str, Any]:
    """Parse component XML string into metadata dict (no 'xml' key - lighter)."""
    root = ET.fromstring(raw_xml)
    return {
        'component_id': root.attrib.get('componentId', fallback_id),
        'id': root.attrib.get('componentId', fallback_id),
        'name': root.attrib.get('name', ''),
        'folder_name': root.attrib.get('folderName', ''),
        'folder_id': root.attrib.get('folderId', ''),
        'folder_full_path': root.attrib.get('folderFullPath', ''),
        'type': root.attrib.get('type', ''),
        'version': int(root.attrib.get('version', 0)),
        'description': _extract_description(root),
        'current_version': root.attrib.get('currentVersion', 'false'),
        'deleted': root.attrib.get('deleted', 'false'),
        'created_date': root.attrib.get('createdDate', ''),
        'modified_date': root.attrib.get('modifiedDate', ''),
        'created_by': root.attrib.get('createdBy', ''),
        'modified_by': root.attrib.get('modifiedBy', ''),
    }


def parse_bulk_response(raw_xml: str) -> List[Dict[str, Any]]:
    """Parse bulk component XML response.

    The SDK's bulk_component_raw() returns XML like:
    <bns:BulkIdProcessingResponse><bns:response><bns:Result>...</bns:Result></bns:response>...
    Each <bns:Result> contains a full component XML document.
    """
    components = []
    root = ET.fromstring(raw_xml)

    # Handle namespace
    ns = {'bns': 'http://api.platform.boomi.com/'}

    for response_elem in root.findall('.//bns:response', ns):
        status_code = response_elem.get('statusCode', '200')
        result_elem = response_elem.find('bns:Result', ns)
        if result_elem is not None and status_code.startswith('2'):
            # Re-serialize the Result element contents
            inner_xml = ET.tostring(result_elem, encoding='unicode')
            try:
                comp = parse_component_xml(inner_xml)
                components.append(comp)
            except ET.ParseError:
                # Fallback: try children of Result
                for child in result_elem:
                    child_xml = ET.tostring(child, encoding='unicode')
                    try:
                        comp = parse_component_xml(child_xml)
                        components.append(comp)
                    except ET.ParseError:
                        pass
        elif status_code and not status_code.startswith('2'):
            error_msg = response_elem.get('errorMessage', f'HTTP {status_code}')
            comp_id = response_elem.get('id', '')
            components.append({
                'component_id': comp_id,
                'error': error_msg,
                'status_code': status_code,
            })

    return components


# ============================================================================
# Pagination helpers for component metadata queries
# ============================================================================

def paginate_metadata(boomi_client: Boomi, query_config, show_all: bool = False, limit: int = 0) -> List[Dict[str, Any]]:
    """Execute a metadata query with pagination. Returns list of component dicts.

    When limit > 0, stops collecting after reaching the cap (applied after filtering).
    """
    result = boomi_client.component_metadata.query_component_metadata(
        request_body=query_config
    )

    components = []
    if hasattr(result, 'result') and result.result:
        for comp in result.result:
            components.append(metadata_to_dict(comp))

    # Paginate
    while hasattr(result, 'query_token') and result.query_token:
        result = boomi_client.component_metadata.query_more_component_metadata(
            request_body=result.query_token
        )
        if hasattr(result, 'result') and result.result:
            for comp in result.result:
                components.append(metadata_to_dict(comp))

    # Client-side filter: current version, not deleted (unless show_all)
    if not show_all:
        components = [
            c for c in components
            if str(c.get('current_version', 'false')).lower() == 'true'
            and str(c.get('deleted', 'true')).lower() == 'false'
        ]

    # Apply limit after filtering
    if limit > 0 and len(components) > limit:
        components = components[:limit]

    return components


def metadata_to_dict(comp) -> Dict[str, Any]:
    """Convert a ComponentMetadata SDK object to a plain dict."""
    return {
        'component_id': getattr(comp, 'component_id', ''),
        'id': getattr(comp, 'component_id', '') or getattr(comp, 'id_', ''),
        'name': getattr(comp, 'name', ''),
        'folder_name': getattr(comp, 'folder_name', ''),
        'type': getattr(comp, 'type_', ''),
        'version': getattr(comp, 'version', ''),
        'current_version': str(getattr(comp, 'current_version', 'false')).lower() == 'true',
        'deleted': str(getattr(comp, 'deleted', 'false')).lower() == 'true',
        'created_date': getattr(comp, 'created_date', ''),
        'modified_date': getattr(comp, 'modified_date', ''),
        'created_by': getattr(comp, 'created_by', ''),
        'modified_by': getattr(comp, 'modified_by', ''),
    }


# ============================================================================
# Soft-delete helper
# ============================================================================

def _create_component_raw(boomi_client: Boomi, xml: str) -> Dict[str, Any]:
    """Create a component via raw POST, returning parsed XML response.

    The SDK's create_component() fails to parse GenericConnectionConfig responses,
    so we use the Serializer directly (same approach as component_get_xml).
    """
    svc = boomi_client.component
    serialized_request = (
        Serializer(
            f"{svc.base_url or Environment.DEFAULT.url}/Component",
            [svc.get_access_token(), svc.get_basic_auth()],
        )
        .add_header("Accept", "application/xml")
        .add_header("Content-Type", "application/xml")
        .serialize()
        .set_method("POST")
    )
    serialized_request.body = xml.encode('utf-8') if isinstance(xml, str) else xml
    response, status, content = svc.send_request(serialized_request)

    if status >= 400:
        raw = response if isinstance(response, str) else response.decode('utf-8')
        raise Exception(f"Create failed: HTTP {status} — {raw}")

    raw_xml = response if isinstance(response, str) else response.decode('utf-8')
    root = ET.fromstring(raw_xml)

    return {
        'component_id': root.attrib.get('componentId', ''),
        'name': root.attrib.get('name', ''),
        'type': root.attrib.get('type', ''),
        'sub_type': root.attrib.get('subType', ''),
        'folder_name': root.attrib.get('folderName', ''),
        'version': root.attrib.get('version', ''),
    }


def _extract_api_error_msg(e) -> str:
    """Extract user-friendly error message from ApiError."""
    detail = getattr(e, "error_detail", None)
    if detail:
        return detail
    resp = getattr(e, "response", None)
    if resp:
        body = getattr(resp, "body", None)
        if isinstance(body, dict):
            msg = body.get("message", "")
            if msg:
                return msg
    return getattr(e, "message", "") or str(e)


def soft_delete_component(boomi_client: Boomi, component_id: str) -> Dict[str, Any]:
    """Delete a component via the metadata API.

    The XML soft-delete (setting deleted=true via PUT) is silently ignored by
    Boomi's API. The metadata delete is the only reliable method.
    """
    current = component_get_xml(boomi_client, component_id)
    boomi_client.component_metadata.delete_component_metadata(id_=component_id)
    return {
        "component_name": current['name'],
        "component_id": component_id,
        "method": "metadata_delete",
    }
