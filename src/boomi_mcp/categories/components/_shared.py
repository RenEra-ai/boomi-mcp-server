"""
Shared helpers for component tools.

Provides XML-based component retrieval and parsing used across
query_components, manage_component, and analyze_component modules.
"""

from typing import Dict, Any, List
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


def component_get_xml(boomi_client: Boomi, component_id: str) -> Dict[str, Any]:
    """GET component as raw XML + parsed metadata dict.

    The SDK's get_component_raw() auto-sets Accept: application/json, but Boomi's
    Component GET endpoint only supports application/xml (returns 406 otherwise).
    We use the Serializer directly with an explicit Accept header.
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
    response, status, content = svc.send_request(serialized_request)
    if status >= 400:
        raise Exception(f"GET failed: HTTP {status} — {response}")

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
        'version': root.attrib.get('version', ''),
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
        'version': root.attrib.get('version', ''),
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

def paginate_metadata(boomi_client: Boomi, query_config, show_all: bool = False) -> List[Dict[str, Any]]:
    """Execute a metadata query with pagination. Returns list of component dicts."""
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

    return components


def metadata_to_dict(comp) -> Dict[str, Any]:
    """Convert a ComponentMetadata SDK object to a plain dict."""
    return {
        'component_id': getattr(comp, 'component_id', ''),
        'id': getattr(comp, 'id_', ''),
        'name': getattr(comp, 'name', ''),
        'folder_name': getattr(comp, 'folder_name', ''),
        'type': getattr(comp, 'type_', ''),
        'version': getattr(comp, 'version', ''),
        'current_version': str(getattr(comp, 'current_version', 'false')),
        'deleted': str(getattr(comp, 'deleted', 'false')),
        'created_date': getattr(comp, 'created_date', ''),
        'modified_date': getattr(comp, 'modified_date', ''),
        'created_by': getattr(comp, 'created_by', ''),
        'modified_by': getattr(comp, 'modified_by', ''),
        'folder_full_path': getattr(comp, 'folder_full_path', ''),
    }


# ============================================================================
# Soft-delete helper
# ============================================================================

def soft_delete_component(boomi_client: Boomi, component_id: str) -> Dict[str, Any]:
    """Soft-delete a component (mark deleted=true via XML update, with metadata fallback).

    Primary: soft-delete via XML (safe, reversible) per SDK examples.
    Fallback: metadata API delete if soft-delete fails.
    """
    try:
        current = component_get_xml(boomi_client, component_id)
        raw_xml = current['xml']
        root = ET.fromstring(raw_xml)
        root.set('deleted', 'true')
        modified_xml = ET.tostring(root, encoding='unicode')
        boomi_client.component.update_component_raw(component_id, modified_xml)

        result = {
            "component_name": current['name'],
            "component_id": component_id,
            "method": "soft_delete",
        }
        # Verify deletion took effect
        try:
            verify = component_get_xml(boomi_client, component_id)
            verify_root = ET.fromstring(verify['xml'])
            if verify_root.attrib.get('deleted', 'false').lower() != 'true':
                result["verify_warning"] = "deleted flag may not have been applied"
        except Exception:
            pass

        return result

    except ApiError as e:
        error_msg = str(e)
        status = getattr(e, 'status', None)
        if status and 400 <= status < 600 and status != 408:
            try:
                boomi_client.component_metadata.delete_component_metadata(id_=component_id)
                return {
                    "component_name": component_id,
                    "component_id": component_id,
                    "method": "metadata_delete",
                }
            except Exception as e2:
                raise Exception(
                    f"Soft-delete failed: {error_msg}. Metadata delete also failed: {str(e2)}"
                ) from e
        raise

    except Exception:
        raise
