"""
Shared helpers for component tools.

Provides XML-based component retrieval and parsing used across
query_components, manage_component, and analyze_component modules.
"""

from typing import Dict, Any, List
import xml.etree.ElementTree as ET

from boomi import Boomi
from boomi.net.transport.serializer import Serializer
from boomi.net.environment.environment import Environment


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
        'type': root.attrib.get('type', ''),
        'version': root.attrib.get('version', ''),
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
        'type': root.attrib.get('type', ''),
        'version': root.attrib.get('version', ''),
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
