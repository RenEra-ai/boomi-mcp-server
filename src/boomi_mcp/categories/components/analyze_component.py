"""
Component Analysis MCP Tools for Boomi API Integration.

Provides component dependency analysis, version comparison, and merge:
- where_used: Find all components that reference a given component (inbound)
- dependencies: Find all components that a given component references (outbound)
- compare_versions: Compare two versions of a component to see changes
- merge: Merge component versions across branches
"""

from typing import Dict, Any, List, Optional
import time
import xml.etree.ElementTree as ET
import difflib

from boomi import Boomi
from boomi.models import (
    ComponentReferenceQueryConfig,
    ComponentReferenceQueryConfigQueryFilter,
    ComponentReferenceSimpleExpression,
    ComponentReferenceSimpleExpressionOperator,
    ComponentReferenceSimpleExpressionProperty,
    ComponentReferenceGroupingExpression,
    ComponentReferenceGroupingExpressionOperator,
    ComponentDiffRequest,
    ComponentDiffResponse,
)

from boomi.net.transport.api_error import ApiError
from ._shared import (
    component_get_xml,
    _extract_api_error_msg,
    ComponentGetDeadlineExceeded,
    component_get_deadline_envelope,
    _component_get_deadline_seconds,
)


# ============================================================================
# Helper: paginate component reference queries
# ============================================================================

def _paginate_references(boomi_client: Boomi, query_config) -> List[Dict[str, Any]]:
    """Execute a component reference query with pagination."""
    result = boomi_client.component_reference.query_component_reference(
        request_body=query_config
    )

    references = []
    if hasattr(result, 'result') and result.result:
        for comp_ref in result.result:
            if hasattr(comp_ref, 'references') and comp_ref.references:
                for ref in comp_ref.references:
                    references.append({
                        'parent_component_id': getattr(ref, 'parent_component_id', ''),
                        'parent_version': getattr(ref, 'parent_version', ''),
                        'component_id': getattr(ref, 'component_id', ''),
                        'type': getattr(ref, 'type_', ''),
                    })

    # Paginate
    while hasattr(result, 'query_token') and result.query_token:
        result = boomi_client.component_reference.query_more_component_reference(
            request_body=result.query_token
        )
        if hasattr(result, 'result') and result.result:
            for comp_ref in result.result:
                if hasattr(comp_ref, 'references') and comp_ref.references:
                    for ref in comp_ref.references:
                        references.append({
                            'parent_component_id': getattr(ref, 'parent_component_id', ''),
                            'parent_version': getattr(ref, 'parent_version', ''),
                            'component_id': getattr(ref, 'component_id', ''),
                            'type': getattr(ref, 'type_', ''),
                        })

    return references


def _enrich_references(boomi_client: Boomi, references: List[Dict], id_key: str) -> List[Dict]:
    """Enrich references with component metadata by fetching each component.

    Best-effort, bounded by an aggregate wall-clock budget: a long reference
    list whose reads stall must not sum past the platform request timeout. Once
    the budget is spent, remaining refs are left un-enriched and flagged rather
    than fetched.
    """
    enriched = []
    budget = float(_component_get_deadline_seconds())
    for ref in references:
        comp_id = ref.get(id_key, '')
        if comp_id and budget >= 1:
            started = time.monotonic()
            try:
                meta = component_get_xml(boomi_client, comp_id, deadline_seconds=int(budget))
                ref['name'] = meta.get('name', '')
                ref['component_type'] = meta.get('type', '')
                ref['folder_name'] = meta.get('folder_name', '')
            except ComponentGetDeadlineExceeded:
                # Bounded timeout enriching this one ref — flag it (don't vanish
                # it) and keep enriching the rest until the budget is spent.
                ref['name'] = ''
                ref['component_type'] = ''
                ref['folder_name'] = ''
                ref['_enrichment_error'] = 'COMPONENT_GET_DEADLINE_EXCEEDED'
            except Exception:
                ref['name'] = ''
                ref['component_type'] = ''
                ref['folder_name'] = ''
            finally:
                budget -= time.monotonic() - started
        elif comp_id:
            # Aggregate budget exhausted — leave un-enriched, flagged, without
            # starting another (possibly stalling) read.
            ref['name'] = ''
            ref['component_type'] = ''
            ref['folder_name'] = ''
            ref['_enrichment_error'] = 'COMPONENT_GET_DEADLINE_BUDGET_EXHAUSTED'
        enriched.append(ref)
    return enriched


# ============================================================================
# Actions
# ============================================================================

def where_used(
    boomi_client: Boomi,
    profile: str,
    component_id: str,
    filters: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Find all components that reference a given component (inbound references)."""
    try:
        expressions = [
            ComponentReferenceSimpleExpression(
                operator=ComponentReferenceSimpleExpressionOperator.EQUALS,
                property=ComponentReferenceSimpleExpressionProperty.COMPONENTID,
                argument=[component_id]
            )
        ]

        # Optional type filter
        if filters and filters.get('type'):
            expressions.append(ComponentReferenceSimpleExpression(
                operator=ComponentReferenceSimpleExpressionOperator.EQUALS,
                property=ComponentReferenceSimpleExpressionProperty.TYPE,
                argument=[filters['type']]
            ))

        if len(expressions) == 1:
            root_expr = expressions[0]
        else:
            root_expr = ComponentReferenceGroupingExpression(
                operator=ComponentReferenceGroupingExpressionOperator.AND,
                nested_expression=expressions
            )

        query_filter = ComponentReferenceQueryConfigQueryFilter(expression=root_expr)
        query_config = ComponentReferenceQueryConfig(query_filter=query_filter)

        references = _paginate_references(boomi_client, query_config)

        # Enrich parent components with metadata
        references = _enrich_references(boomi_client, references, 'parent_component_id')

        # Count by type
        type_counts = {}
        for ref in references:
            ref_type = ref.get('type', 'Unknown')
            type_counts[ref_type] = type_counts.get(ref_type, 0) + 1

        return {
            "_success": True,
            "component_id": component_id,
            "total_references": len(references),
            "references": references,
            "type_summary": type_counts,
            "profile": profile,
            "note": "Shows immediate references only (one level, not recursive)",
        }

    except ApiError as e:
        return {
            "_success": False,
            "error": f"Failed to find where component '{component_id}' is used: {_extract_api_error_msg(e)}",
            "exception_type": "ApiError",
        }
    except Exception as e:
        return {
            "_success": False,
            "error": f"Failed to find where component '{component_id}' is used: {str(e)}",
            "exception_type": type(e).__name__,
        }


def find_dependencies(
    boomi_client: Boomi,
    profile: str,
    component_id: str,
    filters: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Find all components that a given component references (outbound dependencies)."""
    try:
        # Get component version first (required for parentVersion queries)
        comp_meta = component_get_xml(boomi_client, component_id)
        version = comp_meta.get('version', '')

        expressions = [
            ComponentReferenceSimpleExpression(
                operator=ComponentReferenceSimpleExpressionOperator.EQUALS,
                property=ComponentReferenceSimpleExpressionProperty.PARENTCOMPONENTID,
                argument=[component_id]
            )
        ]

        # Add parentVersion if available
        if version:
            expressions.append(ComponentReferenceSimpleExpression(
                operator=ComponentReferenceSimpleExpressionOperator.EQUALS,
                property=ComponentReferenceSimpleExpressionProperty.PARENTVERSION,
                argument=[str(version)]
            ))

        # Optional type filter (same pattern as where_used)
        if filters and filters.get('type'):
            expressions.append(ComponentReferenceSimpleExpression(
                operator=ComponentReferenceSimpleExpressionOperator.EQUALS,
                property=ComponentReferenceSimpleExpressionProperty.TYPE,
                argument=[filters['type']]
            ))

        if len(expressions) == 1:
            root_expr = expressions[0]
        else:
            root_expr = ComponentReferenceGroupingExpression(
                operator=ComponentReferenceGroupingExpressionOperator.AND,
                nested_expression=expressions
            )

        query_filter = ComponentReferenceQueryConfigQueryFilter(expression=root_expr)
        query_config = ComponentReferenceQueryConfig(query_filter=query_filter)

        references = _paginate_references(boomi_client, query_config)

        # Enrich child components with metadata
        references = _enrich_references(boomi_client, references, 'component_id')

        # Count by type
        type_counts = {}
        for ref in references:
            ref_type = ref.get('type', 'Unknown')
            type_counts[ref_type] = type_counts.get(ref_type, 0) + 1

        return {
            "_success": True,
            "component_id": component_id,
            "component_name": comp_meta.get('name', ''),
            "component_version": version,
            "total_dependencies": len(references),
            "dependencies": references,
            "type_summary": type_counts,
            "profile": profile,
            "note": "Shows immediate dependencies only (one level, not recursive)",
        }

    except ComponentGetDeadlineExceeded as e:
        return component_get_deadline_envelope(e)
    except ApiError as e:
        return {
            "_success": False,
            "error": f"Failed to find dependencies for '{component_id}': {_extract_api_error_msg(e)}",
            "exception_type": "ApiError",
        }
    except Exception as e:
        return {
            "_success": False,
            "error": f"Failed to find dependencies for '{component_id}': {str(e)}",
            "exception_type": type(e).__name__,
        }


def compare_versions(
    boomi_client: Boomi,
    profile: str,
    component_id: str,
    config: Dict[str, Any]
) -> Dict[str, Any]:
    """Compare two versions of a component to see what changed."""
    try:
        source_version = config.get('source_version')
        target_version = config.get('target_version')

        if source_version is None or target_version is None:
            return {
                "_success": False,
                "error": "source_version and target_version are required in config",
                "hint": 'Provide config: {"source_version": 1, "target_version": 2}',
            }

        diff_request = ComponentDiffRequest(
            component_id=component_id,
            source_version=int(source_version),
            target_version=int(target_version)
        )

        result = boomi_client.component_diff_request.create_component_diff_request(
            diff_request
        )

        # Parse the diff response
        diff_data = _parse_diff_response(result)

        return {
            "_success": True,
            "component_id": component_id,
            "source_version": int(source_version),
            "target_version": int(target_version),
            "diff": diff_data,
            "profile": profile,
        }

    except ApiError as e:
        return {
            "_success": False,
            "error": f"Failed to compare versions for '{component_id}': {_extract_api_error_msg(e)}",
            "exception_type": "ApiError",
            "hint": "Ensure both version numbers exist for this component",
        }
    except Exception as e:
        return {
            "_success": False,
            "error": f"Failed to compare versions for '{component_id}': {str(e)}",
            "exception_type": type(e).__name__,
            "hint": "Ensure both version numbers exist for this component",
        }


def merge_versions(
    boomi_client: Boomi,
    profile: str,
    component_id: str,
    config: Dict[str, Any]
) -> Dict[str, Any]:
    """Merge component content across branches or versions.

    Supports two modes:
    - Branch merge: source_branch + target_branch (uses componentId~branchId API format)
    - Version merge: source_version + optional target_version (legacy behavior)

    In both modes, source content is applied onto target, preserving target's
    version metadata. The update targets the target branch/version.
    """
    try:
        source_branch = config.get('source_branch')
        target_branch = config.get('target_branch')
        source_version = config.get('source_version')
        target_version = config.get('target_version')

        if source_branch is None and source_version is None:
            return {
                "_success": False,
                "error": "Either source_branch or source_version is required in config",
                "hint": 'Branch merge: {"source_branch": "dev-branch-id", "target_branch": "main-branch-id"}\n'
                        'Version merge: {"source_version": 1, "target_version": 2}',
            }

        # Determine source/target component IDs for GET requests
        if source_branch:
            # Branch merge mode: componentId~branchId
            source_get_id = f"{component_id}~{source_branch}"
            target_get_id = f"{component_id}~{target_branch}" if target_branch else component_id
            source_label = f"branch {source_branch}"
            target_label = f"branch {target_branch}" if target_branch else "main branch"
        else:
            # Version merge mode: componentId~version
            source_get_id = f"{component_id}~{source_version}"
            target_get_id = f"{component_id}~{target_version}" if target_version is not None else component_id
            source_label = f"version {source_version}"
            target_label = f"version {target_version}" if target_version is not None else "current"

        # Get source XML
        source_meta = component_get_xml(boomi_client, source_get_id)
        source_xml = source_meta['xml']
        source_root = ET.fromstring(source_xml)

        # Get target XML
        target_meta = component_get_xml(boomi_client, target_get_id)
        target_xml = target_meta['xml']
        target_root = ET.fromstring(target_xml)

        # Compare source and target
        source_lines = source_xml.splitlines(keepends=True)
        target_lines = target_xml.splitlines(keepends=True)
        diff = list(difflib.unified_diff(
            target_lines, source_lines,
            fromfile=target_label,
            tofile=source_label,
            lineterm=""
        ))

        # Apply source content onto target: preserve target's version metadata
        target_version_attr = target_root.attrib.get('version', '')
        target_current_version = target_root.attrib.get('currentVersion', '')

        merged_root = ET.fromstring(source_xml)
        if target_version_attr:
            merged_root.set('version', target_version_attr)
        if target_current_version:
            merged_root.set('currentVersion', target_current_version)

        # Always strip branchId from source XML to prevent writing to wrong branch,
        # then explicitly set it only if targeting a specific branch
        if 'branchId' in merged_root.attrib:
            del merged_root.attrib['branchId']
        if target_branch:
            merged_root.set('branchId', target_branch)

        merged_xml = ET.tostring(merged_root, encoding='unicode')

        # Perform the update (target branch context)
        boomi_client.component.update_component_raw(component_id, merged_xml)

        # Verify the update (always read the updated head, not an immutable version snapshot)
        verify_id = f"{component_id}~{target_branch}" if target_branch else component_id
        verify = component_get_xml(boomi_client, verify_id)

        result = {
            "_success": True,
            "component_id": component_id,
            "component_name": target_meta.get('name', ''),
            "source": source_label,
            "target": target_label,
            "new_version": verify.get('version', ''),
            "diff_lines": len(diff),
            "diff_preview": ''.join(diff[:50]) if diff else "No differences found",
            "profile": profile,
            "note": f"Source ({source_label}) content merged onto target ({target_label}). Component version incremented.",
        }

        if source_branch:
            result["source_branch"] = source_branch
            result["target_branch"] = target_branch or "main"

        return result

    except ComponentGetDeadlineExceeded as e:
        return component_get_deadline_envelope(e)
    except ApiError as e:
        return {
            "_success": False,
            "error": f"Failed to merge component '{component_id}': {_extract_api_error_msg(e)}",
            "exception_type": "ApiError",
            "hint": "Ensure the component exists on both branches/versions. "
                    "Use query_components to find component IDs and manage_account list_branches for branch IDs.",
        }
    except Exception as e:
        return {
            "_success": False,
            "error": f"Failed to merge component '{component_id}': {str(e)}",
            "exception_type": type(e).__name__,
            "hint": "Ensure the component exists on both branches/versions. "
                    "Use query_components to find component IDs and manage_account list_branches for branch IDs.",
        }


# ============================================================================
# Helpers
# ============================================================================

def _change_value_to_plain(val):
    """Convert a ChangeValue SDK object (or plain string) to plain data."""
    if val is None:
        return ''
    if isinstance(val, str):
        return val
    # ChangeValue has xpath and/or value attributes (SDK sentinel pattern
    # means either may be absent on the instance when not provided)
    has_value = hasattr(val, 'value')
    has_xpath = hasattr(val, 'xpath')
    if has_value or has_xpath:
        result = {'value': getattr(val, 'value', '')}
        xpath = getattr(val, 'xpath', None)
        if xpath:
            result['xpath'] = xpath
        return result
    return str(val)


def _element_key_to_dict(ek):
    """Convert an element_key SDK object to a plain dict."""
    if ek is None or not hasattr(ek, 'element_name'):
        return None
    result = {'element_name': getattr(ek, 'element_name', '')}
    # Use key_parts (plural) to capture all entries, not just the first
    parts = getattr(ek, 'key_parts', None) or []
    if parts:
        serialized = [
            {
                'attribute': getattr(kp, 'attribute', ''),
                'value': getattr(kp, 'value', ''),
            }
            for kp in parts
            if hasattr(kp, 'attribute')
        ]
    else:
        # Fallback to singular key_part
        kp = getattr(ek, 'key_part', None)
        if kp and hasattr(kp, 'attribute'):
            serialized = [{
                'attribute': getattr(kp, 'attribute', ''),
                'value': getattr(kp, 'value', ''),
            }]
        else:
            serialized = []
    if serialized:
        # Always emit key_part (first entry) for backward compat
        result['key_part'] = serialized[0]
        # Also emit key_parts (full list) when composite
        if len(serialized) > 1:
            result['key_parts'] = serialized
    return result


def _parse_diff_response(result) -> Dict[str, Any]:
    """Parse ComponentDiffResponseCreate into a plain dict."""
    diff_data = {
        'message': '',
        'additions': [],
        'deletions': [],
        'modifications': [],
        'summary': {},
    }

    cdr = getattr(result, 'component_diff_response', None)
    if not cdr:
        raw = getattr(result, '_kwargs', {})
        if raw:
            cdr = ComponentDiffResponse._unmap(raw)
    if not cdr:
        diff_data['message'] = 'No diff response returned'
        return diff_data

    diff_data['message'] = getattr(cdr, 'message', '')
    generic_diff = getattr(cdr, 'generic_diff', None)
    if not generic_diff:
        return diff_data

    # Additions
    addition = getattr(generic_diff, 'addition', None)
    if addition:
        diff_data['summary']['additions'] = getattr(addition, 'total', 0)
        changes = getattr(addition, 'change', []) or []
        for c in changes:
            entry = {
                'type': getattr(c, 'type_', ''),
                'changed_particle_name': getattr(c, 'changed_particle_name', ''),
                'new_value': _change_value_to_plain(getattr(c, 'new_value', '')),
            }
            ek = _element_key_to_dict(getattr(c, 'element_key', None))
            if ek:
                entry['element_key'] = ek
            diff_data['additions'].append(entry)

    # Deletions
    deletion = getattr(generic_diff, 'deletion', None)
    if deletion:
        diff_data['summary']['deletions'] = getattr(deletion, 'total', 0)
        changes = getattr(deletion, 'change', []) or []
        for c in changes:
            entry = {
                'type': getattr(c, 'type_', ''),
                'changed_particle_name': getattr(c, 'changed_particle_name', ''),
                'old_value': _change_value_to_plain(getattr(c, 'old_value', '')),
            }
            ek = _element_key_to_dict(getattr(c, 'element_key', None))
            if ek:
                entry['element_key'] = ek
            diff_data['deletions'].append(entry)

    # Modifications
    modification = getattr(generic_diff, 'modification', None)
    if modification:
        diff_data['summary']['modifications'] = getattr(modification, 'total', 0)
        changes = getattr(modification, 'change', []) or []
        for c in changes:
            entry = {
                'type': getattr(c, 'type_', ''),
                'changed_particle_name': getattr(c, 'changed_particle_name', ''),
                'old_value': _change_value_to_plain(getattr(c, 'old_value', '')),
                'new_value': _change_value_to_plain(getattr(c, 'new_value', '')),
            }
            ek = _element_key_to_dict(getattr(c, 'element_key', None))
            if ek:
                entry['element_key'] = ek
            diff_data['modifications'].append(entry)

    return diff_data


# ============================================================================
# Action Router
# ============================================================================

def analyze_component_action(
    boomi_client: Boomi,
    profile: str,
    action: str,
    **params
) -> Dict[str, Any]:
    """Route analyze_component actions."""
    try:
        if action == "where_used":
            component_id = params.get("component_id")
            if not component_id:
                return {
                    "_success": False,
                    "error": "component_id is required for 'where_used' action",
                }
            filters = params.get("filters")
            return where_used(boomi_client, profile, component_id, filters)

        elif action == "dependencies":
            component_id = params.get("component_id")
            if not component_id:
                return {
                    "_success": False,
                    "error": "component_id is required for 'dependencies' action",
                }
            filters = params.get("filters")
            return find_dependencies(boomi_client, profile, component_id, filters)

        elif action == "compare_versions":
            component_id = params.get("component_id")
            if not component_id:
                return {
                    "_success": False,
                    "error": "component_id is required for 'compare_versions' action",
                }
            config = params.get("config", {})
            if not config:
                return {
                    "_success": False,
                    "error": "config with source_version and target_version is required",
                    "hint": 'Provide config: {"source_version": 1, "target_version": 2}',
                }
            return compare_versions(boomi_client, profile, component_id, config)

        elif action == "merge":
            component_id = params.get("component_id")
            if not component_id:
                return {
                    "_success": False,
                    "error": "component_id is required for 'merge' action",
                }
            config = params.get("config", {})
            if not config:
                return {
                    "_success": False,
                    "error": "config with source_branch or source_version is required",
                    "hint": 'Branch merge: {"source_branch": "branch-id", "target_branch": "branch-id"}\n'
                            'Version merge: {"source_version": 1, "target_version": 2}',
                }
            return merge_versions(boomi_client, profile, component_id, config)

        else:
            return {
                "_success": False,
                "error": f"Unknown action: {action}",
                "hint": "Valid actions are: where_used, dependencies, compare_versions, merge",
            }

    except ApiError as e:
        return {
            "_success": False,
            "error": f"Action '{action}' failed: {_extract_api_error_msg(e)}",
            "exception_type": "ApiError",
        }
    except Exception as e:
        return {
            "_success": False,
            "error": f"Action '{action}' failed: {str(e)}",
            "exception_type": type(e).__name__,
        }


__all__ = ['analyze_component_action']
