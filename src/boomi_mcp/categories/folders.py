"""
Folder Management MCP Tools for Boomi Platform.

Provides 7 folder management actions:
- list: List all folders with optional tree view and filters
- get: Get single folder by ID
- create: Create folder or folder hierarchy from path
- move: Move a component to a different folder
- delete: Delete an empty folder
- restore: Restore a deleted folder
- contents: List components and sub-folders in a folder
"""

from typing import Dict, Any, Optional, List

from boomi import Boomi
from boomi.models import (
    Folder,
    FolderQueryConfig,
    FolderQueryConfigQueryFilter,
    FolderSimpleExpression,
    FolderSimpleExpressionOperator,
    FolderSimpleExpressionProperty,
    ComponentMetadataQueryConfig,
    ComponentMetadataQueryConfigQueryFilter,
    ComponentMetadataSimpleExpression,
    ComponentMetadataSimpleExpressionOperator,
    ComponentMetadataSimpleExpressionProperty,
)


# ============================================================================
# Helpers
# ============================================================================

def _folder_to_dict(folder) -> Dict[str, Any]:
    """Convert SDK Folder object to plain dict."""
    return {
        "id": getattr(folder, 'id_', ''),
        "name": getattr(folder, 'name', ''),
        "full_path": getattr(folder, 'full_path', ''),
        "parent_id": getattr(folder, 'parent_id', ''),
        "parent_name": getattr(folder, 'parent_name', ''),
        "deleted": getattr(folder, 'deleted', False),
    }


def _query_all_folders(sdk: Boomi, include_deleted: bool = False) -> List[Dict[str, Any]]:
    """Query all folders with pagination."""
    if include_deleted:
        expression = FolderSimpleExpression(
            operator=FolderSimpleExpressionOperator.ISNOTNULL,
            property=FolderSimpleExpressionProperty.ID,
            argument=[]
        )
    else:
        expression = FolderSimpleExpression(
            operator=FolderSimpleExpressionOperator.EQUALS,
            property=FolderSimpleExpressionProperty.DELETED,
            argument=["false"]
        )

    query_filter = FolderQueryConfigQueryFilter(expression=expression)
    query_config = FolderQueryConfig(query_filter=query_filter)
    result = sdk.folder.query_folder(request_body=query_config)

    folders = []
    if hasattr(result, 'result') and result.result:
        folders.extend([_folder_to_dict(f) for f in result.result])

    while hasattr(result, 'query_token') and result.query_token:
        result = sdk.folder.query_more_folder(request_body=result.query_token)
        if hasattr(result, 'result') and result.result:
            folders.extend([_folder_to_dict(f) for f in result.result])

    return folders


def _find_folder_by_name_and_parent(sdk: Boomi, name: str, parent_id: Optional[str]) -> Optional[Dict[str, Any]]:
    """Find a non-deleted folder by name and parent ID.

    Queries by name, then filters client-side to match the specific parent.
    When parent_id is None (top-level), matches folders whose full_path has
    exactly one slash (account_root/folder_name), since Boomi auto-assigns
    new top-level folders to the account root folder.
    """
    expression = FolderSimpleExpression(
        operator=FolderSimpleExpressionOperator.EQUALS,
        property=FolderSimpleExpressionProperty.NAME,
        argument=[name]
    )
    query_filter = FolderQueryConfigQueryFilter(expression=expression)
    query_config = FolderQueryConfig(query_filter=query_filter)
    result = sdk.folder.query_folder(request_body=query_config)

    if hasattr(result, 'result') and result.result:
        for f in result.result:
            if getattr(f, 'deleted', False):
                continue
            if parent_id:
                # Explicit parent: match by parent_id
                if getattr(f, 'parent_id', '') == parent_id:
                    return _folder_to_dict(f)
            else:
                # No parent specified: match top-level folders
                # Top-level folders have full_path like "AccountRoot/FolderName" (one slash)
                full_path = getattr(f, 'full_path', '')
                if full_path.count('/') == 1 and full_path.endswith('/' + name):
                    return _folder_to_dict(f)
    return None


def _find_folder_by_name(sdk: Boomi, name: str) -> Optional[Dict[str, Any]]:
    """Find a non-deleted folder by name (returns first match)."""
    expression = FolderSimpleExpression(
        operator=FolderSimpleExpressionOperator.EQUALS,
        property=FolderSimpleExpressionProperty.NAME,
        argument=[name]
    )
    query_filter = FolderQueryConfigQueryFilter(expression=expression)
    query_config = FolderQueryConfig(query_filter=query_filter)
    result = sdk.folder.query_folder(request_body=query_config)

    if hasattr(result, 'result') and result.result:
        for f in result.result:
            if not getattr(f, 'deleted', False):
                return _folder_to_dict(f)
    return None


def _build_tree_string(folders: List[Dict[str, Any]]) -> str:
    """Build an ASCII tree representation of the folder hierarchy."""
    # Build children map: parent_id -> [child folders]
    children_map: Dict[str, List[Dict[str, Any]]] = {}
    for f in folders:
        if f.get("deleted"):
            continue
        parent = f.get("parent_id") or "ROOT"
        children_map.setdefault(parent, []).append(f)

    # Sort children by name
    for children in children_map.values():
        children.sort(key=lambda x: x.get("name", "").lower())

    lines = []
    visited = set()

    def _render(parent_id: str, prefix: str):
        children = children_map.get(parent_id, [])
        for i, child in enumerate(children):
            cid = child["id"]
            if cid in visited:
                continue
            visited.add(cid)

            is_last = (i == len(children) - 1)
            connector = "\u2514\u2500\u2500 " if is_last else "\u251c\u2500\u2500 "
            lines.append(f"{prefix}{connector}{child['name']}")

            next_prefix = prefix + ("    " if is_last else "\u2502   ")
            _render(cid, next_prefix)

    # Render root-level folders
    roots = children_map.get("ROOT", [])
    for i, root in enumerate(roots):
        rid = root["id"]
        if rid in visited:
            continue
        visited.add(rid)
        lines.append(root["name"])
        _render(rid, "")

    return "\n".join(lines) if lines else "(empty)"


# ============================================================================
# Action Handlers
# ============================================================================

def _action_list(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """List all folders with optional tree view and filters."""
    include_deleted = kwargs.get("include_deleted", False)
    folder_name = kwargs.get("folder_name")
    folder_path = kwargs.get("folder_path")
    tree_view = kwargs.get("tree_view", True)

    folders = _query_all_folders(sdk, include_deleted=include_deleted)

    # Apply client-side filters
    filtered = folders
    if folder_name:
        filtered = [f for f in folders if folder_name.lower() in f.get("name", "").lower()]
    elif folder_path:
        filtered = [f for f in folders if folder_path.lower() in f.get("full_path", "").lower()]

    active_count = sum(1 for f in filtered if not f.get("deleted"))
    deleted_count = sum(1 for f in filtered if f.get("deleted"))

    result = {
        "_success": True,
        "folders": filtered,
        "count": len(filtered),
        "active_count": active_count,
        "deleted_count": deleted_count,
    }

    if tree_view and not folder_name and not folder_path:
        result["tree"] = _build_tree_string(folders)

    return result


def _action_get(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Get a single folder by ID."""
    folder_id = kwargs.get("folder_id")
    if not folder_id:
        return {"_success": False, "error": "folder_id is required for 'get' action"}

    folder = sdk.folder.get_folder(id_=folder_id)
    return {
        "_success": True,
        "folder": _folder_to_dict(folder),
    }


def _action_create(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Create a folder or folder hierarchy from a path like 'Parent/Child/Grand'."""
    folder_name = kwargs.get("folder_name")
    if not folder_name:
        return {"_success": False, "error": "folder_name is required for 'create' action (single name or path like 'A/B/C')"}

    parent_folder_id = kwargs.get("parent_folder_id")

    parts = [p.strip() for p in folder_name.split('/') if p.strip()]
    created_folders = []
    parent_id = parent_folder_id

    for part in parts:
        # Check if a folder with this name already exists under the current parent
        existing = _find_folder_by_name_and_parent(sdk, part, parent_id)

        if existing:
            parent_id = existing["id"]
            created_folders.append({"name": part, "id": parent_id, "status": "existing"})
        else:
            new_folder = Folder(name=part, parent_id=parent_id)
            created = sdk.folder.create_folder(request_body=new_folder)
            parent_id = getattr(created, 'id_', '')
            created_folders.append({
                "name": part,
                "id": parent_id,
                "full_path": getattr(created, 'full_path', ''),
                "status": "created",
            })

    # Return info about the final (deepest) folder
    final = created_folders[-1] if created_folders else {}
    return {
        "_success": True,
        "folder_id": final.get("id", ""),
        "folder_name": folder_name,
        "hierarchy": created_folders,
        "created_count": sum(1 for f in created_folders if f.get("status") == "created"),
        "reused_count": sum(1 for f in created_folders if f.get("status") == "existing"),
    }


def _action_move(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Move a component to a different folder via XML manipulation."""
    import xml.etree.ElementTree as ET

    component_id = kwargs.get("component_id")
    target_folder_id = kwargs.get("target_folder_id")

    if not component_id:
        return {"_success": False, "error": "component_id is required for 'move' action"}
    if not target_folder_id:
        return {"_success": False, "error": "target_folder_id is required for 'move' action"}

    # Import from _shared.py
    from boomi_mcp.categories.components._shared import component_get_xml

    # Step 1: Get current component XML
    current = component_get_xml(sdk, component_id)
    old_folder_id = current.get('folder_id', '')
    old_folder_name = current.get('folder_name', '')
    component_name = current.get('name', '')
    raw_xml = current['xml']

    # Step 2: Modify folderId attribute
    root = ET.fromstring(raw_xml)
    root.set('folderId', target_folder_id)
    modified_xml = ET.tostring(root, encoding='unicode')

    # Step 3: Update component with modified XML
    sdk.component.update_component_raw(component_id, modified_xml)

    # Step 4: Verify
    verify = component_get_xml(sdk, component_id)
    new_folder_id = verify.get('folder_id', '')
    new_folder_name = verify.get('folder_name', '')

    return {
        "_success": True,
        "component_id": component_id,
        "component_name": component_name,
        "old_folder": {"id": old_folder_id, "name": old_folder_name},
        "new_folder": {"id": new_folder_id, "name": new_folder_name},
    }


def _action_delete(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Delete a folder. By default, checks that the folder is empty first."""
    folder_id = kwargs.get("folder_id")
    force = kwargs.get("force", False)
    if not folder_id:
        return {"_success": False, "error": "folder_id is required for 'delete' action"}

    # Get folder info first (for response)
    folder = sdk.folder.get_folder(id_=folder_id)
    folder_name = getattr(folder, 'name', '')
    folder_path = getattr(folder, 'full_path', '')

    # Safety check: verify the folder is empty before deleting
    # The Boomi API will silently delete non-empty folders (soft-deleting everything)
    if not force:
        # Check for components in folder
        comp_expression = ComponentMetadataSimpleExpression(
            operator=ComponentMetadataSimpleExpressionOperator.EQUALS,
            property=ComponentMetadataSimpleExpressionProperty.FOLDERID,
            argument=[folder_id]
        )
        comp_filter = ComponentMetadataQueryConfigQueryFilter(expression=comp_expression)
        comp_config = ComponentMetadataQueryConfig(query_filter=comp_filter)
        comp_result = sdk.component_metadata.query_component_metadata(request_body=comp_config)
        has_components = hasattr(comp_result, 'result') and comp_result.result and len(comp_result.result) > 0

        # Check for sub-folders
        sub_expression = FolderSimpleExpression(
            operator=FolderSimpleExpressionOperator.EQUALS,
            property=FolderSimpleExpressionProperty.PARENTNAME,
            argument=[folder_name]
        )
        sub_filter = FolderQueryConfigQueryFilter(expression=sub_expression)
        sub_config = FolderQueryConfig(query_filter=sub_filter)
        sub_result = sdk.folder.query_folder(request_body=sub_config)
        has_subfolders = False
        if hasattr(sub_result, 'result') and sub_result.result:
            for sf in sub_result.result:
                if not getattr(sf, 'deleted', False) and getattr(sf, 'parent_id', '') == folder_id:
                    has_subfolders = True
                    break

        if has_components or has_subfolders:
            contents = []
            if has_components:
                contents.append("components")
            if has_subfolders:
                contents.append("sub-folders")
            return {
                "_success": False,
                "error": f"Cannot delete folder '{folder_name}': folder contains {' and '.join(contents)}. "
                         f"Move or delete its contents first, or use force=true in config to delete anyway.",
                "folder_id": folder_id,
                "folder_name": folder_name,
            }

    sdk.folder.delete_folder(id_=folder_id)

    return {
        "_success": True,
        "folder_id": folder_id,
        "folder_name": folder_name,
        "folder_path": folder_path,
        "tip": f"To restore this folder, use action='restore' with folder_id='{folder_id}'",
    }


def _action_restore(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """Restore a deleted folder by re-creating with the same ID."""
    folder_id = kwargs.get("folder_id")
    if not folder_id:
        return {"_success": False, "error": "folder_id is required for 'restore' action"}

    restored_folder = Folder(id_=folder_id)
    restored = sdk.folder.create_folder(request_body=restored_folder)

    return {
        "_success": True,
        "folder_id": getattr(restored, 'id_', folder_id),
        "folder_name": getattr(restored, 'name', ''),
        "folder_path": getattr(restored, 'full_path', ''),
        "note": "Restoring a folder also restores all components that were in it.",
    }


def _action_contents(sdk: Boomi, profile: str, **kwargs) -> Dict[str, Any]:
    """List components and sub-folders in a folder."""
    folder_id = kwargs.get("folder_id")
    folder_name = kwargs.get("folder_name")

    # Resolve folder_id from folder_name if needed
    if not folder_id and folder_name:
        found = _find_folder_by_name(sdk, folder_name)
        if not found:
            return {"_success": False, "error": f"Folder not found: '{folder_name}'"}
        folder_id = found["id"]

    if not folder_id:
        return {"_success": False, "error": "folder_id or folder_name (in config) is required for 'contents' action"}

    # Get folder info
    folder = sdk.folder.get_folder(id_=folder_id)
    folder_dict = _folder_to_dict(folder)

    # Query components in this folder
    comp_expression = ComponentMetadataSimpleExpression(
        operator=ComponentMetadataSimpleExpressionOperator.EQUALS,
        property=ComponentMetadataSimpleExpressionProperty.FOLDERID,
        argument=[folder_id]
    )
    comp_filter = ComponentMetadataQueryConfigQueryFilter(expression=comp_expression)
    comp_config = ComponentMetadataQueryConfig(query_filter=comp_filter)
    comp_result = sdk.component_metadata.query_component_metadata(request_body=comp_config)

    components = []
    if hasattr(comp_result, 'result') and comp_result.result:
        for c in comp_result.result:
            # Only include current, non-deleted versions
            if (str(getattr(c, 'current_version', 'false')).lower() == 'true'
                    and str(getattr(c, 'deleted', 'true')).lower() == 'false'):
                components.append({
                    "id": getattr(c, 'component_id', '') or getattr(c, 'id_', ''),
                    "name": getattr(c, 'name', ''),
                    "type": getattr(c, 'type_', ''),
                })

    # Paginate components
    while hasattr(comp_result, 'query_token') and comp_result.query_token:
        comp_result = sdk.component_metadata.query_more_component_metadata(
            request_body=comp_result.query_token
        )
        if hasattr(comp_result, 'result') and comp_result.result:
            for c in comp_result.result:
                if (str(getattr(c, 'current_version', 'false')).lower() == 'true'
                        and str(getattr(c, 'deleted', 'true')).lower() == 'false'):
                    components.append({
                        "id": getattr(c, 'component_id', '') or getattr(c, 'id_', ''),
                        "name": getattr(c, 'name', ''),
                        "type": getattr(c, 'type_', ''),
                    })

    # Query sub-folders (API limitation: must query by PARENTNAME, not parentId)
    parent_name = getattr(folder, 'name', '')
    sub_expression = FolderSimpleExpression(
        operator=FolderSimpleExpressionOperator.EQUALS,
        property=FolderSimpleExpressionProperty.PARENTNAME,
        argument=[parent_name]
    )
    sub_filter = FolderQueryConfigQueryFilter(expression=sub_expression)
    sub_config = FolderQueryConfig(query_filter=sub_filter)
    sub_result = sdk.folder.query_folder(request_body=sub_config)

    sub_folders = []
    if hasattr(sub_result, 'result') and sub_result.result:
        for sf in sub_result.result:
            if not getattr(sf, 'deleted', False):
                # Verify this is actually a child of the target folder
                sf_parent_id = getattr(sf, 'parent_id', '')
                if sf_parent_id == folder_id:
                    sub_folders.append({
                        "id": getattr(sf, 'id_', ''),
                        "name": getattr(sf, 'name', ''),
                    })

    return {
        "_success": True,
        "folder": folder_dict,
        "components": components,
        "component_count": len(components),
        "sub_folders": sub_folders,
        "sub_folder_count": len(sub_folders),
    }


# ============================================================================
# Action Router
# ============================================================================

def manage_folders_action(
    sdk: Boomi,
    profile: str,
    action: str,
    config_data: Optional[Dict[str, Any]] = None,
    **kwargs,
) -> Dict[str, Any]:
    """
    Route to the appropriate folder action handler.

    Args:
        sdk: Authenticated Boomi SDK client
        profile: Profile name
        action: One of: list, get, create, move, delete, restore, contents
        config_data: Action-specific configuration dict
        **kwargs: Additional parameters (folder_id, etc.)
    """
    if config_data is None:
        config_data = {}

    # Merge config_data into kwargs
    merged = {**config_data, **kwargs}

    actions = {
        "list": _action_list,
        "get": _action_get,
        "create": _action_create,
        "move": _action_move,
        "delete": _action_delete,
        "restore": _action_restore,
        "contents": _action_contents,
    }

    handler = actions.get(action)
    if not handler:
        return {
            "_success": False,
            "error": f"Unknown action: {action}",
            "valid_actions": list(actions.keys()),
        }

    try:
        return handler(sdk, profile, **merged)
    except Exception as e:
        return {
            "_success": False,
            "error": f"Action '{action}' failed: {str(e)}",
            "exception_type": type(e).__name__,
        }
