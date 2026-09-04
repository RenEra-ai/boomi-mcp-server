"""
Component Query MCP Tools for Boomi API Integration.

Provides component discovery and retrieval capabilities:
- list: List all components (optionally filtered by type)
- get: Get a single component by ID with full XML
- search: Multi-field search with AND logic
- bulk_get: Retrieve up to 5 components in one call
"""

import time
from typing import Dict, Any, List, Optional

from boomi import Boomi
from boomi.models import (
    ComponentMetadataQueryConfig,
    ComponentMetadataQueryConfigQueryFilter,
    ComponentMetadataSimpleExpression,
    ComponentMetadataSimpleExpressionOperator,
    ComponentMetadataSimpleExpressionProperty,
    ComponentMetadataGroupingExpression,
    ComponentMetadataGroupingExpressionOperator,
)
from boomi.net.transport.api_error import ApiError

from ._shared import (
    component_get_xml,
    paginate_metadata,
    ComponentGetDeadlineExceeded,
    component_get_deadline_envelope,
    component_get_deadline_item,
    _component_get_deadline_seconds,
)

DEFAULT_LIMIT = 100


def _parse_limit(raw) -> int:
    """Parse limit from user-supplied filter value."""
    if raw is None:
        return DEFAULT_LIMIT
    if isinstance(raw, bool):
        return DEFAULT_LIMIT
    if isinstance(raw, int):
        return raw
    if isinstance(raw, float):
        try:
            if raw != int(raw):
                return DEFAULT_LIMIT
            return int(raw)
        except (ValueError, OverflowError):
            return DEFAULT_LIMIT
    if isinstance(raw, str):
        try:
            return int(raw)
        except ValueError:
            return DEFAULT_LIMIT
    return DEFAULT_LIMIT


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


# ============================================================================
# Actions
# ============================================================================

#: Filter keys `list` actually honours. Anything else is refused — see the
#: docstring below for why silence was the wrong default.
#:
#: HAND-WRITTEN and therefore guarded: the first version of this set omitted
#: `folder_name`, which the function has always honoured, so a refusal built to
#: prevent a silent widening instead broke a working filter. A test derives the
#: real set by parsing what the function reads and compares it to this constant, so
#: the two cannot drift again in either direction.
_LIST_FILTER_KEYS = frozenset({
    "show_all", "type", "component_type", "limit", "folder_name",
})


def list_components(
    boomi_client: Boomi,
    profile: str,
    filters: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """List all components, optionally filtered by type.

    A key this action does not support is REFUSED, not ignored. It used to be
    dropped silently while the call still returned `_success: true`, so a caller
    who believed they had scoped the list received the WHOLE account instead — and
    the failure widened the result set, which is the dangerous direction. A
    name-scoped cleanup built on that answer soft-deleted twenty-one components it
    was never meant to touch.

    Note the asymmetry that made it plausible: `search` DOES support `name`, so the
    same key on the sibling action does what a caller expects.
    """
    try:
        if filters:
            unsupported = sorted(k for k in filters if k not in _LIST_FILTER_KEYS)
            if unsupported:
                return {
                    "_success": False,
                    "error": (
                        "list does not support these filter key(s): "
                        f"{unsupported}"
                    ),
                    "supported_filters": sorted(_LIST_FILTER_KEYS),
                    "hint": (
                        "Refusing rather than ignoring them: an ignored filter "
                        "returns MORE than the caller asked for, and a caller who "
                        "believes the result is scoped may act destructively on it. "
                        "For name filtering use action='search'."
                    ),
                }

        show_all = False
        if filters:
            show_all = filters.get('show_all', False)

        comp_type = (filters.get('type') or filters.get('component_type')) if filters else None

        if comp_type:
            expression = ComponentMetadataSimpleExpression(
                operator=ComponentMetadataSimpleExpressionOperator.EQUALS,
                property=ComponentMetadataSimpleExpressionProperty.TYPE,
                argument=[comp_type]
            )
        else:
            expression = ComponentMetadataSimpleExpression(
                operator=ComponentMetadataSimpleExpressionOperator.LIKE,
                property=ComponentMetadataSimpleExpressionProperty.NAME,
                argument=["%"]
            )

        query_filter = ComponentMetadataQueryConfigQueryFilter(expression=expression)
        query_config = ComponentMetadataQueryConfig(query_filter=query_filter)

        components = paginate_metadata(boomi_client, query_config, show_all=show_all)

        # Client-side folder filter
        if filters and filters.get('folder_name'):
            folder = filters['folder_name']
            components = [c for c in components if c.get('folder_name') == folder]

        # Apply limit after all client-side filters
        limit = _parse_limit(filters.get('limit') if filters else None)
        total_available = len(components)
        if limit > 0 and total_available > limit:
            components = components[:limit]

        result = {
            "_success": True,
            "total_count": len(components),
            "components": components,
            "profile": profile,
        }
        if limit > 0 and total_available > len(components):
            result["has_more"] = True
            result["total_available"] = total_available
        return result

    except ApiError as e:
        return {
            "_success": False,
            "error": f"Failed to list components: {_extract_api_error_msg(e)}",
            "exception_type": "ApiError",
        }
    except Exception as e:
        return {
            "_success": False,
            "error": f"Failed to list components: {str(e)}",
            "exception_type": type(e).__name__,
        }


def get_component(
    boomi_client: Boomi,
    profile: str,
    component_id: str
) -> Dict[str, Any]:
    """Get a single component by ID with full XML."""
    try:
        comp_data = component_get_xml(boomi_client, component_id)
        return {
            "_success": True,
            "component": comp_data,
            "profile": profile,
        }
    except ComponentGetDeadlineExceeded as e:
        return component_get_deadline_envelope(e)
    except ApiError as e:
        return {
            "_success": False,
            "error": f"Failed to get component '{component_id}': {_extract_api_error_msg(e)}",
            "exception_type": "ApiError",
            "hint": "Verify the component ID exists and is accessible",
        }
    except Exception as e:
        return {
            "_success": False,
            "error": f"Failed to get component '{component_id}': {str(e)}",
            "exception_type": type(e).__name__,
            "hint": "Verify the component ID exists and is accessible",
        }


def search_components(
    boomi_client: Boomi,
    profile: str,
    filters: Dict[str, Any]
) -> Dict[str, Any]:
    """Multi-field component search with AND logic."""
    KNOWN_FILTER_KEYS = {'name', 'type', 'component_type', 'sub_type', 'component_id',
                         'created_by', 'modified_by', 'folder_name', 'show_all', 'limit'}
    try:
        expressions = []

        if filters.get('name'):
            name_val = filters['name']
            if '%' not in name_val:
                name_val = '%' + name_val + '%'
            expressions.append(ComponentMetadataSimpleExpression(
                operator=ComponentMetadataSimpleExpressionOperator.LIKE,
                property=ComponentMetadataSimpleExpressionProperty.NAME,
                argument=[name_val]
            ))

        comp_type = filters.get('type') or filters.get('component_type')
        if comp_type:
            expressions.append(ComponentMetadataSimpleExpression(
                operator=ComponentMetadataSimpleExpressionOperator.EQUALS,
                property=ComponentMetadataSimpleExpressionProperty.TYPE,
                argument=[comp_type]
            ))

        # Additional filter fields (EQUALS operator)
        filter_map = {
            'sub_type':     ComponentMetadataSimpleExpressionProperty.SUBTYPE,
            'component_id': ComponentMetadataSimpleExpressionProperty.COMPONENTID,
            'created_by':   ComponentMetadataSimpleExpressionProperty.CREATEDBY,
            'modified_by':  ComponentMetadataSimpleExpressionProperty.MODIFIEDBY,
        }
        for key, prop in filter_map.items():
            if filters.get(key):
                expressions.append(ComponentMetadataSimpleExpression(
                    operator=ComponentMetadataSimpleExpressionOperator.EQUALS,
                    property=prop,
                    argument=[filters[key]]
                ))

        if not expressions:
            # Fallback: match all
            expressions.append(ComponentMetadataSimpleExpression(
                operator=ComponentMetadataSimpleExpressionOperator.LIKE,
                property=ComponentMetadataSimpleExpressionProperty.NAME,
                argument=["%"]
            ))

        if len(expressions) == 1:
            root_expr = expressions[0]
        else:
            root_expr = ComponentMetadataGroupingExpression(
                operator=ComponentMetadataGroupingExpressionOperator.AND,
                nested_expression=expressions
            )

        query_filter = ComponentMetadataQueryConfigQueryFilter(expression=root_expr)
        query_config = ComponentMetadataQueryConfig(query_filter=query_filter)

        show_all = filters.get('show_all', False)
        components = paginate_metadata(boomi_client, query_config, show_all=show_all)

        # Client-side folder filter
        if filters.get('folder_name'):
            folder = filters['folder_name']
            components = [c for c in components if c.get('folder_name') == folder]

        # Apply limit after all client-side filters
        limit = _parse_limit(filters.get('limit'))
        total_available = len(components)
        if limit > 0 and total_available > limit:
            components = components[:limit]

        unknown = set(filters.keys()) - KNOWN_FILTER_KEYS
        result = {
            "_success": True,
            "total_count": len(components),
            "components": components,
            "profile": profile,
            "filters_applied": {k: v for k, v in filters.items() if v and k in KNOWN_FILTER_KEYS and k != 'limit'},
        }
        if limit > 0 and total_available > len(components):
            result["has_more"] = True
            result["total_available"] = total_available
        if unknown:
            result["ignored_filters"] = sorted(unknown)
        return result

    except ApiError as e:
        return {
            "_success": False,
            "error": f"Failed to search components: {_extract_api_error_msg(e)}",
            "exception_type": "ApiError",
        }
    except Exception as e:
        return {
            "_success": False,
            "error": f"Failed to search components: {str(e)}",
            "exception_type": type(e).__name__,
        }


def bulk_get_components(
    boomi_client: Boomi,
    profile: str,
    component_ids: List[str]
) -> Dict[str, Any]:
    """Retrieve up to 5 components by their IDs.

    Uses individual ``component_get_xml()`` calls (one GET per id through the
    SDK-backed raw-XML helper), which is efficient for up to 5 components.
    """
    try:
        if not component_ids:
            return {"_success": False, "error": "component_ids list is empty"}

        if len(component_ids) > 5:
            return {
                "_success": False,
                "error": f"Maximum 5 components per bulk request (got {len(component_ids)})",
                "hint": "Split into multiple bulk_get calls of 5 or fewer IDs",
            }

        components = []
        errors = []
        # Aggregate wall-clock budget: a bulk of stalled components must not sum
        # past the platform request timeout (e.g. 5 × the per-item deadline).
        # Each GET gets at most the remaining budget; once it's spent, the rest
        # are reported as deadline errors without starting another (possibly
        # stalling) request.
        budget = float(_component_get_deadline_seconds())
        for cid in component_ids:
            if budget < 1:
                errors.append({
                    'component_id': cid,
                    'error': 'Skipped: bulk component-read deadline budget exhausted',
                    'error_code': 'COMPONENT_GET_DEADLINE_EXCEEDED',
                    'retryable': True,
                })
                continue
            started = time.monotonic()
            try:
                comp = component_get_xml(boomi_client, cid, deadline_seconds=int(budget))
                # Remove full XML from bulk response to keep it lighter
                comp_summary = {k: v for k, v in comp.items() if k != 'xml'}
                components.append(comp_summary)
            except ComponentGetDeadlineExceeded as e:
                errors.append(component_get_deadline_item(e))
            except ApiError as e:
                errors.append({'component_id': cid, 'error': _extract_api_error_msg(e)})
            except Exception as e:
                errors.append({'component_id': cid, 'error': str(e)})
            finally:
                budget -= time.monotonic() - started

        all_failed = errors and not components
        result = {
            "_success": not all_failed,
            "total_count": len(components),
            "components": components,
            "profile": profile,
        }
        if errors:
            result["errors"] = errors
        if all_failed:
            result["error"] = f"All {len(errors)} component(s) failed to retrieve"

        return result

    except ApiError as e:
        return {
            "_success": False,
            "error": f"Failed to bulk get components: {_extract_api_error_msg(e)}",
            "exception_type": "ApiError",
        }
    except Exception as e:
        return {
            "_success": False,
            "error": f"Failed to bulk get components: {str(e)}",
            "exception_type": type(e).__name__,
        }


# ============================================================================
# Action Router
# ============================================================================

#: The router's action list, and the ONE place it is written. The served copies —
#: the hint below and the capability catalogue — derive from this rather than
#: keeping their own transcript, because a hand-copied action list is how a
#: catalogue comes to advertise ten actions where the router accepts seventeen.
QUERY_COMPONENTS_ACTIONS: tuple = (
    "list",
    "get",
    "search",
    "bulk_get",
    "idempotency_contract_candidates",
)


def query_components_action(
    boomi_client: Boomi,
    profile: str,
    action: str,
    **params
) -> Dict[str, Any]:
    """Route query_components actions."""
    try:
        if action == "list":
            filters = params.get("filters", None)
            return list_components(boomi_client, profile, filters)

        elif action == "get":
            component_id = params.get("component_id")
            if not component_id:
                return {
                    "_success": False,
                    "error": "component_id is required for 'get' action",
                    "hint": "Provide the component ID to retrieve",
                }
            return get_component(boomi_client, profile, component_id)

        elif action == "search":
            filters = params.get("filters")
            if filters is None:
                return {
                    "_success": False,
                    "error": "config with search filters is required for 'search' action",
                    "hint": 'Provide config like: {"name": "Test", "type": "process"}',
                }
            return search_components(boomi_client, profile, filters)

        elif action == "bulk_get":
            component_ids = params.get("component_ids")
            if component_ids is None:
                return {
                    "_success": False,
                    "error": "component_ids is required for 'bulk_get' action",
                    "hint": 'Provide component_ids as a JSON array: ["id1", "id2"]',
                }
            return bulk_get_components(boomi_client, profile, component_ids)

        elif action == "idempotency_contract_candidates":
            config = params.get("config") or {}
            if not isinstance(config, dict):
                return {
                    "_success": False,
                    "error": "config must be an object for 'idempotency_contract_candidates'",
                    "hint": 'Provide config like: {"operation_component_id": "...", '
                            '"connection_component_id": "..."}',
                }
            from ...connector_replay.discovery import idempotency_contract_candidates

            def _live_identity(component_id):
                """The account's current identity for a component, or None.

                Injected so discovery itself stays free of the transport layer.
                A read that fails is None — never a partial identity, which
                would let discovery answer from half a fact.
                """
                try:
                    fetched = get_component(boomi_client, profile, component_id)
                except Exception:
                    return None
                if not isinstance(fetched, dict) or not fetched.get("_success", True):
                    return None
                # The version lives on the fetched COMPONENT, not on the envelope.
                # Reading it off the envelope returned None for every readable
                # component, so a live account reported as unreadable.
                component = fetched.get("component")
                if not isinstance(component, dict):
                    return None
                # A soft-deleted component is not a live identity. The account
                # still serves it with a version, so reading the version alone
                # reported a deleted component as the thing a candidate would be
                # matched against.
                deleted = component.get("deleted")
                if deleted is True or str(deleted).strip().lower() == "true":
                    return None
                version = component.get("version")
                if version is None:
                    return None
                # THE CONFIGURATION DIGEST TOO, when the fetched XML allows one.
                # Id and version alone let a candidate be returned for components
                # whose CONFIGURATION had moved — a version advances on any
                # update, so equal versions do not mean equal configuration, and
                # the matcher had nothing else to compare. Absent rather than
                # invented when the component cannot be digested: the matcher
                # treats a missing digest as "not compared", never as "agreed".
                identity = {"component_id": component_id, "version": version}
                xml = component.get("xml") or fetched.get("xml")
                kind = ("operation" if str(component.get("type", "")).strip()
                        == "connector-action" else "connection")
                if isinstance(xml, str) and xml.strip():
                    try:
                        # THREE dots, not four: this module is
                        # `boomi_mcp.categories.components.query_components`, so
                        # four rises above the top-level package and raises
                        # ImportError. The fail-closed handler below then turned
                        # that into identity-unavailable for EVERY request — a
                        # broken import wearing the disguise of a safe refusal,
                        # which is the hazard of catching a broad exception around
                        # an import.
                        from ...connector_replay.digests import (
                            component_config_digest_v1,
                        )

                        identity["config_digest"] = component_config_digest_v1(
                            xml, kind=kind
                        )
                    except Exception:      # noqa: BLE001
                        # PRESENT BUT UNDIGESTIBLE IS UNAVAILABLE, not silent.
                        # Swallowing this returned an identity carrying id and
                        # version with no digest, and discovery reads a missing
                        # digest as "not compared" — so a component whose XML this
                        # build cannot project would have matched a stale record
                        # on id and version alone. Absence of XML is a different
                        # case and still yields a partial identity; absence of a
                        # digest for XML we HAVE is a failure to read the account.
                        return None
                return identity

            # THE REGISTRY'S REFUSAL KEEPS ITS CODE. `RegistryInvalid` carries a
            # registered error code, and the generic handler below serves only a
            # message and a class name — so a caller was told the action failed
            # with no machine-readable way to tell "this build's packaged
            # evidence is unreadable" from any other failure, which is the one
            # distinction that changes what they should do next.
            #
            # Caught HERE rather than by teaching the generic envelope to read a
            # code off any exception: that envelope is repeated across two dozen
            # modules and this slice consumes it rather than owning it, so
            # rewriting all of them late in this issue would be a large,
            # unrelated change. The narrow catch closes it on the surface this
            # slice does own.
            from ...connector_replay.registry import RegistryInvalid

            try:
                # THE ACCOUNT THIS CALL IS IN, so a record bound to another one
                # cannot be offered as a candidate here. Derived from the client
                # rather than taken from the caller: the account is a fact about
                # the connection, not a parameter a request may assert.
                scope = None
                try:
                    from ...connector_replay.digests import account_scope_hash
                    from ..integration_builder import _client_account_id

                    account_id = _client_account_id(boomi_client)
                    if account_id:
                        scope = account_scope_hash(account_id)
                except Exception:      # noqa: BLE001 — unknown, never guessed
                    scope = None
                return idempotency_contract_candidates(
                    operation_component_id=config.get("operation_component_id"),
                    connection_component_id=config.get("connection_component_id"),
                    live_identity=_live_identity,
                    account_scope_hash=scope,
                )
            except RegistryInvalid as invalid:
                return {
                    "_success": False,
                    "error": str(invalid),
                    "error_code": RegistryInvalid.code,
                    "hint": "The packaged replay-evidence registry could not be "
                            "read by this build, so no replay decision can be "
                            "made from it. This is a build defect, not a request "
                            "defect — retrying with different arguments will not "
                            "change it.",
                }

        else:
            return {
                "_success": False,
                "error": f"Unknown action: {action}",
                "hint": "Valid actions are: " + ", ".join(QUERY_COMPONENTS_ACTIONS),
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


__all__ = ['QUERY_COMPONENTS_ACTIONS', 'query_components_action']
