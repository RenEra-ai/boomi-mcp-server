"""
High-level integration builder orchestration.

This module provides a single action router that can:
- plan: normalize and validate an integration spec, then build an execution plan
- apply: execute component operations in deterministic dependency order
- verify: verify created/updated components and declared dependency wiring
"""

from __future__ import annotations

import re
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

# Matches `subType="database"` and `subType='database'` with any (or no)
# whitespace around the `=`. XML attribute syntax allows whitespace there,
# so an exact substring check would miss valid raw XML and skip the
# database secret scan.
_XML_DATABASE_SUBTYPE_RE = re.compile(r'\bsubType\s*=\s*["\']database["\']')

# Same idea for REST Client raw XML — a connector_type-less raw payload that
# carries `subType="officialboomi-X3979C-rest-prod"` should still trigger the
# REST secret scan so plaintext credentials cannot leak through the plan echo
# (codex review item #2 against the superseded HTTP-issue-#24 implementation).
_XML_REST_SUBTYPE_RE = re.compile(
    r'\bsubType\s*=\s*["\']officialboomi-X3979C-rest-prod["\']'
)

from boomi import Boomi
from boomi.models import (
    ComponentMetadataQueryConfig,
    ComponentMetadataQueryConfigQueryFilter,
    ComponentMetadataSimpleExpression,
    ComponentMetadataSimpleExpressionOperator,
    ComponentMetadataSimpleExpressionProperty,
)

from ..models.integration_models import IntegrationComponentSpec, IntegrationSpecV1
from .components._shared import component_get_xml, paginate_metadata
from .components.builders import (
    BuilderValidationError,
    DatabaseConnectorBuilder,
    DatabaseGetOperationBuilder,
    DatabaseReadProfileBuilder,
    DatabaseStoredProcedureReadProfileBuilder,
    REST_CLIENT_SUBTYPE,
    RestClientConnectionBuilder,
    RestClientOperationBuilder,
    ProcessFlowBuilder,
    PROFILE_BUILDERS,
    PROCESS_FLOW_BUILDERS,
    get_process_flow_builder,
    get_profile_builder,
)
from .components.builders.connector_builder import _resolve_rest_connector_type
from .components.connectors import create_connector, update_connector
from .components.manage_component import create_component, update_component
from .components.processes import create_process, update_process
from .components.trading_partners import create_trading_partner, update_trading_partner


# Session-scoped; lost on server restart. Verify calls are best-effort.
_BUILD_REGISTRY: Dict[str, Dict[str, Any]] = {}

_TYPE_ALIASES = {
    "process": "process",
    "connector": "connector-settings",
    "connection": "connector-settings",
    "connector-settings": "connector-settings",
    "connector_action": "connector-action",
    "operation": "connector-action",
    "connector-action": "connector-action",
    "tradingpartner": "trading_partner",
    "trading_partner": "trading_partner",
    "component": "component",
    "profile.db": "profile.db",
}

_METADATA_TYPE_MAP = {
    "process": "process",
    "connector-settings": "connector-settings",
    "connector-action": "connector-action",
    "trading_partner": "tradingpartner",
    "profile.db": "profile.db",
}


def _normalize_component_type(value: str) -> str:
    key = (value or "").strip().lower()
    return _TYPE_ALIASES.get(key, key)


def _normalize_component(raw: Dict[str, Any], index: int) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError(f"integration_spec.components[{index - 1}] must be a JSON object")

    key = raw.get("key") or raw.get("name") or f"component_{index}"
    component_type = raw.get("type") or raw.get("component_type")
    if not component_type:
        raise ValueError(f"Component '{key}' is missing required field: type")

    normalized_type = _normalize_component_type(component_type)
    action = (raw.get("action") or "create").lower()
    if action not in ("create", "update"):
        raise ValueError(f"Component '{key}' has invalid action '{action}'. Use create or update.")

    config = raw.get("config")
    if config is None:
        config = raw.get("spec", {})
    if not isinstance(config, dict):
        raise ValueError(f"Component '{key}' config must be a JSON object")

    depends_on = raw.get("depends_on")
    if depends_on is None:
        depends_on = raw.get("dependencies", [])
    if not isinstance(depends_on, list):
        raise ValueError(f"Component '{key}' depends_on must be an array")

    # Promote config.name to top-level name when the caller omitted it.
    # _resolve_existing_components matches against comp.name only — without
    # this fallback a process whose only name is inside config bypasses
    # collision detection (Codex review r7 P2.1).
    #
    # Strip whitespace from BOTH surfaces so collision lookup, the
    # PROCESS_NAME_CONFLICT check, and emitted XML all see the same
    # canonical value. Codex review r10: top-level `name="X"` with
    # `config.name=" X "` used to plan as `create` (lookup queried
    # `"X"`, found nothing) and then emit XML carrying `" X "` —
    # bypassing the r8 mismatch guard because the stripped comparison
    # treated them as equal.
    raw_name = raw.get("name")
    if isinstance(raw_name, str):
        raw_name = raw_name.strip()
        raw["name"] = raw_name  # not strictly needed downstream but keeps `raw` consistent for any in-place inspector
    config_name = config.get("name") if isinstance(config, dict) else None
    if isinstance(config_name, str) and isinstance(config, dict):
        config["name"] = config_name.strip()
        config_name = config["name"]
    effective_name = (
        raw_name
        if isinstance(raw_name, str) and raw_name
        else (config_name if isinstance(config_name, str) and config_name else raw_name)
    )

    return {
        "key": key,
        "type": normalized_type,
        "action": action,
        "name": effective_name,
        "component_id": raw.get("component_id"),
        "config": config,
        "depends_on": depends_on,
    }


def _normalize_to_spec(config: Dict[str, Any]) -> IntegrationSpecV1:
    if not isinstance(config, dict):
        raise ValueError("config must be a JSON object")

    mode = (config.get("mode") or "lift_shift").strip().lower()
    source_description = config.get("source_description")
    spec_payload = config.get("integration_spec")

    if spec_payload is None:
        if isinstance(source_description, dict):
            spec_payload = {
                "name": source_description.get("name") or config.get("name") or "Integration Build",
                "mode": mode,
                "components": source_description.get("components", []),
                "goals": source_description.get("goals", []),
                "endpoints": source_description.get("endpoints", []),
                "flows": source_description.get("flows", []),
                "naming": source_description.get("naming", {}),
                "folders": source_description.get("folders", {}),
                "runtime": source_description.get("runtime", {}),
                "validation_rules": source_description.get("validation_rules", {}),
            }
        else:
            spec_payload = {
                "name": config.get("name") or "Integration Build",
                "mode": mode,
                "components": config.get("components", []),
                "goals": [source_description] if isinstance(source_description, str) and source_description.strip() else [],
                "endpoints": config.get("endpoints", []),
                "flows": config.get("flows", []),
                "naming": config.get("naming", {}),
                "folders": config.get("folders", {}),
                "runtime": config.get("runtime", {}),
                "validation_rules": config.get("validation_rules", {}),
            }

    if not isinstance(spec_payload, dict):
        raise ValueError("integration_spec must be a JSON object")

    spec_data = dict(spec_payload)
    spec_data.setdefault("mode", mode)
    if "name" not in spec_data or not spec_data.get("name"):
        spec_data["name"] = config.get("name") or "Integration Build"

    raw_components = spec_data.get("components", [])
    if not isinstance(raw_components, list):
        raise ValueError("integration_spec.components must be an array")
    normalized_components = [_normalize_component(item, idx + 1) for idx, item in enumerate(raw_components)]
    spec_data["components"] = normalized_components

    return IntegrationSpecV1(**spec_data)


def _topological_order(spec: IntegrationSpecV1) -> List[str]:
    components_by_key = {comp.key: comp for comp in spec.components}
    if len(components_by_key) != len(spec.components):
        raise ValueError("Duplicate component keys are not allowed")

    indegree = {key: 0 for key in components_by_key}
    graph: Dict[str, List[str]] = defaultdict(list)

    for comp in spec.components:
        for dep in comp.depends_on:
            if dep not in components_by_key:
                raise ValueError(f"Component '{comp.key}' depends on unknown component '{dep}'")
            graph[dep].append(comp.key)
            indegree[comp.key] += 1

    ready = sorted([key for key, degree in indegree.items() if degree == 0])
    ordered: List[str] = []

    while ready:
        current = ready.pop(0)
        ordered.append(current)
        for dependent in sorted(graph.get(current, [])):
            indegree[dependent] -= 1
            if indegree[dependent] == 0:
                ready.append(dependent)
        ready.sort()

    if len(ordered) != len(spec.components):
        raise ValueError("Circular dependency detected in integration components")

    return ordered


def _metadata_type_for_component(comp: IntegrationComponentSpec) -> Optional[str]:
    if comp.type == "component":
        raw_type = comp.config.get("type")
        if isinstance(raw_type, str):
            return raw_type
        return None
    return _METADATA_TYPE_MAP.get(comp.type)


def _resolve_existing_components(
    boomi_client: Boomi, comp: IntegrationComponentSpec
) -> List[Dict[str, Any]]:
    """Return ALL metadata dicts matching *comp* by type + exact name.

    Each dict contains at least: component_id, name, folder_name, type.
    Returns an empty list when no matches exist or the component has
    no name / no resolvable metadata type.
    """
    if not comp.name:
        return []

    metadata_type = _metadata_type_for_component(comp)
    if not metadata_type:
        return []

    expression = ComponentMetadataSimpleExpression(
        operator=ComponentMetadataSimpleExpressionOperator.EQUALS,
        property=ComponentMetadataSimpleExpressionProperty.TYPE,
        argument=[metadata_type],
    )
    query_filter = ComponentMetadataQueryConfigQueryFilter(expression=expression)
    query_config = ComponentMetadataQueryConfig(query_filter=query_filter)
    components = paginate_metadata(boomi_client, query_config, show_all=False)
    matches = [item for item in components if item.get("name") == comp.name]
    matches.sort(key=lambda item: item.get("component_id", ""))
    return matches


def _extract_component_id(result: Dict[str, Any]) -> Optional[str]:
    if not isinstance(result, dict):
        return None

    direct_keys = ("component_id", "process_id", "id")
    for key in direct_keys:
        value = result.get(key)
        if isinstance(value, str) and value:
            return value

    trading_partner = result.get("trading_partner")
    if isinstance(trading_partner, dict):
        value = trading_partner.get("component_id")
        if isinstance(value, str) and value:
            return value

    components = result.get("components")
    if isinstance(components, dict) and len(components) == 1:
        only = next(iter(components.values()))
        if isinstance(only, dict):
            value = only.get("component_id")
            if isinstance(value, str) and value:
                return value

    return None


def _check_database_get_dependencies(
    comp: IntegrationComponentSpec,
    raw_config: Dict[str, Any],
) -> Optional[BuilderValidationError]:
    """Cross-step dependency checks specific to database Get operations.

    Boomi binds a connection to an operation at the process connector step,
    not in the operation XML — so the connection ID is never embedded. But
    plan-time we still need the caller to declare both dependencies via
    `connection_ref_key` + `depends_on` (for connection) and `read_profile_id`
    + `depends_on` (when read_profile_id is a `$ref:KEY` token), otherwise
    the apply ordering would be unsafe.
    """
    depends_on = set(comp.depends_on or [])

    connection_ref_key = raw_config.get("connection_ref_key")
    if not connection_ref_key or not str(connection_ref_key).strip():
        return BuilderValidationError(
            "connection_ref_key is required for database Get operations",
            error_code="MISSING_DB_DEPENDENCY",
            field="connection_ref_key",
            hint=(
                "Declare the database connector-settings key the operation "
                "will bind to at process time, and add the same key to "
                "depends_on so plan ordering is correct."
            ),
        )
    if connection_ref_key not in depends_on:
        return BuilderValidationError(
            f"connection_ref_key {connection_ref_key!r} must also appear in depends_on",
            error_code="MISSING_DB_DEPENDENCY",
            field="depends_on",
            hint=(
                "Add the connector-settings key to depends_on so the "
                "execution order creates the connection before the operation."
            ),
        )

    read_profile_id = raw_config.get("read_profile_id")
    if isinstance(read_profile_id, str) and read_profile_id.startswith("$ref:"):
        ref_key = read_profile_id[5:]
        if not ref_key:
            return BuilderValidationError(
                "read_profile_id $ref token is empty (expected '$ref:KEY')",
                error_code="MISSING_DB_READ_PROFILE_REF",
                field="read_profile_id",
                hint=(
                    "Use '$ref:db_read_profile' to reference a profile.db "
                    "component created earlier in the same integration spec."
                ),
            )
        if ref_key not in depends_on:
            return BuilderValidationError(
                f"read_profile_id $ref target {ref_key!r} must also appear in depends_on",
                error_code="MISSING_DB_DEPENDENCY",
                field="depends_on",
                hint=(
                    "Add the read profile key to depends_on so the execution "
                    "order creates the profile before the operation."
                ),
            )

    return None


def _check_rest_operation_dependencies(
    comp: IntegrationComponentSpec,
    raw_config: Dict[str, Any],
) -> Optional[BuilderValidationError]:
    """Cross-step dependency checks specific to REST Client operations (issue #24).

    Boomi binds a REST connection to an operation at the process connector
    step, not in the operation XML — so the connection ID is never embedded.
    Plan-time we still need the caller to declare:
      * the connection (`connection_ref_key` + `depends_on`),
      * any referenced profiles via `$ref:KEY` tokens
        (`request_profile_id` AND `response_profile_id` — codex review item
        #3 against the superseded HTTP implementation),
      * any payload-source upstream step (`payload_source_ref_key`).

    Without these, apply-time ordering would be unsafe (operation runs before
    its inputs exist or before `_resolve_dependency_tokens` can substitute
    the `$ref` into a real component_id).
    """
    depends_on = set(comp.depends_on or [])

    connection_ref_key = raw_config.get("connection_ref_key")
    if not connection_ref_key or not str(connection_ref_key).strip():
        return BuilderValidationError(
            "connection_ref_key is required for REST operations",
            error_code="REST_CONNECTION_REF_REQUIRED",
            field="connection_ref_key",
            hint=(
                "Declare the REST connector-settings key the operation will "
                "bind to at process time, and add the same key to depends_on "
                "so plan ordering is correct."
            ),
        )
    if connection_ref_key not in depends_on:
        return BuilderValidationError(
            f"connection_ref_key {connection_ref_key!r} must also appear in depends_on",
            error_code="REST_DEPENDENCY_REQUIRED",
            field="depends_on",
            hint=(
                "Add the connector-settings key to depends_on so the execution "
                "order creates the connection before the operation."
            ),
        )

    for ref_field in ("request_profile_id", "response_profile_id"):
        value = raw_config.get(ref_field)
        if isinstance(value, str) and value.startswith("$ref:"):
            ref_key = value[5:]
            if not ref_key:
                return BuilderValidationError(
                    f"{ref_field} $ref token is empty (expected '$ref:KEY')",
                    error_code="REST_PROFILE_REF_UNRESOLVED",
                    field=ref_field,
                    hint=(
                        f"Use '$ref:<profile key>' to reference a profile "
                        "component declared earlier in the same integration spec."
                    ),
                )
            if ref_key not in depends_on:
                return BuilderValidationError(
                    f"{ref_field} $ref target {ref_key!r} must also appear in depends_on",
                    error_code="REST_DEPENDENCY_REQUIRED",
                    field="depends_on",
                    hint=(
                        "Add the profile key to depends_on so the execution "
                        "order creates the profile before the operation."
                    ),
                )

    payload_source_ref_key = raw_config.get("payload_source_ref_key")
    if (
        payload_source_ref_key
        and isinstance(payload_source_ref_key, str)
        and payload_source_ref_key.strip()
        and payload_source_ref_key not in depends_on
    ):
        return BuilderValidationError(
            f"payload_source_ref_key {payload_source_ref_key!r} must also appear in depends_on",
            error_code="REST_DEPENDENCY_REQUIRED",
            field="depends_on",
            hint=(
                "Add the payload source key to depends_on so the execution "
                "order creates the payload-producing step before the operation."
            ),
        )

    return None


# REST config fields known to carry secret/credential-like values. When ANY
# REST validation error fires, these paths are scrubbed from the plan echo
# regardless of which validator won — otherwise an earlier failing check
# (missing connection_ref_key, missing base_url, etc.) leaves the sensitive
# data unredacted (codex review item P1, round-6). Paths are dotted to match
# `_redact_dotted_field_path`'s contract.
_REST_SENSITIVE_FIELD_PATHS = (
    "oauth2.client_secret",         # also caught by FORBIDDEN_SECRET_FIELDS
    "oauth2.client_secret_ref",     # raw value when it should be credential://
    "credential_ref",               # raw value when it should be credential://
    "request_headers",              # whole dict — Authorization / X-API-Key etc.
    "query_parameters",             # whole dict — api_key / token in querystring
    # Codex round-3 P1: the OAuth2 parameter blocks are deferred-emission
    # (rejected by validation with UNSUPPORTED_REST_OAUTH2_PARAMETERS) but
    # callers can put arbitrary content there — `prompt=consent`,
    # `audience=...`, custom claims, anything. Scrub on the rejection path
    # so the rejected payload doesn't echo through `integration_spec`.
    "oauth2.authorization_parameters",
    "oauth2.access_token_parameters",
)

# Cert refs are handled separately by `_redact_malformed_cert_refs` (below)
# because their redaction is conditional on shape: PEM/key/garbage gets
# scrubbed, but a valid GUID cert ref MUST survive so the caller can fix
# an unrelated error from the plan output without losing the cert binding.
# Codex review round-5 P2.
_REST_CERT_REF_FIELDS = ("private_certificate_ref", "public_certificate_ref")


def _redact_malformed_cert_refs(config: Any) -> None:
    """Conditional redaction for `private_certificate_ref` /
    `public_certificate_ref`.

    Cert refs are NOT a uniformly-secret field like `credential_ref`: the
    expected value is a Boomi component-id GUID, which is itself not a
    secret. We only need to scrub the field when the caller has put
    PEM/SSH-key/garbage there instead — that material IS secret-bearing.

    Codex round-5 P2: previously the cert refs were added to
    `_REST_SENSITIVE_FIELD_PATHS` so the always-on sweep scrubbed them
    unconditionally. That over-redacted valid GUIDs when an unrelated
    field failed validation (e.g. missing base_url), making the returned
    spec unusable for correction. This helper redacts only when the
    value isn't already in the documented GUID shape.
    """
    if not isinstance(config, dict):
        return
    for field in _REST_CERT_REF_FIELDS:
        value = config.get(field)
        if value in (None, ""):
            continue
        # Valid GUID — preserve (the caller can correct other errors and
        # resubmit without re-entering the cert binding).
        if (
            isinstance(value, str)
            and RestClientConnectionBuilder._BOOMI_COMPONENT_ID_RE.match(value.strip())
        ):
            continue
        # Anything else (PEM, SSH key, non-string, malformed) is treated
        # as potential secret material and scrubbed.
        config[field] = "[REDACTED]"


def _redact_dotted_field_path(config: Any, dotted_path: Optional[str]) -> None:
    """Replace the value at a dotted path inside `config` with '[REDACTED]'.

    Targeted at field names returned by REST validation when the offending
    value isn't a forbidden-key (which `redact_forbidden_secret_fields_in_place`
    handles): e.g. `oauth2.client_secret_ref` (raw value where a
    `credential://...` ref was expected) or `request_headers` /
    `query_parameters` (entire dict carries unverified non-empty values
    that may include Authorization / X-API-Key entries).

    Defense-in-depth: if walking the dotted path finds a non-dict at an
    intermediate step (e.g. caller passed `oauth2="raw-secret"` instead
    of a sub-dict), the deep leaf can't be located but the top-level
    segment IS still leaking. Redact the top-level segment in that case
    so the raw value never echoes into the plan output. This case was
    found in codex round-2 QA (Bug #126): widening the stale-oauth2 gate
    to reject non-dict values exposed a residual redaction gap because
    the original walk-down logic silently no-op'd on non-dict
    intermediates.
    """
    if not isinstance(dotted_path, str) or not dotted_path:
        return
    if not isinstance(config, dict):
        return
    parts = dotted_path.split(".")
    cursor: Any = config
    for part in parts[:-1]:
        if not isinstance(cursor, dict):
            return
        next_cursor = cursor.get(part)
        # Malformed intermediate (non-None, non-dict) — the deep leaf
        # can't be reached but the top-level segment carries the raw
        # value. Redact at the top level and return.
        if next_cursor is not None and not isinstance(next_cursor, dict):
            top = parts[0]
            if top in config:
                config[top] = "[REDACTED]"
            return
        cursor = next_cursor
    if not isinstance(cursor, dict):
        return
    leaf = parts[-1]
    if leaf in cursor:
        cursor[leaf] = "[REDACTED]"


def _resolve_dependency_tokens(value: Any, id_registry: Dict[str, str]) -> Any:
    if isinstance(value, str):
        if value.startswith("$ref:"):
            ref_key = value[5:]
            return id_registry.get(ref_key, value)
        return value
    if isinstance(value, list):
        return [_resolve_dependency_tokens(item, id_registry) for item in value]
    if isinstance(value, dict):
        return {k: _resolve_dependency_tokens(v, id_registry) for k, v in value.items()}
    return value


def _apply_clone_suffix(comp: IntegrationComponentSpec, config: Dict[str, Any]) -> Dict[str, Any]:
    suffix = "-clone"
    cloned = dict(config)

    if comp.type == "process":
        base = cloned.get("name") or comp.name
        if base:
            cloned["name"] = f"{base}{suffix}"
        return cloned

    if comp.type in ("connector-settings", "connector-action"):
        base = cloned.get("component_name") or cloned.get("name") or comp.name
        if base:
            cloned["component_name"] = f"{base}{suffix}"
            cloned.setdefault("name", cloned["component_name"])
        return cloned

    if comp.type == "trading_partner":
        base = cloned.get("component_name") or comp.name
        if base:
            cloned["component_name"] = f"{base}{suffix}"
        return cloned

    if comp.type == "profile.db":
        # profile.db participates in metadata lookup since Issue #23 added it
        # to _METADATA_TYPE_MAP, so conflict_policy=clone is reachable. Without
        # the suffix, create_clone would produce an indistinguishable duplicate
        # that the next plan would see as ambiguous.
        base = cloned.get("component_name") or comp.name
        if base:
            cloned["component_name"] = f"{base}{suffix}"
        return cloned

    return cloned


def _execute_component(
    boomi_client: Boomi,
    profile: str,
    comp: IntegrationComponentSpec,
    config: Dict[str, Any],
    target_id: Optional[str] = None,
) -> Dict[str, Any]:
    payload = dict(config)
    # Align apply-time dispatcher predicates with plan-time predicates:
    # _build_plan keys validation off comp.type, but the create_connector /
    # create_component dispatchers branch on config["component_type"]. A
    # spec with top-level type="connector-action" or "profile.db" that omits
    # the duplicate component_type key would plan clean against the right
    # validator and then misroute at apply (Codex review items 1+2 against
    # commit f398b35).
    if comp.type in ("connector-settings", "connector-action", "profile.db"):
        payload.setdefault("component_type", comp.type)
    if comp.name:
        if comp.type == "process":
            payload.setdefault("name", comp.name)
        elif comp.type in ("connector-settings", "connector-action"):
            payload.setdefault("component_name", comp.name)
            payload.setdefault("name", comp.name)
        elif comp.type == "trading_partner":
            payload.setdefault("component_name", comp.name)
        elif comp.type == "profile.db":
            # Mirror plan-time validation, which injects comp.name into
            # effective_config["component_name"] before calling validate_config.
            # Without this, a spec with top-level name="..." but no
            # config.component_name plans clean and then fails at apply with
            # DATABASE_OPERATION_VALIDATION_FAILED: component_name is required.
            payload.setdefault("component_name", comp.name)

    if comp.type == "process":
        # process_kind=... opts into the structured process-flow builder
        # (issue #25). _build_plan has already validated config + depends_on
        # for create/create_clone/update, and rejected the
        # process_kind + raw xml combination via PROCESS_KIND_XML_CONFLICT,
        # so by the time we land here either:
        #   - process_kind is set and we build the XML
        #   - process_kind is unset and we use the legacy JSON path
        # The two are mutually exclusive at the plan layer.
        process_kind = str(
            payload.get("process_kind") or payload.get("process_type") or ""
        ).strip().lower()
        if process_kind:
            builder_cls = get_process_flow_builder(process_kind)
            if builder_cls is None:
                return {
                    "_success": False,
                    "error_code": "PROCESS_KIND_UNSUPPORTED",
                    "error": (
                        f"process_kind {process_kind!r} is not supported "
                        f"by the structured process-flow builder."
                    ),
                    "field": "process_kind",
                    "hint": (
                        f"Supported process_kind values: "
                        f"{sorted(PROCESS_FLOW_BUILDERS)}."
                    ),
                }
            try:
                # payload["name"] takes precedence so _apply_clone_suffix's
                # "<name>-clone" suffix actually reaches the emitted XML.
                # _apply_clone_suffix writes the suffixed name into
                # config["name"] (which becomes payload["name"]); if we
                # consulted comp.name first the original unsuffixed name
                # would win and the clone would emit as a name-duplicate.
                # Codex review r3 P2 (clone bypass).
                #
                # No comp.key fallback: plan-time PROCESS_NAME_REQUIRED
                # (codex review r6 P2.1) guarantees one of these two is
                # set before we get here. Falling back to comp.key would
                # silently rename the Boomi-side process to the user's
                # internal dependency token on update.
                xml = builder_cls.build(
                    payload,
                    name=payload.get("name") or comp.name,
                    folder_name=payload.get("folder_name"),
                )
            except BuilderValidationError as exc:
                return {
                    "_success": False,
                    "error_code": exc.error_code,
                    "error": str(exc),
                    "field": exc.field,
                    "hint": exc.hint,
                }
            if comp.action == "create":
                return create_component(boomi_client, profile, {"xml": xml})
            if not target_id:
                return {
                    "_success": False,
                    "error": f"Missing process_id for update of component '{comp.key}'",
                }
            return update_component(boomi_client, profile, target_id, {"xml": xml})

        if comp.action == "create":
            return create_process(boomi_client, profile, payload)
        if not target_id:
            return {"_success": False, "error": f"Missing process_id for update of component '{comp.key}'"}
        return update_process(boomi_client, profile, target_id, payload)

    if comp.type in ("connector-settings", "connector-action"):
        # Normalize local-alias connector_types to their canonical Boomi form
        # BEFORE the get_connector sanity check, so Boomi's catalog lookup
        # recognizes the type. `rest` and `rest_client` are MCP-local aliases
        # for the canonical REST Client subtype `officialboomi-X3979C-rest-prod`;
        # Boomi's API only knows the canonical. Codex review item P2 against
        # the issue-#24 REST landing.
        rest_canonical = _resolve_rest_connector_type(payload.get("connector_type"))
        if rest_canonical is not None:
            payload["connector_type"] = rest_canonical
        connector_type = payload.get("connector_type")
        if connector_type:
            try:
                boomi_client.connector.get_connector(connector_type)
            except Exception as exc:
                return {
                    "_success": False,
                    "error": f"Connector type validation failed for '{connector_type}': {exc}",
                }
        if comp.action == "create":
            return create_connector(boomi_client, profile, payload)
        if not target_id:
            return {"_success": False, "error": f"Missing component_id for update of connector '{comp.key}'"}
        return update_connector(boomi_client, profile, target_id, payload)

    if comp.type == "trading_partner":
        if comp.action == "create":
            return create_trading_partner(boomi_client, profile, payload)
        if not target_id:
            return {"_success": False, "error": f"Missing component_id for update of trading partner '{comp.key}'"}
        return update_trading_partner(boomi_client, profile, target_id, payload)

    if comp.action == "create":
        return create_component(boomi_client, profile, payload)
    if not target_id:
        return {"_success": False, "error": f"Missing component_id for update of component '{comp.key}'"}
    return update_component(boomi_client, profile, target_id, payload)


def _build_plan(boomi_client: Boomi, config: Dict[str, Any]) -> Dict[str, Any]:
    spec = _normalize_to_spec(config)
    conflict_policy = (config.get("conflict_policy") or "reuse").lower()
    if conflict_policy not in ("reuse", "clone", "fail"):
        return {
            "_success": False,
            "error": f"Invalid conflict_policy '{conflict_policy}'. Valid values: reuse, clone, fail.",
        }

    try:
        execution_order = _topological_order(spec)
    except ValueError as exc:
        return {"_success": False, "error": str(exc)}

    components_by_key = {comp.key: comp for comp in spec.components}
    steps: List[Dict[str, Any]] = []
    warnings: List[str] = []

    for key in execution_order:
        comp = components_by_key[key]

        # If the caller supplied an explicit component_id, skip ambiguity checking
        if comp.component_id:
            candidates: List[Dict[str, Any]] = []
            existing_id: Optional[str] = comp.component_id
        else:
            candidates = _resolve_existing_components(boomi_client, comp)
            existing_id = candidates[0].get("component_id") if len(candidates) == 1 else None

        planned_action = comp.action

        if comp.action == "create":
            if len(candidates) > 1:
                if conflict_policy == "clone":
                    # Clone creates a new component with a suffix — no targeting risk.
                    # Set existing_id so _apply_plan enters the clone-suffix branch.
                    planned_action = "create_clone"
                    existing_id = candidates[0].get("component_id")
                else:
                    planned_action = "error_ambiguous_match"
            elif len(candidates) == 1:
                if conflict_policy == "reuse":
                    planned_action = "reuse"
                elif conflict_policy == "clone":
                    planned_action = "create_clone"
                else:
                    planned_action = "error_if_exists"

        elif comp.action == "update" and not comp.component_id:
            if len(candidates) > 1:
                planned_action = "error_ambiguous_match"
            elif len(candidates) == 0:
                planned_action = "error_missing_target"

        # Process components opt into the structured process-flow builder
        # via config.process_kind (or config.process_type). Without it,
        # processes fall through to the legacy linear JSON-to-XML path
        # in create_process. process_flow_xml is the new structured route
        # added by issue #25 (M2.5).
        raw_config = comp.config or {}
        # str() coercion guards against non-string process_kind (e.g. 123)
        # before .strip(). The builder's validate_config does the same; this
        # site runs FIRST for route selection so it has to coerce too.
        # Codex review L1 / QA bug #128.
        process_kind = (
            str(raw_config.get("process_kind") or raw_config.get("process_type") or "")
            .strip()
            .lower()
        ) if comp.type == "process" else ""

        route = (
            "process_flow_xml"
            if comp.type == "process" and process_kind
            else "process_json_to_xml"
            if comp.type == "process"
            else "connector_builder_or_xml"
            if comp.type in ("connector-settings", "connector-action")
            else "trading_partner_json"
            if comp.type == "trading_partner"
            else "profile_builder_or_xml"
            if comp.type == "profile.db"
            else "generic_component_xml"
        )

        # Database connector-settings preflight. Two-tier validation:
        #
        # (a) scan_forbidden_secret_fields runs on EVERY database
        #     connector-settings step regardless of apply path. Plan output
        #     dumps comp.config verbatim, so a plaintext password in a
        #     reuse/update/raw-XML config would leak into the response even
        #     though apply itself wouldn't use it. We also infer "database"
        #     from raw XML subType when connector_type is omitted —
        #     create_connector's raw-XML path doesn't require connector_type.
        #
        # (b) validate_config (driver, auth, credential_ref, required fields)
        #     runs only when the apply path will actually invoke
        #     DatabaseConnectorBuilder.build(). Reuse short-circuits
        #     (_apply_plan line ~547), update goes through update_connector,
        #     and config.xml bypasses the builder in create_connector.
        #     Validating those paths would block legitimate plans. Mirror
        #     _execute_component's defaulting (component_name from comp.name).
        validation_error: Optional[Dict[str, Any]] = None
        raw_config = comp.config or {}
        xml_payload = raw_config.get("xml") or ""
        xml_says_database = bool(
            xml_payload and _XML_DATABASE_SUBTYPE_RE.search(xml_payload)
        )
        is_database_connector_settings = (
            comp.type == "connector-settings"
            and (
                raw_config.get("connector_type") == "database"
                or xml_says_database
            )
        )
        # Every profile.db component is a builder candidate (regardless of
        # profile_type value). The builder validator surfaces the right
        # structured error — UNSUPPORTED_DB_PROFILE_MODE for missing/blank
        # profile_type, MISSING_DB_QUERY for missing SQL, etc. Without this
        # widening, a malformed profile.db without profile_type would plan
        # as a clean `create` and leak any secret-shaped fields into the
        # plan echo (Codex review item #3).
        is_database_read_profile = (comp.type == "profile.db")
        # Every connector-action with connector_type='database' is a builder
        # candidate (regardless of operation_mode value). The validator
        # returns UNSUPPORTED_DB_OPERATION_MODE for send/upsert/missing, so
        # unknown modes can't slip through as clean `create` plans with
        # un-redacted secret echoes (Codex review item #4).
        is_database_get_operation = (
            comp.type == "connector-action"
            and (raw_config.get("connector_type") or "").lower() == "database"
        )
        will_invoke_builder = (
            (is_database_connector_settings
             or is_database_read_profile
             or is_database_get_operation)
            and not xml_payload
            and planned_action in ("create", "create_clone")
        )
        db_err: Optional[BuilderValidationError] = None
        secret_scanner_cls = None
        if is_database_connector_settings:
            secret_scanner_cls = DatabaseConnectorBuilder
        elif is_database_read_profile:
            secret_scanner_cls = DatabaseReadProfileBuilder
        elif is_database_get_operation:
            secret_scanner_cls = DatabaseGetOperationBuilder
        if secret_scanner_cls is not None:
            db_err = secret_scanner_cls.scan_forbidden_secret_fields(raw_config)
            if db_err is None and will_invoke_builder:
                effective_config = dict(raw_config)
                if comp.name:
                    effective_config.setdefault("component_name", comp.name)
                if is_database_connector_settings:
                    db_err = DatabaseConnectorBuilder.validate_config(effective_config)
                elif is_database_read_profile:
                    # Dispatch to the right profile builder via the registry.
                    # Select (database.read) and Stored Procedure
                    # (database.stored_procedure_read) share the same secret-
                    # scan contract but have statement-specific validation
                    # rules. If profile_type is missing/unknown, surface a
                    # unified UNSUPPORTED_DB_PROFILE_MODE error that lists
                    # all supported protocols.
                    profile_type = (effective_config.get("profile_type") or "").lower()
                    builder_instance = get_profile_builder("profile.db", profile_type)
                    if builder_instance is None:
                        valid = sorted({
                            pt for (ct, pt) in PROFILE_BUILDERS if ct == "profile.db"
                        })
                        db_err = BuilderValidationError(
                            f"profile_type {profile_type!r} is not supported "
                            f"for profile.db. Supported: {', '.join(valid)}.",
                            error_code="UNSUPPORTED_DB_PROFILE_MODE",
                            field="profile_type",
                            hint=(
                                "Use one of the supported profile_type values "
                                "(database.read for Select-statement profiles, "
                                "database.stored_procedure_read for Stored "
                                "Procedure profiles). Write profiles are "
                                "tracked by issue #32."
                            ),
                        )
                    else:
                        db_err = type(builder_instance).validate_config(effective_config)
                elif is_database_get_operation:
                    db_err = DatabaseGetOperationBuilder.validate_config(effective_config)
                    # Cross-step dependency checks only apply to the
                    # supported Get path — for unsupported modes (send,
                    # upsert, missing), validate_config above returns first
                    # with UNSUPPORTED_DB_OPERATION_MODE.
                    if db_err is None:
                        db_err = _check_database_get_dependencies(comp, raw_config)
        if db_err is not None:
            planned_action = "error_database_validation"
            validation_error = {
                "error_code": db_err.error_code,
                "error": str(db_err),
                "field": db_err.field,
                "hint": db_err.hint,
            }
            # Scrub EVERY plaintext secret-shaped field from the spec dump,
            # not just the one named in the error. scan_forbidden_secret_fields
            # stops on first match, but a single bad config can carry multiple
            # offenders — leaving the others as plaintext would still leak.
            # Walks nested dicts (pooling, write_options, etc.) too — otherwise
            # a secret stashed inside a sub-block would still appear in the
            # plan's spec echo.
            if db_err.error_code == "PLAINTEXT_SECRET_REJECTED" and secret_scanner_cls is not None:
                secret_scanner_cls.redact_forbidden_secret_fields_in_place(
                    raw_config
                )

        # REST Client connector-settings / connector-action preflight (issue #24).
        # Mirrors the database block above:
        #   (a) scan_forbidden_secret_fields runs on EVERY REST step regardless
        #       of apply path — so reuse/update/raw-XML configs cannot leak
        #       plaintext secrets into the plan echo (including nested
        #       oauth2.client_secret via the recursive walker, codex item #1).
        #   (b) validate_config + dependency check run only when the apply
        #       path will actually invoke the builder (create / create_clone,
        #       no raw XML).
        #   (c) Raw XML without connector_type still triggers (a) when the
        #       payload carries the REST Client subType (codex item #2).
        xml_says_rest = bool(
            xml_payload and _XML_REST_SUBTYPE_RE.search(xml_payload)
        )
        is_rest_connector_settings = (
            comp.type == "connector-settings"
            and (
                _resolve_rest_connector_type(raw_config.get("connector_type")) is not None
                or xml_says_rest
            )
        )
        is_rest_send_operation = (
            comp.type == "connector-action"
            and (
                _resolve_rest_connector_type(raw_config.get("connector_type")) is not None
                or xml_says_rest
            )
        )
        will_invoke_rest_builder = (
            (is_rest_connector_settings or is_rest_send_operation)
            and not xml_payload
            and planned_action in ("create", "create_clone")
        )
        rest_err: Optional[BuilderValidationError] = None
        rest_scanner_cls = None
        if is_rest_connector_settings:
            rest_scanner_cls = RestClientConnectionBuilder
        elif is_rest_send_operation:
            rest_scanner_cls = RestClientOperationBuilder

        if rest_scanner_cls is not None and db_err is None:
            rest_err = rest_scanner_cls.scan_forbidden_secret_fields(raw_config)
            if rest_err is None and will_invoke_rest_builder:
                effective_config = dict(raw_config)
                if comp.name:
                    effective_config.setdefault("component_name", comp.name)
                if is_rest_connector_settings:
                    rest_err = RestClientConnectionBuilder.validate_config(effective_config)
                else:  # is_rest_send_operation
                    rest_err = RestClientOperationBuilder.validate_config(effective_config)
                    if rest_err is None:
                        rest_err = _check_rest_operation_dependencies(comp, raw_config)

        if rest_err is not None:
            planned_action = "error_rest_validation"
            validation_error = {
                "error_code": rest_err.error_code,
                "error": str(rest_err),
                "field": rest_err.field,
                "hint": rest_err.hint,
            }
            if rest_err.error_code == "PLAINTEXT_SECRET_REJECTED" and rest_scanner_cls is not None:
                rest_scanner_cls.redact_forbidden_secret_fields_in_place(
                    raw_config
                )
            # Any REST validation error must scrub the documented sensitive
            # fields, not just the one named in the winning error. Without
            # this, a sensitive value (Authorization header, raw
            # client_secret_ref, raw credential_ref, populated
            # query_parameters) leaks into the plan echo when an EARLIER
            # validator (e.g. missing connection_ref_key, missing base_url)
            # fires first. Codex review item P1 round-6.
            for sensitive_path in _REST_SENSITIVE_FIELD_PATHS:
                _redact_dotted_field_path(raw_config, sensitive_path)
            # Cert refs: conditional redaction — scrub PEM/key material but
            # preserve valid GUIDs so the caller can correct unrelated
            # errors without losing the cert binding. Codex review round-5 P2.
            _redact_malformed_cert_refs(raw_config)

        # Process-flow builder preflight (issue #25, M2.5). Two-tier like
        # the database / REST blocks above:
        #   (a) scan_forbidden_secret_fields runs whenever process_kind is
        #       set, even on update/reuse paths — so a stray plaintext
        #       credential in process config cannot leak through the plan
        #       echo.
        #   (b) validate_config runs only when the apply path will
        #       actually invoke the builder (create / create_clone, and
        #       no raw-XML override). Unknown process_kind always fails
        #       so a typo cannot silently fall through to the legacy
        #       linear path.
        process_flow_err: Optional[BuilderValidationError] = None
        if (
            comp.type == "process"
            and process_kind
            and db_err is None
            and rest_err is None
        ):
            # Run the secret scan unconditionally. The xml-conflict check
            # below short-circuits early, so without scanning first a
            # process config like {process_kind, xml, password} would
            # surface PROCESS_KIND_XML_CONFLICT while leaving the
            # plaintext password in raw_config (== comp.config), which
            # then echoes through spec.model_dump(). Codex review r2 Q3.
            process_flow_err = ProcessFlowBuilder.scan_forbidden_secret_fields(raw_config)
            # Codex review r6 P2.1: require an explicit name. Without
            # this, _execute_component used to fall back to comp.key as
            # the emitted XML name attribute, which on update silently
            # renamed the existing process to its internal dependency
            # key (e.g. "main_process"). Reject at plan-time so the
            # caller must supply a real display name.
            if process_flow_err is None:
                config_name = raw_config.get("name")
                comp_name_clean = (
                    comp.name.strip()
                    if isinstance(comp.name, str) else ""
                )
                config_name_clean = (
                    config_name.strip()
                    if isinstance(config_name, str) else ""
                )
                effective_name = comp_name_clean or config_name_clean
                if not effective_name:
                    process_flow_err = BuilderValidationError(
                        "process component name is required for structured "
                        "process_kind components; without one the emitted "
                        "XML would carry the internal dependency key as "
                        "the display name (silent rename on update).",
                        error_code="PROCESS_NAME_REQUIRED",
                        field="name",
                        hint=(
                            "Set IntegrationComponentSpec.name or "
                            "config.name to the human-readable display "
                            "name the process should carry in Boomi."
                        ),
                    )
                # Codex review r8 F1: when BOTH surfaces are set and
                # they differ, plan-time collision lookup uses comp.name
                # but _execute_component's build() call prefers
                # payload["name"] (the r3 clone-suffix precedence).
                # That mismatch creates a duplicate on create / silently
                # renames on update because Boomi gets a different name
                # than the metadata search resolved. Reject the conflict
                # explicitly. (Apply-time _apply_clone_suffix intentionally
                # introduces a "-clone" difference; that path mutates
                # config["name"] AFTER plan, so this plan-time check
                # never sees it.)
                elif (
                    comp_name_clean
                    and config_name_clean
                    and comp_name_clean != config_name_clean
                ):
                    process_flow_err = BuilderValidationError(
                        f"top-level name {comp_name_clean!r} and "
                        f"config.name {config_name_clean!r} disagree; "
                        f"collision lookup uses the top-level name but "
                        f"the emitted XML would use config.name.",
                        error_code="PROCESS_NAME_CONFLICT",
                        field="name",
                        hint=(
                            "Either drop config.name or make it match "
                            "the top-level IntegrationComponentSpec.name. "
                            "Pick one surface so plan-time collision "
                            "detection and apply-time XML emission agree."
                        ),
                    )
            xml_override = bool(raw_config.get("xml"))
            # Codex review C4: process_kind + raw xml is ambiguous —
            # _execute_component cannot honor both, and falling through to
            # the legacy create_process path silently drops the user's XML.
            # Reject the conflict explicitly so callers must pick one.
            if process_flow_err is None and xml_override:
                process_flow_err = BuilderValidationError(
                    "process_kind and config.xml are mutually exclusive.",
                    error_code="PROCESS_KIND_XML_CONFLICT",
                    field="config.xml",
                    hint=(
                        "Choose one: process_kind for the structured "
                        "builder, OR omit process_kind and pass raw XML "
                        "to the legacy process_json_to_xml path."
                    ),
                )
            # Codex review r9: enum-membership check is a contract
            # assertion about the spec, not about the apply step. Run it
            # unconditionally so a typo like process_kind="bad" surfaces
            # even when conflict_policy=reuse finds an existing match
            # (planned_action="reuse" used to skip the whole block).
            builder_cls: Optional[type] = None
            if process_flow_err is None:
                builder_cls = get_process_flow_builder(process_kind)
                if builder_cls is None:
                    process_flow_err = BuilderValidationError(
                        f"process_kind {process_kind!r} is not supported.",
                        error_code="PROCESS_KIND_UNSUPPORTED",
                        field="process_kind",
                        hint=(
                            f"Supported process_kind values: "
                            f"{sorted(PROCESS_FLOW_BUILDERS)}."
                        ),
                    )

            # Codex review C2: process update also re-invokes the builder
            # (_execute_component → update_component({"xml": built_xml})),
            # unlike DB/REST whose update paths bypass the builder. So
            # full config validation runs on every mutating action; for
            # reuse / error_* the enum check above is enough — we won't
            # emit XML so source/target bindings don't matter.
            will_invoke_process_flow_builder = (
                process_flow_err is None
                and builder_cls is not None
                and planned_action in ("create", "create_clone", "update")
            )
            if will_invoke_process_flow_builder:
                process_flow_err = builder_cls.validate_config(
                    raw_config,
                    depends_on=comp.depends_on,
                )

        if process_flow_err is not None:
            planned_action = "error_process_validation"
            validation_error = {
                "error_code": process_flow_err.error_code,
                "error": str(process_flow_err),
                "field": process_flow_err.field,
                "hint": process_flow_err.hint,
            }
            # Scrub plaintext secrets from comp.config before the spec is
            # echoed back via spec.model_dump(). Mirrors the DB/REST blocks
            # at lines ~860 and ~943 — without this, a flagged value still
            # leaks through the plan response. Codex review C1.
            if process_flow_err.error_code == "PLAINTEXT_SECRET_REJECTED":
                ProcessFlowBuilder.redact_forbidden_secret_fields_in_place(raw_config)

        step: Dict[str, Any] = {
            "key": comp.key,
            "type": comp.type,
            "declared_action": comp.action,
            "planned_action": planned_action,
            "name": comp.name,
            "depends_on": comp.depends_on,
            "existing_component_id": existing_id,
            "route": route,
        }

        if candidates:
            step["candidates"] = [
                {
                    "component_id": c.get("component_id"),
                    "name": c.get("name"),
                    "folder_name": c.get("folder_name"),
                }
                for c in candidates
            ]

        if validation_error is not None:
            step["validation_error"] = validation_error

        steps.append(step)

    if not spec.components:
        warnings.append("No components were provided; plan contains zero executable steps.")
    if config.get("source_description") and not config.get("integration_spec"):
        warnings.append("Spec was derived from source_description. Review normalized output before apply.")

    return {
        "_success": True,
        "integration_spec": spec.model_dump(),
        "conflict_policy": conflict_policy,
        "execution_order": execution_order,
        "steps": steps,
        "warnings": warnings or None,
    }


def _apply_plan(boomi_client: Boomi, profile: str, config: Dict[str, Any]) -> Dict[str, Any]:
    dry_run = bool(config.get("dry_run", True))
    planned = _build_plan(boomi_client, config)
    if not planned.get("_success"):
        return planned
    if dry_run:
        planned["dry_run"] = True
        planned["message"] = "Dry run only. Set dry_run=false to execute."
        return planned

    # Fail-fast: reject plans with unresolvable steps before executing anything
    unresolvable_steps = [
        step for step in planned["steps"]
        if step["planned_action"] in (
            "error_ambiguous_match",
            "error_missing_target",
            "error_database_validation",
            "error_rest_validation",
            "error_process_validation",
        )
    ]
    if unresolvable_steps:
        errors = []
        for step in unresolvable_steps:
            if step["planned_action"] == "error_ambiguous_match":
                candidate_info = step.get("candidates", [])
                ids = [c["component_id"] for c in candidate_info]
                errors.append(
                    f"Component '{step.get('name') or step['key']}' matched "
                    f"{len(candidate_info)} components: {ids}. "
                    f"Supply an explicit component_id to disambiguate."
                )
            elif step["planned_action"] == "error_missing_target":
                errors.append(
                    f"Component '{step.get('name') or step['key']}' has action=update "
                    f"but no matching component was found and no component_id was provided."
                )
            elif step["planned_action"] == "error_database_validation":
                ve = step.get("validation_error") or {}
                errors.append(
                    f"Component '{step.get('name') or step['key']}' failed "
                    f"database validation: "
                    f"{ve.get('error_code', 'DATABASE_CONNECTOR_VALIDATION_FAILED')} "
                    f"on field {ve.get('field')!r}."
                )
            elif step["planned_action"] == "error_rest_validation":
                ve = step.get("validation_error") or {}
                errors.append(
                    f"Component '{step.get('name') or step['key']}' failed "
                    f"REST validation: "
                    f"{ve.get('error_code', 'REST_CONNECTOR_VALIDATION_FAILED')} "
                    f"on field {ve.get('field')!r}."
                )
            elif step["planned_action"] == "error_process_validation":
                ve = step.get("validation_error") or {}
                errors.append(
                    f"Component '{step.get('name') or step['key']}' failed "
                    f"process-flow validation: "
                    f"{ve.get('error_code', 'PROCESS_XML_VALIDATION_FAILED')} "
                    f"on field {ve.get('field')!r}."
                )
        return {
            "_success": False,
            "error": "Plan contains unresolvable steps. No operations were executed.",
            "unresolvable_steps": [
                {
                    "key": s["key"],
                    "planned_action": s["planned_action"],
                    "candidates": s.get("candidates", []),
                    "validation_error": s.get("validation_error"),
                }
                for s in unresolvable_steps
            ],
            "details": errors,
        }

    spec = IntegrationSpecV1(**planned["integration_spec"])
    conflict_policy = planned["conflict_policy"]
    execution_order = planned["execution_order"]
    components_by_key = {comp.key: comp for comp in spec.components}
    existing_ids = {step["key"]: step["existing_component_id"] for step in planned["steps"]}

    id_registry: Dict[str, str] = {}
    results: Dict[str, Dict[str, Any]] = {}

    for key in execution_order:
        comp = components_by_key[key]
        existing_id = existing_ids.get(key)
        resolved_config = _resolve_dependency_tokens(comp.config, id_registry)

        if comp.action == "create" and existing_id:
            if conflict_policy == "reuse":
                results[key] = {
                    "status": "reused",
                    "component_id": existing_id,
                    "type": comp.type,
                    "name": comp.name,
                }
                id_registry[key] = existing_id
                continue
            if conflict_policy == "fail":
                return {
                    "_success": False,
                    "error": f"Component '{comp.name or comp.key}' already exists and conflict_policy=fail",
                    "failed_step": key,
                    "partial_results": results,
                }
            resolved_config = _apply_clone_suffix(comp, resolved_config)

        target_id = comp.component_id or existing_id
        exec_result = _execute_component(
            boomi_client=boomi_client,
            profile=profile,
            comp=comp,
            config=resolved_config,
            target_id=target_id,
        )

        component_id = _extract_component_id(exec_result)
        if component_id:
            id_registry[key] = component_id

        results[key] = {
            "status": "updated" if comp.action == "update" else "created",
            "component_id": component_id,
            "type": comp.type,
            "name": comp.name,
            "result": exec_result,
        }

        if not exec_result.get("_success", False):
            return {
                "_success": False,
                "error": f"Failed at step '{key}'",
                "failed_step": key,
                "step_result": exec_result,
                "partial_results": results,
            }

    build_id = str(uuid4())
    _BUILD_REGISTRY[build_id] = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "profile": profile,
        "spec": spec.model_dump(),
        "results": results,
        "execution_order": execution_order,
    }

    return {
        "_success": True,
        "build_id": build_id,
        "message": f"Applied integration '{spec.name}' with {len(results)} steps.",
        "execution_order": execution_order,
        "results": results,
    }


def _verify_build(boomi_client: Boomi, config: Dict[str, Any]) -> Dict[str, Any]:
    build_id = config.get("build_id")
    if not build_id:
        return {"_success": False, "error": "build_id is required for verify action"}

    build = _BUILD_REGISTRY.get(build_id)
    if not build:
        return {"_success": False, "error": f"Unknown build_id '{build_id}'"}

    spec = IntegrationSpecV1(**build["spec"])
    results: Dict[str, Dict[str, Any]] = build["results"]

    verification: Dict[str, Any] = {"components": {}, "dependency_issues": []}
    verified_count = 0
    failed_count = 0

    for comp in spec.components:
        step = results.get(comp.key)
        component_id = step.get("component_id") if isinstance(step, dict) else None
        if not component_id:
            verification["components"][comp.key] = {
                "verified": False,
                "reason": "No component_id available in build results",
            }
            failed_count += 1
            continue

        try:
            if comp.type == "trading_partner":
                boomi_client.trading_partner_component.get_trading_partner_component(id_=component_id)
            else:
                component_get_xml(boomi_client, component_id)
            verification["components"][comp.key] = {"verified": True, "component_id": component_id}
            verified_count += 1
        except Exception as exc:
            verification["components"][comp.key] = {
                "verified": False,
                "component_id": component_id,
                "error": str(exc),
            }
            failed_count += 1

        for dep in comp.depends_on:
            dep_result = results.get(dep)
            dep_id = dep_result.get("component_id") if isinstance(dep_result, dict) else None
            if not dep_result or not dep_id:
                verification["dependency_issues"].append(
                    f"Component '{comp.key}' depends on '{dep}', but '{dep}' was not resolved to a component_id."
                )

    return {
        "_success": failed_count == 0 and not verification["dependency_issues"],
        "build_id": build_id,
        "verified_components": verified_count,
        "failed_components": failed_count,
        "dependency_issues": verification["dependency_issues"] or None,
        "verification": verification["components"],
    }


def build_integration_action(
    boomi_client: Boomi,
    profile: str,
    action: str,
    config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Route integration builder actions."""
    cfg = config or {}
    if not isinstance(cfg, dict):
        return {"_success": False, "error": "config must be a JSON object"}

    try:
        normalized_action = action.strip().lower()
        if normalized_action == "plan":
            result = _build_plan(boomi_client, cfg)
            result["profile"] = profile
            return result
        if normalized_action == "apply":
            result = _apply_plan(boomi_client, profile, cfg)
            result["profile"] = profile
            return result
        if normalized_action == "verify":
            result = _verify_build(boomi_client, cfg)
            result["profile"] = profile
            return result
        return {
            "_success": False,
            "error": f"Unknown action '{action}'",
            "hint": "Valid actions are: plan, apply, verify",
        }
    except ValueError as exc:
        return {
            "_success": False,
            "error": f"Validation error: {exc}",
            "exception_type": "ValidationError",
        }
    except Exception as exc:
        return {
            "_success": False,
            "error": f"Integration builder failed: {exc}",
            "exception_type": type(exc).__name__,
        }


__all__ = ["build_integration_action"]
