"""Derive the typed ``flows`` projection and normalize legacy rows (issue #157).

Two responsibilities, ONE representation:

* :func:`derive_transform_flows` — the canonical projection. For each authored
  process root, in key order, one :class:`DerivedTransformFlowV1` per ``map_ref``
  node the root's body executes, built from the surviving profile generator
  (``profile_from_json_schema`` / ``profile_from_db_read_fields``), the
  referenced map component's normalized destinations, and
  ``validate_field_mappings``. Served on the compiling intents' preview; never
  compiled, hashed or applied.

* :func:`normalize_flow_row` — the ONE normalizer that brings a LEGACY rich row
  (DB-source form, API-source form) and a canonical row to the same
  representation, so the flows accounting can compare them by full payload.
  The only representation change is the recorded one (ledger correction P6):
  a map-script body is carried as ``script_body_sha256`` + ``script_body_present``.

Rows are DERIVED from executable authorities and are not one themselves.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from ..models.derived_flows import DerivedTransformFlowV1
from ..models.integration_models import IntegrationComponentSpec

REF_PREFIX = "$ref:"

#: Connector family -> the legacy row's `source` token for the step that feeds
#: the transform. A closed three-entry table, pinned by test against the
#: connector-call capability registry's family vocabulary so a fourth family
#: cannot arrive without a decision here.
SOURCE_TOKEN_BY_FAMILY: Mapping[str, str] = {
    "database": "extract",
    "rest": "fetch",
    "wss": "listen",
}


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# normalization (the one representation)
# ---------------------------------------------------------------------------


#: The legacy `future_builder_issue` annotation is a PURE FUNCTION of the
#: route — measured on every frozen row (`test_issue_157_flows_accounting`
#: asserts it) — so it carries no content of its own and is normalized out
#: (rule R2 below). The mapping is kept so that measurement can be made.
FUTURE_BUILDER_ISSUE_BY_ROUTE: Mapping[str, str] = {
    "direct": "#26",
    "map_function": "#40",
    "map_script": "#41",
}

#: The normalization RULES, each a representation reconciliation recorded on
#: the ledger — never a content discard. Numbered so a test can name them.
NORMALIZATION_RULES: Tuple[str, ...] = (
    "R1 script_body -> script_body_sha256 (+ script_body_present); ADR-001 §11",
    "R2 future_builder_issue dropped: a pure function of operation_type (measured)",
    "R3 *_profile_generation.component_name dropped: the legacy DB form passed "
    "None and the API form a derived default, while the emitted profile "
    "component carries the real name in both worlds",
    "R4 direct source_field -> source_path: two spellings of one leaf (the DB "
    "form spelled 'field', the API form served both; both-present ⇒ equal, measured)",
)


def normalize_operation_summary(summary: Mapping[str, Any]) -> Dict[str, Any]:
    """One operation summary in the normalized representation.

    Field ORDER is not significant (the comparison is by sorted keys); content
    is preserved verbatim except the recorded rules R1, R2 and R4.
    """
    out: Dict[str, Any] = {}
    for key, value in summary.items():
        if key == "script_body":  # R1
            if value is not None:
                out["script_body_sha256"] = _sha256(str(value))
            continue
        if key == "future_builder_issue":  # R2
            continue
        if isinstance(value, (list, tuple)):
            out[key] = list(value)
        elif isinstance(value, Mapping):
            out[key] = _plain(value)
        else:
            out[key] = value
    if out.get("operation_type") == "map_script":
        out.setdefault("script_body_present", "script_body_sha256" in out)
    if out.get("operation_type") == "direct" and "source_field" in out:  # R4
        field = out.pop("source_field")
        if "source_path" not in out or out["source_path"] is None:
            out["source_path"] = field
        elif out["source_path"] != field:
            # Two DIFFERENT references would be content; keep both, loudly.
            out["source_field"] = field
    return out


def _normalize_generation(artifact: Any) -> Any:
    if not isinstance(artifact, Mapping):
        return _plain(artifact)
    out = {k: _plain(v) for k, v in artifact.items() if k != "component_name"}  # R3
    return out


def _plain(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(k): _plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(v) for v in value]
    return value


def normalize_flow_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    """A legacy or canonical flow row in the ONE comparable representation.

    Plain JSON types only, keys as authored, list order preserved, meaningful
    ``None`` values kept, ``operations`` normalized per entry (P6). Callers that
    need a digest hash the canonical JSON of this dict.
    """
    out: Dict[str, Any] = {}
    for key, value in row.items():
        if key == "operations" and isinstance(value, (list, tuple)):
            out[key] = [
                normalize_operation_summary(item) if isinstance(item, Mapping) else _plain(item)
                for item in value
            ]
        elif key in ("source_profile_generation", "target_profile_generation"):
            out[key] = _normalize_generation(value)
        else:
            out[key] = _plain(value)
    return out


def flow_row_digest(row: Mapping[str, Any]) -> str:
    """sha256 of the canonical JSON of the normalized row."""
    return _sha256(
        json.dumps(
            normalize_flow_row(row), sort_keys=True, separators=(",", ":"), ensure_ascii=True
        )
    )


def typed_row_from_legacy(row: Mapping[str, Any]) -> DerivedTransformFlowV1:
    """Parse a LEGACY rich transform row into the typed model via the normalizer.

    This is the normalization test's subject: both legacy producer forms must
    parse without dropping content. A legacy leaf the model cannot carry is a
    parse failure here, never a silent omission.
    """
    return DerivedTransformFlowV1.model_validate(normalize_flow_row(row))


# ---------------------------------------------------------------------------
# the canonical projection
# ---------------------------------------------------------------------------


def _ref_key(reference: Any) -> Optional[str]:
    if isinstance(reference, str) and reference.startswith(REF_PREFIX):
        return reference[len(REF_PREFIX):] or None
    return None


def _walk_nodes(node: Any) -> List[Any]:
    """Every node of a ProcessIR body in authored (document) order.

    Generic over the model's OWN field set rather than a hand-list of child
    attribute names: every field holding a pydantic model, or a list of them,
    is descended, so a body kind added later is walked without a decision here.
    """
    from pydantic import BaseModel

    found: List[Any] = []
    if isinstance(node, BaseModel):
        found.append(node)
        for name in type(node).model_fields:
            child = getattr(node, name, None)
            if isinstance(child, BaseModel):
                found.extend(_walk_nodes(child))
            elif isinstance(child, (list, tuple)):
                for item in child:
                    if isinstance(item, BaseModel):
                        found.extend(_walk_nodes(item))
    return found


def _leaves_summary(index: Mapping[str, Mapping[str, Any]]) -> Dict[str, Any]:
    """A JSON profile's legacy `format/root_name/leaf_count/leaves` summary from its index."""
    leaves = sorted(
        (path, record.get("data_type") or "")
        for path, record in index.items()
        if record.get("mappable")
    )
    root_name = ""
    for path, record in index.items():
        if record.get("kind") == "object" and "/" not in path:
            root_name = record.get("name") or path
            break
    return {
        "format": "json",
        "root_name": root_name,
        "leaf_count": len(leaves),
        "leaves": [{"path": path, "data_type": data_type} for path, data_type in leaves],
    }


def _db_schema_summary(index: Mapping[str, Mapping[str, Any]], config: Mapping[str, Any]) -> Dict[str, Any]:
    fields = []
    declared = {}
    for entry in config.get("output_fields") or config.get("fields") or ():
        if isinstance(entry, Mapping) and isinstance(entry.get("name"), str):
            declared[entry["name"]] = entry
    for name, record in index.items():
        source = declared.get(name, {})
        fields.append(
            {
                "name": name,
                "data_type": record.get("data_type") or "",
                "required": bool(source.get("required", source.get("mandatory", False))),
            }
        )
    return {"field_count": len(fields), "fields": fields}


def _generated_profile(component: IntegrationComponentSpec) -> Optional[Dict[str, Any]]:
    """The surviving generator's artifact for an in-plan profile component."""
    from ..categories.components.builders.profile_generation import (
        profile_from_db_read_fields,
        profile_from_json_schema,
    )

    config = component.config or {}
    name = component.name or config.get("component_name")
    try:
        if component.type == "profile.json":
            root = config.get("root")
            if root is None and isinstance(config.get("payload_profile"), Mapping):
                return profile_from_json_schema(config["payload_profile"], component_name=name)
            if root is None:
                return None
            return profile_from_json_schema(
                {"format": config.get("format", "json"), "root": root}, component_name=name
            )
        if component.type == "profile.db":
            fields = config.get("output_fields") or config.get("fields")
            if not fields:
                return None
            return profile_from_db_read_fields(
                [
                    {
                        "name": f.get("name"),
                        "data_type": f.get("data_type"),
                        "required": f.get("required", f.get("mandatory", False)),
                    }
                    for f in fields
                    if isinstance(f, Mapping)
                ],
                component_name=name,
            )
    except Exception:  # noqa: BLE001 — a profile the generator refuses has no artifact
        return None
    return None


def _operation_summaries(map_config: Mapping[str, Any]) -> List[Dict[str, Any]]:
    summaries: List[Dict[str, Any]] = []
    for entry in map_config.get("field_mappings") or ():
        if isinstance(entry, Mapping):
            summary: Dict[str, Any] = {
                "operation_type": "direct",
                "source_path": entry.get("source_path"),
                "target_path": entry.get("target_path"),
            }
            if isinstance(entry.get("documentation_hint"), str):
                summary["documentation_hint"] = entry["documentation_hint"]
            summaries.append(summary)
    for entry in map_config.get("function_mappings") or ():
        if isinstance(entry, Mapping):
            inputs = list(entry.get("inputs") or ())
            summary = {
                "operation_type": "map_function",
                "function_type": entry.get("function_type"),
                "inputs": inputs,
                "input_count": len(inputs),
                "target_path": entry.get("target_path"),
            }
            if isinstance(entry.get("parameters"), Mapping):
                summary["parameters"] = dict(entry["parameters"])
            if isinstance(entry.get("documentation_hint"), str):
                summary["documentation_hint"] = entry["documentation_hint"]
            summaries.append(summary)
    for entry in map_config.get("script_mappings") or ():
        if isinstance(entry, Mapping):
            inputs = [
                item.get("source_path")
                for item in (entry.get("inputs") or ())
                if isinstance(item, Mapping)
            ]
            outputs = [
                item.get("target_path")
                for item in (entry.get("outputs") or ())
                if isinstance(item, Mapping)
            ]
            summary = {
                "operation_type": "map_script",
                "language": entry.get("language") or "groovy",
                "inputs": inputs,
                "input_count": len(inputs),
                "outputs": outputs,
                "output_count": len(outputs),
                "script_body_present": bool(entry.get("script_body")),
            }
            if entry.get("script_body"):
                summary["script_body"] = entry["script_body"]
            if entry.get("script_component_id"):
                summary["script_component_ref"] = entry["script_component_id"]
            if isinstance(entry.get("documentation_hint"), str):
                summary["documentation_hint"] = entry["documentation_hint"]
            summaries.append(summary)
    return summaries


def derive_transform_flows(
    units: Sequence[Any],
    components: Sequence[IntegrationComponentSpec],
    *,
    connector_metadata: Optional[Mapping[str, Tuple[Optional[str], Optional[str]]]] = None,
) -> Tuple[DerivedTransformFlowV1, ...]:
    """One typed row per ``map_ref`` node, per root, in key order.

    Every field is read from an authority: the referenced map component's
    config (destinations, operation summaries), the referenced profiles'
    generator artifacts, and the connector metadata projection for the feeding
    step's family. A map whose profiles the generator cannot index yields a row
    with the summaries it CAN derive, never an invented one.
    """
    from ..categories.components.builders.profile_generation import (
        validate_field_mappings,
    )

    by_key = {component.key: component for component in components}
    metadata = dict(connector_metadata or {})
    rows: List[DerivedTransformFlowV1] = []
    for unit in sorted(units, key=lambda u: u.envelope.component_key):
        nodes = _walk_nodes(unit.process_ir)
        map_nodes = [node for node in nodes if getattr(node, "kind", None) == "map_ref"]
        for position, node in enumerate(map_nodes):
            map_key = _ref_key(getattr(node, "map_ref", None) or getattr(node, "component_ref", None))
            component = by_key.get(map_key) if map_key else None
            config = (component.config or {}) if component is not None else {}
            source_component = by_key.get(_ref_key(config.get("source_profile_id")) or "")
            target_component = by_key.get(_ref_key(config.get("target_profile_id")) or "")
            source_gen = _generated_profile(source_component) if source_component else None
            target_gen = _generated_profile(target_component) if target_component else None

            # the feeding step: the nearest preceding connector node
            source_token = ""
            for previous in nodes[: nodes.index(node)][::-1]:
                if getattr(previous, "kind", None) in ("source", "connector_call"):
                    conn_key = _ref_key(getattr(previous, "connection_ref", None))
                    family = (metadata.get(conn_key) or (None, None))[0] if conn_key else None
                    if family:
                        source_token = SOURCE_TOKEN_BY_FAMILY.get(str(family).lower(), "")
                    break

            if source_gen is not None and source_gen["component_type"] == "profile.db":
                source_schema: Dict[str, Any] = _db_schema_summary(
                    source_gen["field_index_by_path"], source_component.config or {}
                )
            elif source_gen is not None:
                source_schema = _leaves_summary(source_gen["field_index_by_path"])
            else:
                source_schema = {"field_count": 0, "fields": []}

            direct = []
            if source_gen is not None and target_gen is not None:
                try:
                    direct = validate_field_mappings(
                        source_gen["field_index_by_path"],
                        target_gen["field_index_by_path"],
                        [
                            {"source_field": e.get("source_path"), "target_path": e.get("target_path")}
                            for e in (config.get("field_mappings") or ())
                            if isinstance(e, Mapping)
                        ],
                    )
                except Exception:  # noqa: BLE001 — the map validator owns the refusal
                    direct = []

            row: Dict[str, Any] = {
                "key": "transform" if len(map_nodes) == 1 else "transform/{0}".format(position + 1),
                "name": getattr(node, "label", None) or (component.name if component else "") or "",
                "source": source_token,
                "target": None,
                "operation": "transform",
                "executable": False,
                "source_schema": source_schema,
                "target_payload_profile": (
                    _leaves_summary(target_gen["field_index_by_path"])
                    if target_gen is not None and target_gen["component_type"] == "profile.json"
                    else None
                ),
                "operations": _operation_summaries(config),
                "source_profile_generation": source_gen,
                "target_profile_generation": target_gen,
                "direct_field_mappings": direct,
            }
            rows.append(typed_row_from_legacy(row))
    return tuple(rows)


__all__ = [
    "FUTURE_BUILDER_ISSUE_BY_ROUTE",
    "NORMALIZATION_RULES",
    "SOURCE_TOKEN_BY_FAMILY",
    "derive_transform_flows",
    "flow_row_digest",
    "normalize_flow_row",
    "normalize_operation_summary",
    "typed_row_from_legacy",
]
