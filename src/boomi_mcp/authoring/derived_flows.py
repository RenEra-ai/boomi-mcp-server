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
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, get_args

from ..categories.components.builders.transform_map_validation import mapping_entries
from ..models.derived_flows import (
    DerivedTransformFlowV1,
    FieldIndexEntryV1,
    GeneratedProfileSummaryV1,
)
from ..models.integration_models import IntegrationComponentSpec
from ..models.process_ir import _CONNECTOR_KINDS

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
    "R3 *_profile_generation.component_name dropped FOR COMPARISON ONLY: the "
    "legacy DB form passed None and the API form a derived default, so an "
    "equality check between the two must ignore it — the SERVED row keeps the "
    "name, which its own model declares a field for",
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


def _normalize_generation(artifact: Any, *, for_comparison: bool = True) -> Any:
    if not isinstance(artifact, Mapping):
        return _plain(artifact)
    if not for_comparison:
        # THE SERVED PAYLOAD KEEPS THE NAME. R3 exists because the two LEGACY
        # producers disagree with each other — one passes None, the other a
        # derived default — so an equality check between them has to ignore it.
        # That is a reconciliation of the COMPARISON, and applying it to the
        # served row discarded content the row's own model declares a field for
        # (architect review, item 2).
        return {str(k): _plain(v) for k, v in artifact.items()}
    out = {k: _plain(v) for k, v in artifact.items() if k != "component_name"}  # R3
    return out


def _plain(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(k): _plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(v) for v in value]
    return value


def normalize_flow_row(
    row: Mapping[str, Any], *, for_comparison: bool = True
) -> Dict[str, Any]:
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
            out[key] = _normalize_generation(value, for_comparison=for_comparison)
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
    return DerivedTransformFlowV1.model_validate(
        normalize_flow_row(row, for_comparison=False)
    )


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


def _authored_pointers(root: Any) -> Dict[str, Any]:
    """Every authored node by its JSON pointer — the ``source_path`` the CFG carries."""
    from pydantic import BaseModel

    found: Dict[str, Any] = {}

    def _visit(node: Any, pointer: str) -> None:
        found[pointer] = node
        for name in type(node).model_fields:
            child = getattr(node, name, None)
            if isinstance(child, BaseModel):
                _visit(child, "{0}/{1}".format(pointer, name))
            elif isinstance(child, (list, tuple)):
                for index, item in enumerate(child):
                    if isinstance(item, BaseModel):
                        _visit(item, "{0}/{1}/{2}".format(pointer, name, index))

    if isinstance(root, BaseModel):
        _visit(root, "")
    return found


def _feeding_nodes(root: Any, pointers: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    """For every ``map_ref``, the connector node that feeds it — read off the CFG.

    #158 (CDX-158-r2-01, CDX-158-r3-01, CDX-158-r3-02): "the nearest preceding
    connector" is a question about CONTROL FLOW, and the compiler's CFG is its
    authority. Two models of it failed here in turn: the flattened document
    order crossed sibling branch legs, and an authored-tree walk missed that a
    ``continue`` hands the successful try body's stream to the next step, and
    that a container's terminal follows its steps. The CFG has none of these to
    model — a ``continue`` is an ordering edge from the try body's last step,
    and a catch path hangs off its own ``catch`` edge — so a depth-first walk
    carrying the last connector node down each path answers exactly. None when
    the document does not lower (the preview then claims no source).
    """
    from ..compiler.process_ir.lowering import lower_process_ir_to_cfg

    try:
        cfg = lower_process_ir_to_cfg(root)
    except Exception:
        return None
    by_id = {node.node_id: node for node in cfg.nodes}
    outgoing: Dict[str, List[str]] = {}
    for edge in cfg.edges:
        outgoing.setdefault(edge.source_node_id, []).append(edge.target_node_id)
    feeders: Dict[str, Any] = {}
    stack: List[Tuple[str, Any]] = [(cfg.entry_node_id, None)]
    while stack:
        node_id, feeder = stack.pop()
        authored = pointers.get(by_id[node_id].source_path)
        kind = getattr(authored, "kind", None)
        if kind == "map_ref":
            feeders[by_id[node_id].source_path] = feeder
        elif kind in _CONNECTOR_KINDS:
            feeder = authored
        for target_id in outgoing.get(node_id, ()):
            stack.append((target_id, feeder))
    return feeders


def _source_token_for(feeder: Any, by_key: Mapping[str, Any], metadata: Mapping[str, Any]) -> str:
    """The served source token for the connector node feeding a map ("" if none).

    EACH NODE KIND CARRIES ITS OWN REFERENCE. A `source` names the connection
    directly; a `connector_call` names only the OPERATION, and the operation
    names the connection. Reading `connection_ref` off both meant every map fed
    by a canonical connector call resolved no family and served an empty
    source, even where the operation's family was known.
    """
    if feeder is None:
        return ""
    conn_key = _ref_key(getattr(feeder, "connection_ref", None))
    if conn_key is None:
        op_key = _ref_key(getattr(feeder, "operation_ref", None))
        operation = by_key.get(op_key) if op_key else None
        op_config = (getattr(operation, "config", None) or {}) if operation else {}
        conn_key = _ref_key(op_config.get("connection_ref_key")) or (
            op_config.get("connection_ref_key")
            if isinstance(op_config.get("connection_ref_key"), str)
            else None
        )
        if conn_key is None and operation is not None:
            # An operation-only node (a WSS listener's operation has no
            # connection): its family, through the resolver the builders route
            # on, so an alias such as `web_services` names the same family as `wss`.
            family = str(op_config.get("connector_type") or "") or None
            if family:
                from ..categories.components.builders.connector_builder import (
                    connector_family_of,
                )

                resolved = connector_family_of(family) or family.strip().lower()
                return SOURCE_TOKEN_BY_FAMILY.get(resolved, "")
    family = (metadata.get(conn_key) or (None, None))[0] if conn_key else None
    return SOURCE_TOKEN_BY_FAMILY.get(str(family).lower(), "") if family else ""


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


def _db_schema_summary(
    index: Mapping[str, Mapping[str, Any]],
    config: Mapping[str, Any],
    from_selected_artifact: bool = False,
) -> Dict[str, Any]:
    """The DB field summary, taken from ONE authority.

    ``from_selected_artifact`` says the index came from the account, and then
    the candidate config is not consulted at all: reading the required flags off
    it beside the artifact's own fields served a row whose two halves
    contradicted each other — the same field `required: true` in one and
    `required: false` in the other (architect evaluation 3, e3-03).
    """
    fields = []
    declared = {}
    if not from_selected_artifact:
        for entry in mapping_entries(config.get("output_fields") or config.get("fields")):
            if isinstance(entry, Mapping) and isinstance(entry.get("name"), str):
                declared[entry["name"]] = entry
    for name, record in index.items():
        source = declared.get(name, {})
        fields.append(
            {
                "name": name,
                "data_type": record.get("data_type") or "",
                "required": bool(
                    record.get("required", False)
                    if from_selected_artifact
                    else source.get("required", source.get("mandatory", False))
                ),
            }
        )
    return {"field_count": len(fields), "fields": fields}


def _selected_or_generated(
    component: Optional[IntegrationComponentSpec],
    key: str,
    selected: Mapping[str, Mapping[str, Any]],
    final_names: Optional[Mapping[str, str]] = None,
) -> Optional[Dict[str, Any]]:
    """The SELECTED artifact's index when there is one, else the generator's.

    A component this request will reuse is described by the account, not by the
    candidate config the request happens to carry — the same authority rule the
    coverage gate follows (decision D4). Applying it here too keeps the served
    projection from describing fields the reused profile does not have.
    """
    if key and key in selected:
        # THE REQUEST WILL NOT WRITE THIS PROFILE, so nothing the request says
        # about it is evidence — not its field shape, not its type, not its
        # name. Either the account's own artifact describes it or nothing does.
        artifact = selected[key]
        if not isinstance(artifact, Mapping):
            return None
        index = artifact.get("field_index_by_path")
        if not isinstance(index, Mapping):
            return None
        component_type = artifact.get("profile_component_type")
        if component_type not in _SUMMARISABLE_PROFILE_TYPES:
            # OPAQUE RATHER THAN WRONG. This summary's model carries the two
            # profile families the surviving generators emit; a reused
            # `profile.xml` has an index but no representation here, and
            # building one anyway made its `component_type` unrepresentable and
            # failed the whole preview — for a plan that used to succeed
            # (Stage-2 round 9). Extending the projection to XML profiles is a
            # capability, not a correction.
            return None
        served = _served_index_entries(index)
        return {
            "evidence_source": "selected_artifact",
            "component_type": component_type,
            "field_index_by_path": served,
            "mappable_paths": sorted(
                path for path, entry in served.items() if entry.get("mappable", True)
            ),
        }
    return (
        _generated_profile(component, (final_names or {}).get(key))
        if component is not None
        else None
    )


#: The profile families `GeneratedProfileSummaryV1` can carry, read FROM the
#: model rather than listed again here.
_SUMMARISABLE_PROFILE_TYPES = frozenset(
    get_args(GeneratedProfileSummaryV1.model_fields["component_type"].annotation)
)

#: The keys `FieldIndexEntryV1` declares, read FROM the model. A live-indexed
#: profile's entries are a documented SUPERSET of the generator's — they carry
#: `key`, `key_path`, `name_path`, `is_mappable` and `structural` for the map
#: builder and the review — and the served model forbids extras, so copying one
#: straight through made every plan over a reused profile fail to validate.
_SERVED_INDEX_ENTRY_KEYS = frozenset(FieldIndexEntryV1.model_fields)


def _served_index_entries(index: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Project a field index onto the SERVED entry schema, key set and all.

    Derived from the model rather than hand-listed: a field added to
    `FieldIndexEntryV1` is served without editing this, and a key the model does
    not declare cannot reach the payload however the index was built.
    """
    projected: Dict[str, Dict[str, Any]] = {}
    for path, entry in index.items():
        if not isinstance(entry, Mapping):
            continue
        projected[str(path)] = {
            str(k): _plain(v) for k, v in entry.items() if k in _SERVED_INDEX_ENTRY_KEYS
        }
    return projected


def _generated_profile(
    component: IntegrationComponentSpec, final_name: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """The surviving generator's artifact for an in-plan profile component.

    ``final_name`` is the name apply will actually give it, which differs from
    the authored one for a clone. Supplied by the caller from the apply-side
    naming authority rather than derived here.
    """
    from ..categories.components.builders.profile_generation import (
        profile_from_db_read_fields,
        profile_from_json_schema,
    )

    config = component.config or {}
    name = final_name or component.name or config.get("component_name")
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
    for entry in mapping_entries(map_config.get("field_mappings")):
        if isinstance(entry, Mapping):
            summary: Dict[str, Any] = {
                "operation_type": "direct",
                "source_path": entry.get("source_path"),
                "target_path": entry.get("target_path"),
            }
            if isinstance(entry.get("documentation_hint"), str):
                summary["documentation_hint"] = entry["documentation_hint"]
            summaries.append(summary)
    for entry in mapping_entries(map_config.get("function_mappings")):
        if isinstance(entry, Mapping):
            inputs = list(mapping_entries(entry.get("inputs")))
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
    for entry in mapping_entries(map_config.get("script_mappings")):
        if isinstance(entry, Mapping):
            inputs = [
                item.get("source_path")
                for item in mapping_entries(entry.get("inputs"))
                if isinstance(item, Mapping)
            ]
            outputs = [
                item.get("target_path")
                for item in mapping_entries(entry.get("outputs"))
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
    selected_artifacts: Optional[Mapping[str, Mapping[str, Any]]] = None,
    reused_keys: Optional[Any] = None,
    final_names: Optional[Mapping[str, str]] = None,
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
    #: A REUSED profile's candidate config describes a component this request
    #: will not write, so a row generated from it can describe fields the
    #: account never held — and omit fields it does (architect review, item 2).
    #: Where the selected artifact's own index is in hand, it wins.
    selected = dict(selected_artifacts or {})
    #: A REUSED MAP IS OPAQUE TO THE PROJECTION TOO. Its in-spec mappings
    #: describe a component this request will not write, so serving them
    #: published a destination the account may never bind — the same rule the
    #: coverage gate follows, applied to the row that DESCRIBES the map
    #: (architect evaluation 2, finding 2).
    reused = frozenset(reused_keys or ())
    rows: List[DerivedTransformFlowV1] = []
    for unit in sorted(units, key=lambda u: u.envelope.component_key):
        nodes = _walk_nodes(unit.process_ir)
        pointers = _authored_pointers(unit.process_ir)
        pointer_of = {id(node): pointer for pointer, node in pointers.items()}
        feeders = _feeding_nodes(unit.process_ir, pointers) or {}
        map_nodes = [node for node in nodes if getattr(node, "kind", None) == "map_ref"]
        for position, node in enumerate(map_nodes):
            map_key = _ref_key(getattr(node, "map_ref", None) or getattr(node, "component_ref", None))
            component = by_key.get(map_key) if map_key else None
            opaque = bool(map_key) and map_key in reused
            config = {} if opaque else ((component.config or {}) if component is not None else {})
            source_key = _ref_key(config.get("source_profile_id")) or ""
            target_key = _ref_key(config.get("target_profile_id")) or ""
            source_component = by_key.get(source_key)
            target_component = by_key.get(target_key)
            source_gen = (
                None
                if opaque
                else _selected_or_generated(source_component, source_key, selected, final_names)
            )
            target_gen = (
                None
                if opaque
                else _selected_or_generated(target_component, target_key, selected, final_names)
            )

            # the feeding step: the connector node the CFG says feeds this map
            # (see `_feeding_nodes`). #158 CDX-158-r1-02: "connector node" is
            # the MODEL's set, so a `listener` feeds a map like any source.
            source_token = _source_token_for(
                feeders.get(pointer_of.get(id(node), "")), by_key, metadata
            )

            if source_gen is not None and source_gen["component_type"] == "profile.db":
                source_schema: Dict[str, Any] = _db_schema_summary(
                    source_gen["field_index_by_path"],
                    source_component.config or {} if source_component is not None else {},
                    from_selected_artifact=source_gen.get("evidence_source") == "selected_artifact",
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
                            for e in mapping_entries(config.get("field_mappings"))
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


# ---------------------------------------------------------------------------
# the recipe-route projection (#159 S1)
# ---------------------------------------------------------------------------


#: The fixed SPELLINGS of the recipe-route rows: display names, and the row key
#: each one is served under. Content-free strings the legacy producers served
#: verbatim; pinned against the frozen #157 composition rows by
#: ``test_the_recipe_row_spellings_are_the_frozen_legacy_spellings``.
RECIPE_FLOW_ROW_SPELLINGS: Mapping[str, str] = {
    "extract": "Read from database",
    "send": "Send to REST target",
    "reliability": "Retry / DLQ",
    "watermark": "Advance watermark",
    "fanout": "Fan out to REST targets",
}

#: Connector family -> the endpoint token a route serves for it, per direction.
#: Closed and pinned like ``SOURCE_TOKEN_BY_FAMILY``: a route over a fourth
#: family is a decision here, not a silent empty token.
ENDPOINT_TOKEN_BY_DIRECTION_AND_FAMILY: Mapping[Tuple[str, str], str] = {
    ("source", "database"): "db_source",
    ("target", "rest"): "rest_target",
}

#: Connector family -> the connector-row ``operation`` for its direction.
CONNECTOR_OPERATION_BY_DIRECTION_AND_FAMILY: Mapping[Tuple[str, str], str] = {
    ("source", "database"): "db_get",
    ("target", "rest"): "rest_send",
}

#: The DLQ token the reliability row serves when the root wires a catch path.
_DLQ_TOKEN = "dlq"


class RecipeProjection:
    """The served projection of one plan: rows, endpoints, composition summaries."""

    __slots__ = ("flows", "endpoints", "composition_summaries")

    def __init__(self, flows, endpoints, composition_summaries) -> None:
        self.flows = tuple(flows)
        self.endpoints = tuple(endpoints)
        self.composition_summaries = dict(composition_summaries)


def _served_rest_auth(config: Mapping[str, Any]) -> Optional[str]:
    """A created REST connection's ``auth`` translated to the SERVED vocabulary.

    Inverted from the ONE create-auth authority the primitives emit through, so
    a new create-mode auth arrives here without a second table.
    """
    from ..patterns.archetype_assembly import _REST_CREATE_AUTH_MAP

    inverse = {wire: served for served, wire in _REST_CREATE_AUTH_MAP.items()}
    value = config.get("auth")
    return inverse.get(value) if isinstance(value, str) else None


def _family_of(component: Optional[IntegrationComponentSpec], metadata) -> Optional[str]:
    if component is None:
        return None
    family = (metadata.get(component.key) or (None, None))[0]
    if not family:
        family = (component.config or {}).get("connector_type")
    return str(family).lower() if family else None


def _endpoint_row(
    key: str,
    direction: str,
    connection: IntegrationComponentSpec,
    family: str,
    *,
    method: Optional[str] = None,
):
    """One endpoint row, read off the connection component (never the input)."""
    from ..models.derived_flows import DerivedEndpointRowV1

    config = connection.config or {}
    reuse = bool(config.get("reference_only"))
    row: Dict[str, Any] = {
        "key": key,
        "type": family,
        "direction": direction,
        "binding_mode": "reuse" if reuse else "create",
        "executable": False,
    }
    if method is not None:
        row["method"] = method
    if reuse:
        component_id = connection.component_id or config.get("component_id")
        if component_id:
            row["component_id"] = component_id
        name = config.get("component_name")
        if name:
            row["component_name"] = name
    elif family == "database":
        row["driver"] = config.get("driver_id")
        row["auth_mode"] = config.get("auth_mode")
    else:
        row["auth_mode"] = _served_rest_auth(config)
    return DerivedEndpointRowV1(**row)


def _root_steps(unit: Any) -> List[Any]:
    body = getattr(unit.process_ir, "body", None)
    return list(getattr(body, "steps", None) or ())


def _branch_of(unit: Any) -> Optional[Any]:
    """The route's terminal Branch: the LAST step of the root's sequence."""
    steps = _root_steps(unit)
    if steps and getattr(steps[-1], "kind", None) == "branch":
        return steps[-1]
    return None


def _leg_positions(branch: Any, facts: Any):
    """Per target part: its ACTUAL 1-based leg, its handoff, and the staging leg."""
    positions: Dict[str, Tuple[int, str]] = {}
    staging: Optional[int] = None
    cache_key: Optional[str] = None
    by_operation = {
        "$ref:" + target.operation_key: target.part_key for target in facts.targets
    }
    for index, leg in enumerate(branch.legs, start=1):
        terminal = leg.terminal
        kind = getattr(terminal, "kind", None)
        if kind == "cache_put" and staging is None:
            staging = index
            cache_key = _ref_key(getattr(terminal, "cache_ref", None))
            continue
        part = by_operation.get(getattr(terminal, "operation_ref", None))
        if part is None:
            continue
        staged = any(getattr(step, "kind", None) == "cache_get" for step in leg.steps)
        positions[part] = (index, "document_cache" if staged else "document_stream")
    return positions, staging, cache_key


def _with_hints(row: Any, facts: Any) -> Any:
    """The transform row with the route's ``documentation_hint`` facts applied.

    A hint the map component already carries wins (it is the same content);
    otherwise each operation summary gains the hint its route and target path
    name. Rows the facts name nothing for are returned unchanged.
    """
    hints = {
        (hint.operation_type, hint.target_path): hint.documentation_hint
        for hint in getattr(facts, "documentation_hints", ()) or ()
    }
    if not hints or not getattr(row, "operations", None):
        return row
    payload = row.model_dump(mode="json", exclude_unset=True)
    changed = False
    for summary in payload.get("operations") or ():
        targets = summary.get("outputs") or [summary.get("target_path")]
        for target in targets:
            hint = hints.get((summary.get("operation_type"), target))
            if hint is not None and summary.get("documentation_hint") is None:
                summary["documentation_hint"] = hint
                changed = True
                break
    return type(row).model_validate(payload) if changed else row


def _route_projection(unit, by_key, metadata, facts, transform_rows, watermarked):
    """``(rows, endpoints, summary)`` for one fact-carrying unit, or None.

    None — and the unit is then projected as before, transform rows only —
    whenever a fact cannot be resolved against the composed root and
    components: the projection never serves a row it could not derive.
    """
    from ..models.derived_flows import (
        DerivedCompositionSummaryV1,
        DerivedConnectorFlowV1,
        DerivedFanoutFlowV1,
        DerivedReliabilityFlowV1,
        DerivedWatermarkFlowV1,
    )
    from ..patterns.primitives._helpers import slugify

    branch = _branch_of(unit)
    source_connection = by_key.get(facts.source_connection_key)
    source_family = _family_of(source_connection, metadata)
    target_components = [
        (target, by_key.get(target.connection_key), by_key.get(target.operation_key))
        for target in facts.targets
    ]
    if branch is None or source_connection is None or any(
        connection is None or operation is None for _t, connection, operation in target_components
    ):
        return None
    target_family = _family_of(target_components[0][1], metadata)
    source_token = ENDPOINT_TOKEN_BY_DIRECTION_AND_FAMILY.get(("source", source_family or ""))
    target_token = ENDPOINT_TOKEN_BY_DIRECTION_AND_FAMILY.get(("target", target_family or ""))
    if source_token is None or target_token is None:
        return None
    positions, staging, cache_key = _leg_positions(branch, facts)
    if set(positions) != {target.part_key for target in facts.targets}:
        return None

    transform_rows = [_with_hints(row, facts) for row in transform_rows]
    transform_key = transform_rows[0].key if transform_rows else "transform"
    has_catch = any(
        getattr(node, "kind", None) == "try_catch" for node in _walk_nodes(unit.process_ir)
    )
    rows: List[Any] = [
        DerivedConnectorFlowV1(
            key="extract",
            name=RECIPE_FLOW_ROW_SPELLINGS["extract"],
            source=source_token,
            target=None,
            operation=CONNECTOR_OPERATION_BY_DIRECTION_AND_FAMILY[("source", source_family)],
            executable=False,
        ),
        *transform_rows,
        DerivedConnectorFlowV1(
            key="send",
            name=RECIPE_FLOW_ROW_SPELLINGS["send"],
            source=transform_key,
            target=target_token,
            operation=CONNECTOR_OPERATION_BY_DIRECTION_AND_FAMILY[("target", target_family)],
            executable=False,
        ),
        DerivedReliabilityFlowV1(
            key="reliability",
            name=RECIPE_FLOW_ROW_SPELLINGS["reliability"],
            source="send",
            target=_DLQ_TOKEN if has_catch else None,
            operation="reliability",
            executable=False,
        ),
    ]
    if watermarked:
        rows.append(
            DerivedWatermarkFlowV1(
                key="watermark",
                name=RECIPE_FLOW_ROW_SPELLINGS["watermark"],
                source="send",
                target=None,
                operation="watermark",
                executable=False,
            )
        )
    rows.append(
        DerivedFanoutFlowV1(
            key="fanout",
            name=RECIPE_FLOW_ROW_SPELLINGS["fanout"],
            source=transform_key,
            target=target_token,
            operation="branch",
            executable=False,
            legs=len(branch.legs),
        )
    )

    endpoints = [_endpoint_row(source_token, "source", source_connection, source_family)]
    summaries_legs = []
    modes: Dict[str, str] = {}
    binding_modes: Dict[str, str] = {}
    for index, (target, connection, operation) in enumerate(target_components):
        method = (operation.config or {}).get("method")
        endpoint = _endpoint_row(
            target_token if index == 0 else "{0}_{1}".format(target_token, slugify(target.part_key)),
            "target",
            connection,
            target_family,
            method=method,
        )
        endpoints.append(endpoint)
        binding_modes[target.part_key] = endpoint.binding_mode
        leg, mode = positions[target.part_key]
        modes[target.part_key] = mode
        if index == 0:
            continue
        leg_summary: Dict[str, Any] = {
            "part_key": target.part_key,
            "leg": leg,
            "method": method,
            "binding_mode": endpoint.binding_mode,
        }
        if target.expected_status_codes:
            leg_summary["expected_status_codes"] = tuple(target.expected_status_codes)
        if staging is not None:
            leg_summary["handoff"] = mode
        summaries_legs.append(leg_summary)
    staged = staging is not None
    summary: Dict[str, Any] = {
        "topology": facts.topology,
        "handoff": (
            "document_stream"
            if not staged
            else (
                "document_cache"
                if all(mode == "document_cache" for mode in modes.values())
                else "mixed"
            )
        ),
        "fanout_targets": len(facts.targets),
        "parts": [part.model_dump() for part in facts.parts],
        "first_target_part": facts.targets[0].part_key,
        "fanout_legs": summaries_legs,
    }
    if staged:
        summary["cache_component_key"] = cache_key
        summary["staging_leg"] = staging
        summary["handoff_modes"] = dict(modes)
    return rows, endpoints, DerivedCompositionSummaryV1(**summary)


def derive_recipe_projection(
    units: Sequence[Any],
    components: Sequence[IntegrationComponentSpec],
    *,
    connector_metadata: Optional[Mapping[str, Tuple[Optional[str], Optional[str]]]] = None,
    selected_artifacts: Optional[Mapping[str, Mapping[str, Any]]] = None,
    reused_keys: Optional[Any] = None,
    final_names: Optional[Mapping[str, str]] = None,
    projection_facts: Optional[Mapping[str, Any]] = None,
    recorded_intents: Sequence[Any] = (),
) -> RecipeProjection:
    """The whole served projection, per unit in key order (#159 S1, C8).

    Every unit serves its transform rows exactly as
    :func:`derive_transform_flows` does. A unit whose recipe route supplied
    trusted projection facts ALSO serves, around them, its extract, send,
    reliability, conditional watermark and fan-out rows, its endpoint rows and
    one composition summary keyed by its process key. The facts are the
    trigger and name the parts; every served value is derived from the root
    and components. Without facts the result is exactly the pre-#159 one.
    """
    facts_by_key = dict(projection_facts or {})
    metadata = dict(connector_metadata or {})
    by_key = {component.key: component for component in components}
    from ..models.governance_intent import WatermarkDeclarationV1

    watermarked = {
        record.process_key
        for record in recorded_intents or ()
        if isinstance(getattr(record, "declaration", None), WatermarkDeclarationV1)
    }
    flows: List[Any] = []
    endpoints: List[Any] = []
    summaries: Dict[str, Any] = {}
    for unit in sorted(units, key=lambda u: u.envelope.component_key):
        key = unit.envelope.component_key
        transform_rows = list(
            derive_transform_flows(
                [unit],
                components,
                connector_metadata=metadata,
                selected_artifacts=selected_artifacts,
                reused_keys=reused_keys,
                final_names=final_names,
            )
        )
        facts = facts_by_key.get(key)
        route = (
            _route_projection(unit, by_key, metadata, facts, transform_rows, key in watermarked)
            if facts is not None
            else None
        )
        if route is None:
            flows.extend(transform_rows)
            continue
        rows, unit_endpoints, summary = route
        flows.extend(rows)
        endpoints.extend(unit_endpoints)
        summaries[key] = summary
    return RecipeProjection(flows, endpoints, summaries)


__all__ = [
    "CONNECTOR_OPERATION_BY_DIRECTION_AND_FAMILY",
    "ENDPOINT_TOKEN_BY_DIRECTION_AND_FAMILY",
    "FUTURE_BUILDER_ISSUE_BY_ROUTE",
    "NORMALIZATION_RULES",
    "RECIPE_FLOW_ROW_SPELLINGS",
    "RecipeProjection",
    "SOURCE_TOKEN_BY_FAMILY",
    "derive_recipe_projection",
    "derive_transform_flows",
    "flow_row_digest",
    "normalize_flow_row",
    "normalize_operation_summary",
    "typed_row_from_legacy",
]
