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
            return SOURCE_TOKEN_BY_FAMILY.get(_operation_family(operation) or "", "")
    family = (metadata.get(conn_key) or (None, None))[0] if conn_key else None
    return SOURCE_TOKEN_BY_FAMILY.get(_resolved_family(family) or "", "")


def _resolved_family(connector_type: Any) -> Optional[str]:
    """A declared ``connector_type`` -> its canonical family, or None.

    THE one resolution every family-keyed table here is read through:
    ``connector_family_of``, the resolver the builders route on. Lowercasing the
    raw spelling instead read a supported alias (``rest_client``, the canonical
    REST subtype) as a family no table names, so its route lost every connector
    row and endpoint (#159 architect review r1, finding 8). Every family the
    tables key on is one this resolver returns; anything else resolves to None.
    """
    from ..categories.components.builders.connector_builder import connector_family_of

    return connector_family_of(connector_type) if connector_type else None


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
    # #159 S2: the linear presets' rows
    "fetch": "Fetch from REST source",
    "listen": "Listen for inbound HTTP payloads",
    "write": "Write to database target",
    "stub_noop_flow": "Stub no-op flow",
}

#: Connector family -> the endpoint token a route serves for it, per direction.
#: Closed and pinned like ``SOURCE_TOKEN_BY_FAMILY``: a route over a fourth
#: family is a decision here, not a silent empty token.
ENDPOINT_TOKEN_BY_DIRECTION_AND_FAMILY: Mapping[Tuple[str, str], str] = {
    ("source", "database"): "db_source",
    ("target", "rest"): "rest_target",
    # #159 S2
    ("source", "rest"): "rest_source",
    ("source", "wss"): "wss_listener",
    ("target", "database"): "database_target",
}

#: Connector family -> the connector-row ``operation`` for its direction.
CONNECTOR_OPERATION_BY_DIRECTION_AND_FAMILY: Mapping[Tuple[str, str], str] = {
    ("source", "database"): "db_get",
    ("target", "rest"): "rest_send",
    # #159 S2
    ("source", "rest"): "rest_fetch",
    ("source", "wss"): "wss_listen",
    ("target", "database"): "db_write",
}

#: Connector family -> the connector ROW KEY for its direction (#159 S2). The
#: source side IS the token the transform row names its feeding step by
#: (``SOURCE_TOKEN_BY_FAMILY``), so it is derived from that table, not retyped.
CONNECTOR_ROW_KEY_BY_DIRECTION_AND_FAMILY: Mapping[Tuple[str, str], str] = {
    **{("source", family): token for family, token in SOURCE_TOKEN_BY_FAMILY.items()},
    ("target", "rest"): "send",
    ("target", "database"): "write",
}

#: The stub route's two placeholder endpoints: ``(key, label)`` per direction.
STUB_ENDPOINTS: Mapping[str, Tuple[str, str]] = {
    "source": ("stub_source", "Stub source"),
    "target": ("stub_target", "Stub target"),
}

#: A database write operation's ``operation_mode`` -> the served ``operation``
#: spelling on its endpoint row (the legacy DB-write producers' spelling).
DB_WRITE_OPERATION_SPELLINGS: Mapping[str, str] = {"send": "Send"}

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
    return _resolved_family(family)


def _endpoint_row(
    key: str,
    direction: str,
    connection: IntegrationComponentSpec,
    family: str,
    *,
    method: Optional[str] = None,
    reused: bool = False,
    existing_ids: Optional[Mapping[str, Any]] = None,
    extra: Optional[Mapping[str, Any]] = None,
):
    """One endpoint row, read off the connection component (never the input).

    B12-01/04: the binding mode is what apply DOES — ``reused`` carries the
    component plan's answer (a create it resolved by name is reused) — and the
    served selector follows the planner's own binding rule
    (``resolve_planner_binding``: a whitespace-only id is absent, the
    top-level name precedes ``config.component_name``). The served id is the
    one the route BINDS — ``_bound_existing_id`` with the component plan's
    resolved ids (``existing_ids``) when the route has them."""
    from ..categories.integration_builder import _bound_existing_id, _first_nonblank_str
    from ..models.derived_flows import DerivedEndpointRowV1

    config = connection.config or {}
    reuse = bool(config.get("reference_only")) or reused
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
        component_id = _bound_existing_id(connection, existing_ids)
        if component_id:
            row["component_id"] = component_id
        if _bound_existing_id(connection, None):
            # A DECLARED id binds; a configured name is served beside it.
            name = _first_nonblank_str(config.get("component_name"))
        else:
            # The name drove the lookup (even when the plan resolved an id),
            # so it stays served beside any resolved id (B12 review).
            # ARCH-r1-07: a name-only binding resolves by the TOP-LEVEL name
            # first, then config.component_name — the planner's precedence.
            name = _first_nonblank_str(connection.name, config.get("component_name"))
        if name:
            row["component_name"] = name
    elif family == "database":
        row["driver"] = config.get("driver_id")
        row["auth_mode"] = config.get("auth_mode")
    else:
        row["auth_mode"] = _served_rest_auth(config)
    # #159 S2: keys read off OTHER components of the same step (the write
    # profile's statement type, the operation's mode, a declared status set).
    row.update(dict(extra or {}))
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


def recipe_leg_ordinals(unit: Any, facts: Any) -> Tuple[Tuple[str, int], ...]:
    """``(part_key, ACTUAL 1-based Branch ordinal)`` per fact-named target leg.

    The SAME derivation the served ``DerivedFanoutLegV1.leg`` reads
    (:func:`_leg_positions` over the composed root's terminal Branch), so a
    staging leg shifts these ordinals exactly as it shifts the served ones. The
    staging leg itself realizes no input part and is not listed. In ordinal
    order; empty when the root has no terminal Branch.
    """
    from ..models.derived_flows import RecipeUnitProjectionFactsV1

    branch = _branch_of(unit)
    if branch is None or not isinstance(facts, RecipeUnitProjectionFactsV1):
        # #159 S2: a linear or stub route has no fan-out leg identity.
        return ()
    positions, _staging, _cache_key = _leg_positions(branch, facts)
    return tuple(
        (part_key, ordinal)
        for part_key, (ordinal, _mode) in sorted(positions.items(), key=lambda item: item[1][0])
    )


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


def _route_projection(
    unit, by_key, metadata, facts, transform_rows, watermarked, reused_keys=frozenset(),
    existing_ids=None,
):
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
    if getattr(facts, "transform_row_name", None):
        transform_rows = [
            row.model_copy(update={"name": facts.transform_row_name}) for row in transform_rows
        ]
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

    endpoints = [
        _endpoint_row(
            source_token, "source", source_connection, source_family,
            reused=source_connection.key in reused_keys,
            existing_ids=existing_ids,
        )
    ]
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
            reused=connection.key in reused_keys,
            existing_ids=existing_ids,
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


# ---------------------------------------------------------------------------
# the linear-route projection (#159 S2)
# ---------------------------------------------------------------------------


def _operation_family(operation: Optional[IntegrationComponentSpec]) -> Optional[str]:
    """An operation-only step's family, through the resolver the builders route on."""
    if operation is None:
        return None
    return _resolved_family((operation.config or {}).get("connector_type"))


def _connector_node_index(nodes: Sequence[Any], operation_key: str) -> Optional[int]:
    """Document-order position of the ONE connector node binding ``operation_key``."""
    wanted = REF_PREFIX + operation_key
    found = [
        index
        for index, node in enumerate(nodes)
        if getattr(node, "kind", None) in _CONNECTOR_KINDS
        and getattr(node, "operation_ref", None) == wanted
    ]
    return found[0] if len(found) == 1 else None


def _root_references(nodes: Sequence[Any], key: str) -> bool:
    """Whether any node of the root references component ``key`` by any field."""
    wanted = REF_PREFIX + key
    for node in nodes:
        for name in type(node).model_fields:
            if getattr(node, name, None) == wanted:
                return True
    return False


def _dlq_is_wired(nodes: Sequence[Any], by_key: Mapping[str, Any], dlq: str) -> bool:
    """Whether the root writes the DLQ the facts name.

    Either form a ``ComponentRefV1`` takes: a plan component referenced as
    ``$ref:<key>`` (the key must resolve), or an EXISTING component referenced
    by its literal id (the legacy DB->REST DLQ is an existing document cache
    the producer never emitted) — the root must carry that literal verbatim.
    """
    if dlq in by_key and _root_references(nodes, dlq):
        return True
    for node in nodes:
        for name in type(node).model_fields:
            if getattr(node, name, None) == dlq:
                return True
    return False


def write_profile_summary(config: Mapping[str, Any]) -> Dict[str, Any]:
    """The served DB write-profile summary, read off the write profile's config.

    The index is ``DatabaseWriteProfileBuilder.build_field_index`` — the SAME
    index the map builder binds a DB-write target against — so the summary
    cannot describe a leaf the map cannot address.
    """
    from ..categories.components.builders.profile_builder import (
        DatabaseWriteProfileBuilder,
    )

    index = DatabaseWriteProfileBuilder.build_field_index(dict(config))
    return {
        "kind": "database.write",
        "statement_type": config.get("statement_type"),
        "field_count": sum(1 for path in index if path.startswith("Fields/")),
        "condition_count": sum(1 for path in index if path.startswith("Conditions/")),
        "target_paths": [
            {
                "path": path,
                "data_type": entry.get("data_type"),
                "required": bool(entry.get("required")),
            }
            for path, entry in sorted(index.items())
        ],
    }


def _is_write_profile(component: Optional[IntegrationComponentSpec]) -> bool:
    return (
        component is not None
        and component.type == "profile.db"
        and (component.config or {}).get("profile_type") == "database.write"
    )


#: The transform-row keys the ``schema_summary`` form serves (beside exactly
#: one of the two target summaries). The generation keys are ABSENT there.
_SCHEMA_SUMMARY_ROW_KEYS: Tuple[str, ...] = (
    "key", "name", "source", "target", "operation", "executable",
    "source_schema", "operations",
)


def _schema_summary_row(row: Any, map_component, by_key, reused) -> Optional[Any]:
    """The ``schema_summary`` transform row, built from the rich row and the
    map's target profile component. None when the target cannot be summarized."""
    if map_component is None or map_component.key in reused:
        return None
    payload = row.model_dump(mode="json", exclude_unset=True)
    out = {key: payload[key] for key in _SCHEMA_SUMMARY_ROW_KEYS if key in payload}
    target_key = _ref_key((map_component.config or {}).get("target_profile_id")) or ""
    target = by_key.get(target_key)
    if _is_write_profile(target) and target_key not in reused:
        out["target_write_profile"] = write_profile_summary(target.config or {})
    elif payload.get("target_payload_profile") is not None:
        out["target_payload_profile"] = payload["target_payload_profile"]
    else:
        return None
    return DerivedTransformFlowV1.model_validate(out)


def _apply_writes(
    component: IntegrationComponentSpec, reused_keys, existing_ids, conflict_policy
) -> Optional[bool]:
    """Whether apply WRITES this component's own configuration: True, False, or
    None when the answer depends on a conflict policy the caller did not hand in.

    Asked of apply's own predicate (``apply_writes_component_config``) with the
    component plan's resolved ids (``existing_ids``): a create matched by name
    under ``reuse`` is bound as it stands and its candidate configuration is
    discarded. A key the caller already answered as reused (``reused_keys``) is
    not written. Without a policy, the predicate is asked under EVERY policy the
    recipe intent accepts, and only a unanimous answer is an answer.
    """
    from ..categories.integration_builder import apply_writes_component_config

    if component.key in (reused_keys or ()):
        return False
    if conflict_policy is not None:
        policies: Tuple[str, ...] = (conflict_policy,)
    else:
        from typing import get_args

        from ..models.authoring_workflow import RecipeAuthoringIntentV1

        policies = get_args(RecipeAuthoringIntentV1.model_fields["conflict_policy"].annotation)
    answers = {
        bool(apply_writes_component_config(component, policy, existing_ids=existing_ids))
        for policy in policies
    }
    return answers.pop() if len(answers) == 1 else None


def _listener_endpoint_extra(
    operation: IntegrationComponentSpec,
    components: Sequence[IntegrationComponentSpec],
    process_key: str,
    reused_keys=frozenset(),
    existing_ids=None,
    conflict_policy=None,
) -> Optional[Dict[str, Any]]:
    """A WSS listener's served method and path, from the listener operation and
    — when one routes to this process — its API Service component.

    Bare WSS: ``wss_http_method`` and ``compute_wss_endpoint`` (the formulas the
    deploy-time ``listener_verify`` probe uses). Behind an API Service:
    ``effective_api_service_route`` over that component's route and the linked
    operation, served with ``publish_mode: api_service``.

    Both are read off CANDIDATE configuration, so each is served only when apply
    WRITES that configuration (:func:`_apply_writes`). An API Service apply
    binds without writing keeps its existing routes — the route read off the
    candidate names nothing apply wires (#159 architect review r1, finding 4) —
    and a listener operation apply binds without writing keeps its existing
    object and type. Either way the endpoint cannot be derived: None, and the
    bare-WSS formula is never offered in its place.
    """
    from ..categories.components.builders._api_service_paths import (
        effective_api_service_route,
        wss_http_method,
    )
    from ..patterns.primitives.wss_listen import compute_wss_endpoint

    if _apply_writes(operation, reused_keys, existing_ids, conflict_policy) is not True:
        return None
    op_config = dict(operation.config or {})
    wanted = REF_PREFIX + process_key
    services = []
    for component in components:
        if component.type != "webservice":
            continue
        config = component.config or {}
        for route in mapping_entries(config.get("routes")):
            if isinstance(route, Mapping) and route.get("process") == wanted:
                services.append((component, route))
    if len(services) > 1:
        return None
    if services:
        service, route = services[0]
        if _apply_writes(service, reused_keys, existing_ids, conflict_policy) is not True:
            return None
        config = service.config or {}
        resolved = effective_api_service_route(
            config.get("base_url_path"), dict(route), op_config
        )
        if not resolved.get("method"):
            return None
        return {
            "method": resolved["method"],
            "endpoint_path": resolved["path"],
            "publish_mode": "api_service",
        }
    object_name = str(op_config.get("object_name") or "").strip()
    operation_type = str(op_config.get("operation_type") or "").strip()
    if not object_name or not operation_type:
        return None
    return {
        "method": wss_http_method(op_config.get("input_type")),
        "endpoint_path": compute_wss_endpoint(operation_type, object_name),
    }


def _linear_endpoint(
    direction: str,
    token: str,
    family: str,
    connection: Optional[IntegrationComponentSpec],
    operation: IntegrationComponentSpec,
    fact: Any,
    by_key: Mapping[str, IntegrationComponentSpec],
    components: Sequence[IntegrationComponentSpec],
    process_key: str,
    reused_keys,
    existing_ids,
    conflict_policy=None,
):
    """One linear endpoint row, or None when a served key cannot be derived."""
    from ..models.derived_flows import DerivedEndpointRowV1

    op_config = operation.config or {}
    if family == "wss":
        extra = _listener_endpoint_extra(
            operation, components, process_key, reused_keys, existing_ids, conflict_policy
        )
        if extra is None:
            return None
        # Served only for an operation apply writes, so the binding is a create.
        return DerivedEndpointRowV1(
            key=token,
            type="wss",
            direction=direction,
            binding_mode="create",
            executable=False,
            **extra,
        )
    if connection is None:
        return None
    extra: Dict[str, Any] = {}
    method = None
    if family == "rest":
        method = op_config.get("method")
        if fact.expected_status_codes is not None:
            extra["expected_status_codes"] = tuple(fact.expected_status_codes)
    elif family == "database" and direction == "target":
        profile = by_key.get(_ref_key(op_config.get("write_profile_id")) or "")
        spelling = DB_WRITE_OPERATION_SPELLINGS.get(str(op_config.get("operation_mode") or ""))
        if not _is_write_profile(profile) or spelling is None:
            return None
        extra["operation"] = spelling
        extra["statement_type"] = (profile.config or {}).get("statement_type")
    return _endpoint_row(
        token,
        direction,
        connection,
        family,
        method=method,
        reused=connection.key in reused_keys,
        existing_ids=existing_ids,
        extra=extra,
    )


def _linear_route_projection(
    unit, by_key, components, metadata, facts, transform_rows, watermarked,
    reused_keys=frozenset(), existing_ids=None, conflict_policy=None,
):
    """``(rows, endpoints)`` for one linear-facts unit, or None.

    None — the unit is then projected transform rows only — whenever a fact
    does not resolve against the root and components: one ``map_ref``, the
    source connector node before it and the target connector node after it,
    both binding the operations the facts name, each family in the closed
    tables, and every endpoint key derivable.
    """
    from ..models.derived_flows import (
        DerivedConnectorFlowV1,
        DerivedReliabilityFlowV1,
        DerivedWatermarkFlowV1,
    )

    if len(transform_rows) != 1:
        return None
    nodes = _walk_nodes(unit.process_ir)
    map_positions = [i for i, node in enumerate(nodes) if getattr(node, "kind", None) == "map_ref"]
    source_at = _connector_node_index(nodes, facts.source.operation_key)
    target_at = _connector_node_index(nodes, facts.target.operation_key)
    if len(map_positions) != 1 or source_at is None or target_at is None:
        return None
    if not source_at < map_positions[0] < target_at:
        return None
    sides = []
    for direction, fact in (("source", facts.source), ("target", facts.target)):
        operation = by_key.get(fact.operation_key)
        connection = by_key.get(fact.connection_key) if fact.connection_key else None
        if operation is None or (fact.connection_key and connection is None):
            return None
        family = _family_of(connection, metadata) if connection is not None else _operation_family(operation)
        token = ENDPOINT_TOKEN_BY_DIRECTION_AND_FAMILY.get((direction, family or ""))
        row_key = CONNECTOR_ROW_KEY_BY_DIRECTION_AND_FAMILY.get((direction, family or ""))
        if token is None or row_key is None:
            return None
        endpoint = _linear_endpoint(
            direction, token, family, connection, operation, fact, by_key, components,
            unit.envelope.component_key, reused_keys, existing_ids, conflict_policy,
        )
        if endpoint is None:
            return None
        sides.append((direction, family, token, row_key, endpoint))
    (_sd, source_family, source_token, source_key, source_endpoint) = sides[0]
    (_td, target_family, target_token, target_key, target_endpoint) = sides[1]

    reused = frozenset(reused_keys)
    hints = facts.transform
    (transform,) = [_with_hints(row, hints) for row in transform_rows]
    if hints.row_name:
        transform = transform.model_copy(update={"name": hints.row_name})
    if hints.detail == "schema_summary":
        map_node = nodes[map_positions[0]]
        map_key = _ref_key(getattr(map_node, "map_ref", None)) or ""
        transform = _schema_summary_row(transform, by_key.get(map_key), by_key, reused)
        if transform is None:
            return None

    rows: List[Any] = [
        DerivedConnectorFlowV1(
            key=source_key,
            name=RECIPE_FLOW_ROW_SPELLINGS[source_key],
            source=source_token,
            target=None,
            operation=CONNECTOR_OPERATION_BY_DIRECTION_AND_FAMILY[("source", source_family)],
            executable=False,
        ),
        transform,
        DerivedConnectorFlowV1(
            key=target_key,
            name=RECIPE_FLOW_ROW_SPELLINGS[target_key],
            source=transform.key,
            target=target_token,
            operation=CONNECTOR_OPERATION_BY_DIRECTION_AND_FAMILY[("target", target_family)],
            executable=False,
        ),
    ]
    if facts.reliability is not None:
        dlq_key = facts.reliability.dlq_component_key
        if dlq_key is not None and not _dlq_is_wired(nodes, by_key, dlq_key):
            return None
        rows.append(
            DerivedReliabilityFlowV1(
                key="reliability",
                name=RECIPE_FLOW_ROW_SPELLINGS["reliability"],
                source=target_key,
                target=_DLQ_TOKEN if dlq_key is not None else None,
                operation="reliability",
                executable=False,
            )
        )
    if watermarked:
        rows.append(
            DerivedWatermarkFlowV1(
                key="watermark",
                name=RECIPE_FLOW_ROW_SPELLINGS["watermark"],
                source=target_key,
                target=None,
                operation="watermark",
                executable=False,
            )
        )
    return rows, [source_endpoint, target_endpoint]


def _stub_route_projection(unit, transform_rows):
    """``(rows, endpoints)`` for a stub-facts unit whose root IS Stop-only
    (``stop``, optionally after the ``passthrough`` entry), or None."""
    from ..models.derived_flows import DerivedEndpointRowV1, DerivedNoopFlowV1

    kinds = [getattr(step, "kind", None) for step in _root_steps(unit)]
    if kinds[:1] == ["passthrough"]:
        # The grammar's no-connector entry: a Stop-only root is authored as
        # ``passthrough -> stop`` (a bare ``stop`` does not parse).
        kinds = kinds[1:]
    if transform_rows or kinds != ["stop"]:
        return None
    source_key, source_label = STUB_ENDPOINTS["source"]
    target_key, target_label = STUB_ENDPOINTS["target"]
    rows = [
        DerivedNoopFlowV1(
            key="stub_noop_flow",
            name=RECIPE_FLOW_ROW_SPELLINGS["stub_noop_flow"],
            source=source_key,
            target=target_key,
            operation="noop",
            executable=False,
        )
    ]
    endpoints = [
        DerivedEndpointRowV1(key=source_key, type="stub", direction="source", label=source_label),
        DerivedEndpointRowV1(key=target_key, type="stub", direction="target", label=target_label),
    ]
    return rows, endpoints


def infer_direct_linear_facts(
    unit: Any,
    components: Sequence[IntegrationComponentSpec],
    connector_metadata: Optional[Mapping[str, Tuple[Optional[str], Optional[str]]]] = None,
):
    """Linear facts for a DIRECT root that is exactly REST ``source -> map_ref ->
    REST target`` (optionally ``-> stop``), or None (#159 S2).

    The retired API→API producer has no preparer, so its two frozen cases are
    replayed as direct canonical units and their facts are read off the root
    itself: the two connector nodes name the components and the transform keeps
    the rich form. No success-status expectation is inferred: a route's topology
    does not establish one and no component declares it, so the target endpoint
    serves ``expected_status_codes`` only when a trusted route fact declares it
    (CDX-159-S2-r1-02). Every other root shape — a DB source, a Branch, a
    second map — infers nothing and projects exactly as before.
    """
    from ..models.derived_flows import (
        LinearConnectorFactV1,
        LinearRouteProjectionFactsV1,
        LinearTransformFactV1,
    )

    steps = _root_steps(unit)
    kinds = [getattr(step, "kind", None) for step in steps]
    if kinds and kinds[-1] == "stop":
        kinds, steps = kinds[:-1], steps[:-1]
    if kinds != ["source", "map_ref", "target"]:
        return None
    by_key = {component.key: component for component in components}
    metadata = dict(connector_metadata or {})
    facts = {}
    for role, step in (("source", steps[0]), ("target", steps[2])):
        connection_key = _ref_key(getattr(step, "connection_ref", None))
        operation_key = _ref_key(getattr(step, "operation_ref", None))
        if not connection_key or not operation_key:
            return None
        if _family_of(by_key.get(connection_key), metadata) != "rest":
            return None
        facts[role] = LinearConnectorFactV1(
            connection_key=connection_key,
            operation_key=operation_key,
        )
    return LinearRouteProjectionFactsV1(
        process_key=unit.envelope.component_key,
        source=facts["source"],
        transform=LinearTransformFactV1(detail="generated_artifacts"),
        target=facts["target"],
    )


def infer_stub_facts(unit: Any):
    """Stub facts for a fact-less root that is EXACTLY ``passthrough -> stop``
    and references nothing, or None (#159 S2).

    The stub recipe has no preparer and so supplies no facts; like the API->API
    inference above, its facts are read off the root itself. The root must be
    the grammar's two-step Stop-only form, and the unit must depend on no
    component and no step may carry a component reference: anything else
    infers nothing and projects exactly as before.
    """
    from ..models.derived_flows import StubRouteProjectionFactsV1

    steps = _root_steps(unit)
    if [getattr(step, "kind", None) for step in steps] != ["passthrough", "stop"]:
        return None
    if tuple(getattr(unit.envelope, "depends_on", ()) or ()):
        return None
    for step in steps:
        for name in type(step).model_fields:
            value = getattr(step, name, None)
            if isinstance(value, str) and value.startswith(REF_PREFIX):
                return None
    return StubRouteProjectionFactsV1(process_key=unit.envelope.component_key)


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
    existing_ids: Optional[Mapping[str, Any]] = None,
    conflict_policy: Optional[str] = None,
) -> RecipeProjection:
    """The whole served projection, per unit in key order (#159 S1, C8).

    Every unit serves its transform rows exactly as
    :func:`derive_transform_flows` does. A unit whose recipe route supplied
    trusted projection facts ALSO serves, around them, its extract, send,
    reliability, conditional watermark and fan-out rows, its endpoint rows and
    one composition summary keyed by its process key. The facts are the
    trigger and name the parts; every served value is derived from the root
    and components. Without facts the result is exactly the pre-#159 one.

    ``conflict_policy`` is the request's policy, the one apply binds with. A
    served value read off candidate configuration apply may discard is served
    only when apply writes it under that policy — or, when it is not handed in,
    under every policy (see :func:`_apply_writes`).
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
    projected: List[Tuple[str, Any, Any]] = []
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
        if facts is None:
            # #159 S2: a DIRECT root that is exactly REST source -> map -> REST
            # target (the retired API->API producer's canonical form), or a
            # reference-free ``passthrough -> stop`` root (the stub), carries
            # its own facts; every other fact-less root infers none.
            facts = infer_direct_linear_facts(unit, components, metadata)
            if facts is None:
                facts = infer_stub_facts(unit)
        route = _project_unit_route(
            unit, by_key, components, metadata, facts, transform_rows, key in watermarked,
            frozenset(reused_keys or ()), existing_ids, conflict_policy,
        )
        projected.append((key, transform_rows, route))

    # CDX-r1-07: every route serves the SAME fixed row keys and endpoint tokens
    # (``extract``, ``send``, ``db_source``, ...), so two routes in one preview
    # made every key-based join ambiguous. With MORE THAN ONE route, each
    # route's row keys, endpoint keys and every in-row reference to one of them
    # are qualified by the owning process key. A single route keeps the
    # established wire byte-identical (the #157 composition fixtures and the
    # flagship parity pin it).
    #
    # ARCH-r1-06: EVERY projected root counts, with or without facts — a v1 or
    # direct root's ``transform`` row beside a route's ``transform`` row is the
    # same ambiguity. So once a preview holds a route AND more than one root,
    # every root's rows are qualified by its process key. A preview with no
    # route at all (direct intents only) is unchanged: its pre-#159 shape.
    routes = sum(1 for _key, _rows, route in projected if route is not None)
    qualify = routes >= 1 and len(projected) > 1
    flows: List[Any] = []
    endpoints: List[Any] = []
    summaries: Dict[str, Any] = {}
    for key, transform_rows, route in projected:
        if route is None:
            if qualify:
                transform_rows, _none = _qualified_route(key, transform_rows, [])
            flows.extend(transform_rows)
            continue
        rows, unit_endpoints, summary = route
        if qualify:
            rows, unit_endpoints = _qualified_route(key, rows, unit_endpoints)
        flows.extend(rows)
        endpoints.extend(unit_endpoints)
        if summary is not None:
            summaries[key] = summary
    return RecipeProjection(flows, endpoints, summaries)


def _project_unit_route(
    unit, by_key, components, metadata, facts, transform_rows, watermarked,
    reused_keys, existing_ids, conflict_policy=None,
):
    """``(rows, endpoints, summary-or-None)`` for one unit, dispatched on its
    facts' TYPE; None when it has none (or they do not resolve)."""
    from ..models.derived_flows import (
        LinearRouteProjectionFactsV1,
        RecipeUnitProjectionFactsV1,
        StubRouteProjectionFactsV1,
    )

    if isinstance(facts, RecipeUnitProjectionFactsV1):
        return _route_projection(
            unit, by_key, metadata, facts, transform_rows, watermarked,
            reused_keys, existing_ids,
        )
    if isinstance(facts, LinearRouteProjectionFactsV1):
        linear = _linear_route_projection(
            unit, by_key, components, metadata, facts, transform_rows, watermarked,
            reused_keys, existing_ids, conflict_policy,
        )
        return None if linear is None else (linear[0], linear[1], None)
    if isinstance(facts, StubRouteProjectionFactsV1):
        stub = _stub_route_projection(unit, transform_rows)
        return None if stub is None else (stub[0], stub[1], None)
    return None


def qualified_route_key(process_key: str, key: str) -> str:
    """The process-scoped spelling of a route's row or endpoint key."""
    return "{0}/{1}".format(process_key, key)


def _qualified_route(process_key: str, rows: Sequence[Any], endpoints: Sequence[Any]):
    """One route's rows and endpoints with keys AND references scoped to its process.

    A reference is qualified exactly when it names a row or endpoint of THIS
    route, so every qualified reference resolves inside its own process. A
    token naming nothing served (the reliability row's ``dlq``) is left as is.
    """
    local = {row.key for row in rows} | {endpoint.key for endpoint in endpoints}

    def scoped(value: Optional[str]) -> Optional[str]:
        return qualified_route_key(process_key, value) if value in local else value

    qualified_rows = [
        row.model_copy(
            update={
                "key": qualified_route_key(process_key, row.key),
                "source": scoped(row.source),
                "target": scoped(row.target),
            }
        )
        for row in rows
    ]
    qualified_endpoints = [
        endpoint.model_copy(update={"key": qualified_route_key(process_key, endpoint.key)})
        for endpoint in endpoints
    ]
    return qualified_rows, qualified_endpoints


__all__ = [
    "CONNECTOR_OPERATION_BY_DIRECTION_AND_FAMILY",
    "CONNECTOR_ROW_KEY_BY_DIRECTION_AND_FAMILY",
    "DB_WRITE_OPERATION_SPELLINGS",
    "ENDPOINT_TOKEN_BY_DIRECTION_AND_FAMILY",
    "FUTURE_BUILDER_ISSUE_BY_ROUTE",
    "NORMALIZATION_RULES",
    "RECIPE_FLOW_ROW_SPELLINGS",
    "RecipeProjection",
    "SOURCE_TOKEN_BY_FAMILY",
    "STUB_ENDPOINTS",
    "derive_recipe_projection",
    "derive_transform_flows",
    "flow_row_digest",
    "infer_direct_linear_facts",
    "infer_stub_facts",
    "normalize_flow_row",
    "normalize_operation_summary",
    "qualified_route_key",
    "recipe_leg_ordinals",
    "typed_row_from_legacy",
    "write_profile_summary",
]
