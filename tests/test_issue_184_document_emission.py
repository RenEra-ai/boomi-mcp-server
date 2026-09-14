"""#184 amendment 3 §1-§6 and §12: the document-emission authority, pinned in both directions.

``DOCUMENT_EMISSION_V1`` (``boomi_mcp.models.process_ir_document_semantics``) is the ONE
statement of what each step does with documents. It replaced five hand-written rules
that disagreed with the platform (ledger class ``cache-step-document-emission``, rows
E0-184-01, E0-184-03, E1-184-01): Add to Cache and an all-document Remove from Cache
emit zero documents, so the platform skips whatever is wired after them and the run
still reads COMPLETE, and a cache read runs only when a document arrives.

This file is what the authority's docstring and ``invariants._CACHE_STAGE_PATHS`` cite.
It pins six things:

1. **Coverage, both directions.** Every authored kind, compiler semantic kind and
   emitter-registry key has a row, and every row names real kinds. Each case set is
   read from its runtime authority (the model's unions, ``CfgSemanticV1``, the
   registry). The three rows with no semantic kind (``start``, ``continue``,
   ``sequence``) are each justified by the compiler, not by a list.
2. **Capture binding, both directions, with hashes.** Measured rows cite indexed
   measured rules. Measured index rules are cited. Every indexed file hashes to the
   archive. The index rebuilds byte for byte.
3. **The placement matrix.** Every body context (read from
   ``body_capabilities.BODY_SLOT_AUTHORITIES_V1``), every root grammar (read from the
   verdict's own call sites), every cache action (zero-emission and
   triggered-replacement kinds), the step and terminal positions, and every successor
   the slot admits. Both compiler entry points are measured and must agree.
4. **Positives.** Each terminal cache action a terminal union admits compiles, and its
   rendered shape has no outgoing wire.
5. **Trigger.** A read after an exhausted path is refused; a read on a triggered path
   compiles with its one wire.
6. **Mutants.** Four measured mutants (inverted consumption, an omitted alias, a
   restored read-after-sink exception, an outgoing cache wire in the registry) each
   make named checks in this file fail, with the unmutated control passing. The
   derived frozensets are bound by name in their consumers at import. The mutant
   harness therefore re-derives them with the authority module's OWN code, rebinds
   every module-level copy, and re-evaluates the module-level values composed from
   them. It returns the bindings it reached, and asserts every module-level consumer
   is among them.

Every measured rule in the capture index is cited exactly once. An emission rule is
cited by the table's rows. Any other rule is cited by the single ``src`` module that owns
its fact, found by scanning ``src`` for the rule id.
"""

from __future__ import annotations

import ast
import copy
import dataclasses
import functools
import hashlib
import importlib
import importlib.util
import json
import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import get_args

import pytest
from pydantic import TypeAdapter

_ROOT = Path(__file__).resolve().parent.parent
for _p in (str(_ROOT), str(_ROOT / "src")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from boomi_mcp.categories.components import process_graph_verifier as graph_verifier  # noqa: E402
from boomi_mcp.categories.components.builders import process_flow_builder as legacy_builder  # noqa: E402
from boomi_mcp.compiler.process_ir import body_capabilities as bc  # noqa: E402
from boomi_mcp.compiler.process_ir import connector_capabilities as CC  # noqa: E402
from boomi_mcp.compiler.process_ir import contracts as compiler_contracts  # noqa: E402
from boomi_mcp.compiler.process_ir import emitter_registry as registry  # noqa: E402
from boomi_mcp.compiler.process_ir import connector_resolution  # noqa: E402
from boomi_mcp.compiler.process_ir import invariants  # noqa: E402
from boomi_mcp.compiler.process_ir import lowering  # noqa: E402
from boomi_mcp.compiler.process_ir import pipeline  # noqa: E402
from boomi_mcp.compiler.process_ir.semantic_validation import context as validation_context  # noqa: E402
from boomi_mcp.compiler.process_ir.semantic_validation import lineage  # noqa: E402
from boomi_mcp.compiler.process_ir.contracts import (  # noqa: E402
    ComponentSymbolV1,
    EmissionTransitionV1,
    SymbolTableV1,
)
from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError  # noqa: E402
from boomi_mcp.errors import (  # noqa: E402
    PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY,
    PROCESS_IR_COMPILE_EMITTER_INPUT_INVALID,
    PROCESS_IR_SCHEMA_INVALID_CARDINALITY,
    PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW,
    PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH,
    PROCESS_IR_SEMANTIC_PROFILE_MISMATCH,
)
from boomi_mcp.models import process_ir as model  # noqa: E402
from boomi_mcp.models import process_ir_document_semantics as emission  # noqa: E402
from boomi_mcp.models.process_ir import (  # noqa: E402
    ProcessNodeV1,
    parse_process_ir_v1,
    process_ir_v1_node_kinds,
)
from boomi_mcp.models.process_ir_tokens import CAUGHT_ERROR_PROPERTY_ID  # noqa: E402

from _process_ir_entrypoint_differential import measure_entrypoints  # noqa: E402

_NODE = TypeAdapter(ProcessNodeV1)
_SRC = _ROOT / "src"
_ARCHIVE = _ROOT / "docs/architecture/evidence/issue-184"
_INDEX_PATH = _ARCHIVE / "document_emission/CAPTURE_INDEX.json"
_INDEX_SCRIPT = _ARCHIVE / "document_emission/build_capture_index.py"

_CARD = PROCESS_IR_SCHEMA_INVALID_CARDINALITY


# ---------------------------------------------------------------------------
# Atoms, symbols, compile helpers
# ---------------------------------------------------------------------------

_GET = {"kind": "connector_call", "operation_ref": "$ref:GET"}
_PATCH = {"kind": "connector_call", "operation_ref": "$ref:PATCH"}
_MSG = {"kind": "message", "text": "m"}
_STOP = {"kind": "stop"}
_DPP = {"kind": "set_dpp", "name": "Z", "source_values": [{"value_type": "static", "value": "v"}]}


def _atom(kind):
    """A minimal, internally valid node of ``kind``.

    Values are inert placeholders; ``test_every_atom_is_a_valid_node_of_its_kind``
    proves each one validates, so a broken atom fails there instead of silently
    emptying a matrix family.
    """
    if kind == "branch":
        return {"kind": "branch", "legs": [
            {"steps": [dict(_MSG)], "terminal": dict(_STOP)},
            {"steps": [dict(_MSG)], "terminal": dict(_STOP)},
        ]}
    if kind == "decision":
        return {
            "kind": "decision", "comparison": "equals",
            "left": {"value_type": "static", "static_value": "a"},
            "right": {"value_type": "static", "static_value": "a"},
            "true_arm": {"steps": [dict(_MSG)], "terminal": dict(_STOP)},
            "false_arm": {"steps": [], "terminal": dict(_STOP)},
        }
    if kind == "try_catch":
        return {
            "kind": "try_catch", "scope": "process",
            "try_body": {"steps": [dict(_GET)], "terminal": dict(_STOP)},
            "catch_body": {"steps": [dict(_MSG)], "terminal": dict(_STOP)},
        }
    if kind == "data_process":
        return {"kind": "data_process", "steps": [
            {"operation": "custom_scripting", "language": "groovy2", "script": "x"}]}
    fields = {
        "cache_get": {"cache_ref": "$ref:CACHE"},
        "cache_put": {"cache_ref": "$ref:CACHE"},
        "cache_remove": {"cache_ref": "$ref:CACHE"},
        "document_cache_retrieve": {"cache_ref": "$ref:CACHE"},
        "connector_call": {"operation_ref": "$ref:GET"},
        "exception": {"message_template": "boom {1}"},
        "notify": {"level": "ERROR", "message_template": "boom {0}".format(CAUGHT_ERROR_PROPERTY_ID)},
        "flow_control": {"for_each_count": 1},
        "map_ref": {"map_ref": "$ref:MAP"},
        "message": {"text": "m"},
        "process_call": {"process_ref": "$ref:CHILD"},
        "return_documents": {},
        "continue": {},
        "stop": {},
        "set_ddp": {"name": "n", "source_values": [{"value_type": "static", "value": "v"}]},
        "set_dpp": {"name": "n", "source_values": [{"value_type": "static", "value": "v"}]},
        "source": {"connection_ref": "$ref:CONN", "operation_ref": "$ref:GET"},
        "target": {"connection_ref": "$ref:CONN", "operation_ref": "$ref:PATCH"},
        "listener": {"operation_ref": "$ref:LISTEN"},
        "passthrough": {},
    }
    return {"kind": kind, **copy.deepcopy(fields[kind])}


def _symbols():
    rest = CC.REST_FAMILY

    def sym(ref, cid, ctype, **kw):
        return ComponentSymbolV1(ref="$ref:" + ref, component_id=cid, component_type=ctype, **kw)

    return SymbolTableV1(symbols=(
        sym("CONN", "CONN", "connector-settings", connector_type=rest),
        sym("GET", "GETOP", "connector-action", connector_type=rest, action_type="GET",
            connection_ref="$ref:CONN", output_profile_ref="$ref:P1"),
        sym("PATCH", "PATCHOP", "connector-action", connector_type=rest, action_type="PATCH",
            connection_ref="$ref:CONN", input_profile_ref="$ref:P1"),
        sym("P1", "P1", "profile.json"),
        sym("CACHE", "CACHE", "documentcache"),
        sym("CHILD", "CHILD", "process"),
    ))


def _doc(*steps):
    return {"version": "1", "body": {"kind": "sequence", "steps": [copy.deepcopy(s) for s in steps]}}


def _compile_and_emit(payload):
    """Full public compile (re-parse, lowering, semantic gate, plan) and emission."""
    ir = parse_process_ir_v1(copy.deepcopy(payload))
    cfg, plan = pipeline.compile_process_ir_v1(ir, _symbols())
    return cfg, plan, registry.emit_process(plan, _symbols()).process_xml


def _pointer(payload, path):
    node = payload
    for part in path.strip("/").split("/"):
        node = node[int(part)] if isinstance(node, list) else node[part]
    return node


def _outbound(process_xml):
    """``{shape name: (shape type, [toShape, ...])}``: the wires the graph verifier reads."""
    shapes = {}
    for shape in ET.fromstring(process_xml).iter("shape"):
        dragpoints = shape.find("dragpoints")
        wires = [] if dragpoints is None else [
            dp.get("toShape") for dp in dragpoints.findall("dragpoint") if dp.get("toShape")]
        shapes[shape.get("name")] = (shape.get("shapetype"), wires, dragpoints is not None)
    return shapes


def _verdict(ir):
    """Both public entry points on one model; they must serve the same identity."""
    parser, compiler = measure_entrypoints(ir)
    assert parser == compiler, {"parser": parser, "compiler": compiler}
    return parser


def _first(verdict):
    return None if verdict == ("ACCEPTED",) else tuple(verdict[1][:2])


def _emission_refusals(verdict):
    return [item[:2] for item in verdict[1:] if item[0] == _CARD and item[1].endswith("/cache_ref")]


# ---------------------------------------------------------------------------
# 1. Coverage, both directions
# ---------------------------------------------------------------------------

_ROWS = emission.DOCUMENT_EMISSION_V1
_CACHE_KINDS = frozenset({"cache_put", "cache_remove", "cache_get", "document_cache_retrieve"})


def _semantic_kinds():
    members = get_args(get_args(compiler_contracts.CfgSemanticV1)[0])
    return frozenset(
        literal for member in members for literal in get_args(member.model_fields["semantic_kind"].annotation)
    )


def test_every_atom_is_a_valid_node_of_its_kind():
    """The atoms cover the parser's closed vocabulary exactly, and each validates alone."""
    kinds = set(process_ir_v1_node_kinds()) - {"sequence"}
    assert _CACHE_KINDS <= kinds, "the node vocabulary lost a cache kind — every matrix below would be vacuous"
    for kind in sorted(kinds):
        assert _NODE.validate_python(_atom(kind)).kind == kind


def _check_every_authored_kind_has_a_row():
    kinds = frozenset(process_ir_v1_node_kinds())
    assert _CACHE_KINDS | {"sequence"} <= kinds, sorted(kinds)
    body_kinds = frozenset().union(*bc.BODY_CAPABILITIES_V1.values())
    assert body_kinds <= kinds, sorted(body_kinds - kinds)
    missing = kinds - set(emission.DOCUMENT_EMISSION_V1)
    assert not missing, {"authored_kinds_without_a_row": sorted(missing)}


def test_every_authored_kind_has_a_row():
    """Every kind the parser accepts (``process_ir_v1_node_kinds``, derived from
    ``ProcessNodeV1`` plus the ``sequence`` body) and every kind a body slot admits has
    a row. A kind without a row would be emitted with no statement of its documents."""
    _check_every_authored_kind_has_a_row()


def test_every_row_names_an_authored_kind_or_the_synthesized_start():
    """A row whose kind is not authored must be a step the compiler only synthesizes.

    Justified by the compiler, not a list: the row has no semantic kind, and every plan
    node rendered with one of its emitter keys is ``origin="synthetic"``.
    """
    kinds = frozenset(process_ir_v1_node_kinds())
    unauthored = {kind: row for kind, row in _ROWS.items() if kind not in kinds}
    assert unauthored, "no synthesized row — the scheduled start lost its row"
    _cfg, plan, _xml = _compile_and_emit(_doc(_GET, _STOP))
    for kind, row in unauthored.items():
        assert row.semantic_kind is None, kind
        rendered = [node for node in plan.nodes if node.emitter_input.emitter_kind in row.emitter_kinds]
        assert rendered, (kind, "the plan rendered no node with the row's emitter keys")
        assert {node.origin for node in rendered} == {"synthetic"}, (kind, rendered)


def _check_every_semantic_kind_has_a_row():
    semantic = _semantic_kinds()
    assert _CACHE_KINDS <= semantic, sorted(semantic)
    named = {row.semantic_kind for row in emission.DOCUMENT_EMISSION_V1.values() if row.semantic_kind is not None}
    assert semantic == named, {
        "semantic_kinds_without_a_row": sorted(semantic - named),
        "rows_naming_no_semantic_kind": sorted(named - semantic),
    }


def test_every_semantic_kind_has_a_row_and_every_row_names_a_real_one():
    """``CfgSemanticV1``'s discriminators are exactly the rows' semantic kinds."""
    _check_every_semantic_kind_has_a_row()


def test_rows_without_a_semantic_kind_are_justified_by_the_compiler():
    """``start``, ``continue`` and ``sequence`` lower to no semantic node; each is proven.

    * a synthesized step: not authored (the test above);
    * a container: the ``ProcessIRV1.body`` kind, which is no member of ``ProcessNodeV1``;
    * an edge: authored, yet compiling it yields no CFG node and no plan node at its pointer.
    """
    none_rows = {kind for kind, row in _ROWS.items() if row.semantic_kind is None}
    assert none_rows, "no row lacks a semantic kind — the justifications below are vacuous"
    authored = frozenset(process_ir_v1_node_kinds())
    root_union = frozenset(model._NODE_KIND_TAGS)
    body_kind = get_args(model.ProcessIRV1.model_fields["body"].annotation.model_fields["kind"].annotation)

    chain = _doc(_GET,
                 {"kind": "try_catch", "scope": "connector",
                  "try_body": {"steps": [_PATCH], "terminal": {"kind": "continue"}},
                  "catch_body": {"steps": [_MSG], "terminal": _STOP}},
                 {"kind": "try_catch", "scope": "connector",
                  "try_body": {"steps": [_PATCH], "terminal": _STOP},
                  "catch_body": {"steps": [_MSG], "terminal": _STOP}})
    cfg, plan, _xml = _compile_and_emit(chain)
    edge_pointers = [path for path in ("/body/steps/1/try_body/terminal",)
                     if _pointer(chain, path)["kind"] in none_rows]
    assert edge_pointers, "the witness carries no edge-only kind"

    justified = {}
    for kind in none_rows:
        row = _ROWS[kind]
        if kind not in authored:
            justified[kind] = "synthesized"
        elif kind in body_kind:
            assert kind not in root_union and row.trigger == emission.TRIGGER_CONTAINER, kind
            justified[kind] = "container"
        else:
            pointers = [p for p in edge_pointers if _pointer(chain, p)["kind"] == kind]
            assert pointers, (kind, "no compiled witness for this edge-only kind")
            for p in pointers:
                assert p not in {node.source_path for node in cfg.nodes}, (kind, p)
                assert p not in {node.source_path for node in plan.nodes}, (kind, p)
            assert not row.emitter_kinds, kind
            justified[kind] = "edge"
    assert set(justified) == none_rows


def _check_emitter_keys_and_aliases():
    rows = emission.DOCUMENT_EMISSION_V1
    named = {key for row in rows.values() for key in row.emitter_kinds}
    keys = registry.registry_keys()
    assert keys == named, {"keys_without_a_row": sorted(keys - named), "rows_naming_no_key": sorted(named - keys)}
    by_key = {}
    for kind, row in rows.items():
        for key in row.emitter_kinds:
            by_key.setdefault(key, []).append(kind)
    aliases = {key: kinds for key, kinds in by_key.items() if len(kinds) > 1}
    assert any(set(kinds) >= {"cache_get", "document_cache_retrieve"} for kinds in aliases.values()), aliases
    for key, kinds in aliases.items():
        facts = {(rows[k].trigger, rows[k].result, rows[k].continuation, rows[k].evidence, rows[k].capture_rules)
                 for k in kinds}
        assert len(facts) == 1, (key, kinds, facts)
    for key in emission.ZERO_EMISSION_EMITTER_KINDS:
        # The graph verifier reads these keys as SHAPE TYPES.
        assert registry.registration_for(key).produced_shape_type == key, key


def test_every_emitter_key_has_a_row_and_aliases_state_one_physical_step():
    """Registry keys are exactly the rows' emitter keys. Rows sharing a key (both read
    aliases on ``doccacheretrieve``; the connector and set-property spellings) state the
    same trigger, result, continuation and evidence. A zero-emission key is also the
    shape type the verifier matches."""
    _check_emitter_keys_and_aliases()


def test_derived_sets_are_the_rows_that_state_them():
    """The frozensets consumers import are the rows' own fields, never a second list."""
    rows = _ROWS
    assert emission.ZERO_EMISSION_KINDS == {k for k, r in rows.items() if r.result == emission.RESULT_CONSUMES}
    assert emission.TRIGGERED_REPLACEMENT_KINDS == {k for k, r in rows.items() if r.result == emission.RESULT_REPLACES}
    assert emission.ZERO_EMISSION_KINDS and emission.TRIGGERED_REPLACEMENT_KINDS
    for kind in emission.ZERO_EMISSION_KINDS:
        assert rows[kind].continuation == emission.CONTINUATION_NONE, kind
        assert emission.emits_zero_documents(kind), kind
    for kind in emission.TRIGGERED_REPLACEMENT_KINDS:
        assert rows[kind].trigger == emission.TRIGGER_ARRIVING_DOCUMENT, kind
        assert rows[kind].continuation == emission.CONTINUATION_ONE, kind
    assert set(emission.STEP_DISPLAY_NAMES) == emission.ZERO_EMISSION_KINDS | emission.TRIGGERED_REPLACEMENT_KINDS


def _compiled_witnesses():
    return [_terminal_positive_payload(ctx, kind) for ctx, kind in _TERMINAL_POSITIVES] + [
        _triggered_read_payload(read) for read in sorted(emission.TRIGGERED_REPLACEMENT_KINDS)]


def test_compiled_nodes_lower_to_their_rows_semantic_and_emitter_kinds():
    """What the compiler DOES matches the row: each authored node's CFG semantic kind is
    the row's, and its plan node renders with one of the row's emitter keys."""
    covered = set()
    for payload in _compiled_witnesses():
        cfg, plan, _xml = _compile_and_emit(payload)
        for node in cfg.nodes:
            authored = _pointer(payload, node.source_path)
            if isinstance(authored, dict) and "kind" in authored:
                assert node.semantic.semantic_kind == _ROWS[authored["kind"]].semantic_kind, (node, authored)
        for node in plan.nodes:
            if node.origin != "ir":
                continue
            kind = _pointer(payload, node.source_path)["kind"]
            assert node.emitter_input.emitter_kind in _ROWS[kind].emitter_kinds, (kind, node.emitter_input.emitter_kind)
            covered.add(kind)
    missing = (emission.ZERO_EMISSION_KINDS | emission.TRIGGERED_REPLACEMENT_KINDS) - covered
    assert not missing, sorted(missing)


def _module_level_authority_imports(path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return [
        (alias.name, alias.asname or alias.name)
        for stmt in tree.body
        if isinstance(stmt, ast.ImportFrom) and (stmt.module or "").endswith("process_ir_document_semantics")
        for alias in stmt.names
    ]


@functools.lru_cache(maxsize=1)
def _authority_consumer_modules():
    """Every ``src`` module that names the authority, imported, with its module-level imports."""
    found = {}
    for path in sorted(_SRC.rglob("*.py")):
        if path.name == "process_ir_document_semantics.py":
            continue
        if "process_ir_document_semantics" not in path.read_text(encoding="utf-8"):
            continue
        name = ".".join(path.relative_to(_SRC).with_suffix("").parts)
        found[name] = (importlib.import_module(name), _module_level_authority_imports(path))
    return found


def _check_consumers_read_the_authority():
    consumers = _authority_consumer_modules()
    bound = {name for name, (_mod, imports) in consumers.items() if imports}
    assert {model.__name__, registry.__name__, lowering.__name__, connector_resolution.__name__,
            lineage.__name__, graph_verifier.__name__, legacy_builder.__name__} <= bound, sorted(bound)
    for name, (module, imports) in consumers.items():
        for source_name, local_name in imports:
            assert getattr(module, local_name) is getattr(emission, source_name), (name, local_name)
    rows = emission.DOCUMENT_EMISSION_V1
    none_keys = {k for r in rows.values() if r.continuation == emission.CONTINUATION_NONE for k in r.emitter_kinds}
    assert emission.ZERO_EMISSION_EMITTER_KINDS <= graph_verifier._ALWAYS_TERMINAL_SHAPE_TYPES
    assert graph_verifier._ALWAYS_TERMINAL_SHAPE_TYPES <= none_keys
    assert graph_verifier._TERMINAL_SHAPE_TYPES <= none_keys
    assert legacy_builder._LEGACY_ZERO_EMISSION_KINDS == emission.ZERO_EMISSION_KINDS | emission.ZERO_EMISSION_EMITTER_KINDS


def test_consumers_bind_the_authoritys_own_objects():
    """Every module-level import of the authority is the authority's object, found by
    scanning ``src`` rather than listed. The values composed from it at import (the
    verifier's terminal shape sets and the legacy builder's kind set) agree with the rows."""
    _check_consumers_read_the_authority()


def _check_registry_cardinality_is_the_rows_continuation():
    rows = emission.DOCUMENT_EMISSION_V1
    checked = set()
    for kind, row in rows.items():
        for key in row.emitter_kinds:
            outgoing = registry.registration_for(key).outgoing
            if row.continuation == emission.CONTINUATION_NONE:
                assert outgoing == registry.EXACT_ZERO, (kind, key, outgoing)
            elif row.continuation == emission.CONTINUATION_ONE:
                assert outgoing == registry.EXACT_ONE, (kind, key, outgoing)
            else:
                assert outgoing.kind == "branch" or (outgoing.kind == "exact" and outgoing.value >= 2), (kind, key)
            checked.add(key)
    assert emission.ZERO_EMISSION_EMITTER_KINDS <= checked


def test_registry_cardinality_is_the_rows_continuation():
    """The registry's outgoing-edge count is the row's continuation: none is exactly zero,
    one is exactly one, control is one wire per body."""
    _check_registry_cardinality_is_the_rows_continuation()


# ---------------------------------------------------------------------------
# 2. Capture binding, both directions, with hashes
# ---------------------------------------------------------------------------


def _index():
    return json.loads(_INDEX_PATH.read_text(encoding="utf-8"))


def _index_rules():
    return {entry["rule"]: entry for entry in _index()["rules"]}


#: What a rule's CLAIM (the id after its step prefix) decides about the row citing it.
#: No runtime authority enumerates claim meanings, so this is a pinned closed contract
#: with a bidirectional pin: every claim a measured row cites has an entry, and every
#: entry is cited (``test_each_measured_row_states_what_its_evidence_decides``).
_CLAIM_DECIDES = {
    "emits_zero_documents": (("result", emission.RESULT_CONSUMES),),
    "terminal_executes": (("continuation", emission.CONTINUATION_NONE),),
    "requires_arriving_document": (("trigger", emission.TRIGGER_ARRIVING_DOCUMENT),),
    "empty_cache_emits_no_documents": (("result", emission.RESULT_REPLACES),),
    "replaces_payload_with_cached_documents": (("result", emission.RESULT_REPLACES),),
    # A No Data start hands on exactly one empty document when the process starts
    # (cap184-passthrough-standalone, cap184-nodata-per-document).
    "scheduled_supplies_one_empty_document": (
        ("trigger", emission.TRIGGER_PROCESS_START), ("result", emission.RESULT_SUPPLIES)),
}


@functools.lru_cache(maxsize=1)
def _src_texts():
    return {
        ".".join(path.relative_to(_SRC).with_suffix("").parts): path.read_text(encoding="utf-8")
        for path in sorted(_SRC.rglob("*.py"))
    }


def _modules_citing(rule):
    """Every ``src`` module whose text names ``rule`` as a whole id (not as a prefix)."""
    pattern = re.compile(r"(?<![\w.]){0}(?![\w.])".format(re.escape(rule)))
    return sorted(name for name, text in _src_texts().items() if pattern.search(text))


def _cited_rules():
    return sorted({(kind, rule) for kind, row in _ROWS.items() if row.evidence == emission.MEASURED
                   for rule in row.capture_rules})


def test_measured_rows_cite_rules_and_other_rows_cite_none():
    measured = [kind for kind, row in _ROWS.items() if row.evidence == emission.MEASURED]
    assert _CACHE_KINDS <= set(measured), "a cache row is no longer measured"
    for kind, row in _ROWS.items():
        if row.evidence == emission.MEASURED:
            assert row.capture_rules, kind
        else:
            assert not row.capture_rules, (kind, row.evidence, row.capture_rules)


@pytest.mark.parametrize("kind,rule", [
    pytest.param(kind, rule, id="{0}-{1}".format(kind, rule)) for kind, rule in _cited_rules()
])
def test_every_cited_rule_is_indexed_as_measured(kind, rule):
    entry = _index_rules().get(rule)
    assert entry is not None, (kind, rule, "not in CAPTURE_INDEX.json")
    assert entry["status"] == "measured", (kind, rule, entry["status"])
    assert any(r["verdict"] != "OPEN" for r in entry["rows"]) or entry["controls"], rule


@pytest.mark.parametrize("rule", sorted(_index_rules()))
def test_every_index_rule_is_cited_or_recorded_open(rule):
    """Each measured index rule has exactly one citing authority.

    An emission rule is cited by the table's rows. Its id then appears in no ``src``
    module but the authority's own. Any other measured rule decides a fact outside the
    table, and is cited by exactly one other ``src`` module, found by scanning ``src``
    for the id. An OPEN rule may decide no row.
    """
    entry = _index_rules()[rule]
    citing_rows = [kind for kind, row in _ROWS.items() if rule in row.capture_rules]
    modules = _modules_citing(rule)
    if entry["status"] == "open":
        assert not citing_rows, (rule, "an OPEN rule cannot decide a row", citing_rows)
        assert all(r["verdict"] == "OPEN" for r in entry["rows"]), rule
        return
    assert entry["status"] == "measured", (rule, entry["status"])
    if citing_rows:
        assert modules == [emission.__name__], (rule, "an emission rule is cited by rows only", modules)
    else:
        assert len(modules) == 1 and modules != [emission.__name__], (rule, "cited by no single owner", modules)


def test_the_citation_scan_finds_nothing_for_an_unknown_rule():
    """Negative control for the scan above: an unknown id, and a strict prefix of a real
    id, match no module. A known emission rule matches only the authority."""
    assert _modules_citing("cache_load.no_such_rule_measured_by_184") == []
    assert _modules_citing("cache_retrieve.ddp_overlay_one_current") == []
    assert _modules_citing("cache_load.emits_zero_documents") == [emission.__name__]


def _check_measured_rows_state_their_evidence():
    rows = emission.DOCUMENT_EMISSION_V1
    cited_claims = set()
    for kind, row in rows.items():
        if row.evidence != emission.MEASURED:
            continue
        for rule in row.capture_rules:
            claim = rule.split(".", 1)[1]
            cited_claims.add(claim)
            assert claim in _CLAIM_DECIDES, (kind, rule, "a claim with no recorded meaning")
            for field, value in _CLAIM_DECIDES[claim]:
                assert getattr(row, field) == value, (kind, rule, field, getattr(row, field), value)
    assert cited_claims == set(_CLAIM_DECIDES), {"stale_claims": sorted(set(_CLAIM_DECIDES) - cited_claims)}
    by_rule = {}
    for kind, row in rows.items():
        for rule in row.capture_rules:
            by_rule.setdefault(rule, set()).add(kind)
    for rule, kinds in by_rule.items():
        shared = set.intersection(*(set(rows[k].emitter_kinds) for k in kinds))
        assert shared, (rule, "cited by rows of different physical steps", sorted(kinds))


def test_each_measured_row_states_what_its_evidence_decides():
    """A measured row may not contradict the claim of the rule it cites: a row citing
    ``*.emits_zero_documents`` consumes, a row citing ``*.requires_arriving_document`` is
    triggered by an arriving document. A rule cited by several rows is cited only by
    aliases of one physical step (rows sharing an emitter key)."""
    _check_measured_rows_state_their_evidence()


def test_every_indexed_file_hashes_to_the_archive():
    """Each file the index binds matches the archive's SHA256SUMS and the bytes on disk."""
    index = _index()
    sums = {}
    for line in (_ROOT / index["sums"]).read_text(encoding="utf-8").splitlines():
        digest, path = line.split("  ", 1)
        sums[path] = digest
    files = [f for entry in index["rules"] for row in entry["rows"] for f in row["files"]]
    files += [f for entry in index["rules"] for f in entry["controls"]]
    assert len(files) >= len(index["rules"]), "the index binds almost no files — the check would be vacuous"
    for item in files:
        assert sums.get(item["file"]) == item["sha256"], item["file"]
        actual = hashlib.sha256((_ARCHIVE / item["file"]).read_bytes()).hexdigest()
        assert actual == item["sha256"], item["file"]


def test_the_capture_index_rebuilds_byte_for_byte(tmp_path, capsys):
    """The committed index is the script's output over the archived MANIFESTs and sums.
    The rebuild writes to a temp path, never over the committed file."""
    spec = importlib.util.spec_from_file_location("_issue184_build_capture_index", _INDEX_SCRIPT)
    script = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(script)
    assert script.OUT == _INDEX_PATH
    script.OUT = tmp_path / "CAPTURE_INDEX.json"
    assert script.main() == 0
    capsys.readouterr()
    assert script.OUT.read_bytes() == _INDEX_PATH.read_bytes()


# ---------------------------------------------------------------------------
# 3. The placement matrix
# ---------------------------------------------------------------------------

_BODY_CONTEXTS = tuple(sorted({context for context, _slot in bc.BODY_SLOT_AUTHORITIES_V1}))
_CACHE_ACTIONS = tuple(sorted(emission.ZERO_EMISSION_KINDS | emission.TRIGGERED_REPLACEMENT_KINDS))


def _body_carrier(context):
    """A legal document holding the target body. Exact routing: an unknown context fails."""
    if context == bc.BRANCH_LEG:
        control = _atom("branch")
    elif context in (bc.DECISION_TRUE_ARM, bc.DECISION_FALSE_ARM):
        control = _atom("decision")
    elif context in (bc.TRY_BODY, bc.CATCH_BODY):
        control = _atom("try_catch")
    else:
        raise AssertionError("unrecognised body context: {0}".format(context))
    return _doc(control)


def _body_of(ir, context):
    node = ir.body.steps[0]
    return {
        bc.BRANCH_LEG: lambda: node.legs[0],
        bc.DECISION_TRUE_ARM: lambda: node.true_arm,
        bc.DECISION_FALSE_ARM: lambda: node.false_arm,
        bc.TRY_BODY: lambda: node.try_body,
        bc.CATCH_BODY: lambda: node.catch_body,
    }[context]()


def _anchor(context):
    """A process-scoped try body must BEGIN with its producing call."""
    return [dict(_GET)] if context == bc.TRY_BODY else []


def _body_model(context, steps, terminal):
    ir = parse_process_ir_v1(_body_carrier(context))
    body = _body_of(ir, context)
    body.steps = [_NODE.validate_python(copy.deepcopy(s)) for s in _anchor(context) + list(steps)]
    body.terminal = _NODE.validate_python(copy.deepcopy(terminal))
    return ir


@functools.lru_cache(maxsize=None)
def _body_pointer(context):
    """The body's JSON pointer, read from the parser: a read is no terminal anywhere, so
    placing one in the terminal slot is refused AT that slot."""
    never_terminal = sorted(emission.TRIGGERED_REPLACEMENT_KINDS - frozenset().union(
        *(kinds for (_c, slot), kinds in bc.BODY_CAPABILITIES_V1.items() if slot == bc.TERMINAL_SLOT)))
    assert never_terminal, "every read became a terminal somewhere"
    verdict = _verdict(_body_model(context, [_MSG], _atom(never_terminal[0])))
    code, path = _first(verdict)
    assert code == PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY and path.endswith("/terminal"), verdict
    return path[: -len("/terminal")]


def _body_cells(context, action):
    """Every (position, successor slot, successor kind) the context's slots admit."""
    step_kinds = bc.BODY_CAPABILITIES_V1[(context, bc.STEP_SLOT)]
    terminal_kinds = bc.BODY_CAPABILITIES_V1[(context, bc.TERMINAL_SLOT)]
    assert "stop" in terminal_kinds, context
    cells = [("terminal", None, None)]
    cells += [("step", bc.STEP_SLOT, kind) for kind in sorted(step_kinds)]
    cells += [("step", bc.TERMINAL_SLOT, kind) for kind in sorted(terminal_kinds)]
    return cells


def _expected_first_emission_refusal(path_kinds, pointers):
    """The authority's verdict: the first zero-emission node with an authored successor."""
    for index, kind in enumerate(path_kinds[:-1]):
        if kind in emission.ZERO_EMISSION_KINDS:
            return (_CARD, pointers[index] + "/cache_ref")
    return None


def _check_body_cell(context, action, position, slot, successor):
    pointer = _body_pointer(context)
    offset = len(_anchor(context))
    if position == "terminal":
        ir = _body_model(context, [_MSG], _atom(action))
        verdict = _verdict(ir)
        if action in bc.BODY_CAPABILITIES_V1[(context, bc.TERMINAL_SLOT)]:
            assert verdict == ("ACCEPTED",), (context, action, verdict)
        else:
            assert _first(verdict) == (PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY, pointer + "/terminal"), (
                context, action, verdict)
        return
    if slot == bc.STEP_SLOT:
        steps, terminal = [_atom(action), _atom(successor)], _atom("stop")
    else:
        steps, terminal = [_atom(action)], _atom(successor)
    kinds = [s["kind"] for s in _anchor(context)] + [s["kind"] for s in steps] + [terminal["kind"]]
    pointers = ["{0}/steps/{1}".format(pointer, i) for i in range(offset + len(steps))] + [pointer + "/terminal"]
    verdict = _verdict(_body_model(context, steps, terminal))
    expected = _expected_first_emission_refusal(kinds, pointers)
    if expected is None:
        assert not _emission_refusals(verdict), (context, action, slot, successor, verdict)
    else:
        assert _first(verdict) == expected, (context, action, slot, successor, verdict)


def _check_body_matrix(context, action):
    for cell in _body_cells(context, action):
        _check_body_cell(context, action, *cell)


@pytest.mark.parametrize("action", _CACHE_ACTIONS)
@pytest.mark.parametrize("context", _BODY_CONTEXTS)
def test_body_placement_matrix(context, action):
    """Every body context x cache action x position x admitted successor, at both entry points.

    A zero-emission step with ANY authored successor is refused
    ``PROCESS_IR_SCHEMA_INVALID_CARDINALITY`` at its ``/cache_ref``, the first diagnostic.
    A read may continue: no successor draws an emission refusal at it. A cache action in
    a terminal slot is admitted exactly when that slot's union admits it, and otherwise
    refused at the slot. The successors are every kind the context's step and terminal
    slots admit (``BODY_CAPABILITIES_V1``); a successor a slot does not admit is not a
    cell, because the slot rule, not emission, refuses it.
    """
    _check_body_matrix(context, action)


def test_the_collected_matrix_parameters_keep_every_cache_kind():
    """Floor on the COLLECTED parameter sets, which are read from the authority at import.

    Measured with real source edits: inverting ``cache_put``'s consumption, or dropping the
    ``document_cache_retrieve`` row, shrinks ``_CACHE_ACTIONS`` and ``_TERMINAL_POSITIVES``.
    The matrix families for that kind then vanish rather than fail. This floor makes the
    shrinkage itself a failure.
    """
    assert _CACHE_KINDS <= set(_CACHE_ACTIONS), sorted(_CACHE_KINDS - set(_CACHE_ACTIONS))
    assert {kind for _context, kind in _TERMINAL_POSITIVES} >= _CACHE_KINDS - emission.TRIGGERED_REPLACEMENT_KINDS
    assert set(emission.TRIGGERED_REPLACEMENT_KINDS) >= _CACHE_KINDS - emission.ZERO_EMISSION_KINDS


def test_the_body_matrix_covers_every_requested_successor_category():
    """Non-vacuity over the cells generated above: each requested successor category occurs."""
    seen = {"none": set(), "terminal": set(), "step": set()}
    for context in _BODY_CONTEXTS:
        for action in sorted(emission.ZERO_EMISSION_KINDS):
            for position, slot, successor in _body_cells(context, action):
                if position == "terminal":
                    seen["none"].add((context, action))
                else:
                    seen["terminal" if slot == bc.TERMINAL_SLOT else "step"].add(successor)
    assert {"stop", "exception", "process_call"} <= seen["terminal"], seen["terminal"]
    assert any(_ROWS[k].result == emission.RESULT_FORWARDS for k in seen["step"]), seen["step"]
    assert emission.TRIGGERED_REPLACEMENT_KINDS <= seen["step"]
    assert emission.ZERO_EMISSION_KINDS <= seen["step"] | seen["terminal"]
    assert len(seen["none"]) == len(_BODY_CONTEXTS) * len(emission.ZERO_EMISSION_KINDS)


#: One carrier per root grammar: ``head + [cache action] + tail``. Which grammars exist is
#: not taken from this table: ``test_every_root_grammar_runs_the_one_verdict`` proves the
#: forms reach exactly the verdict's call sites with ``followed=False``, one site each.
_HANDLER_CONTINUE = {"kind": "try_catch", "scope": "connector",
                     "try_body": {"steps": [_GET], "terminal": {"kind": "continue"}},
                     "catch_body": {"steps": [_MSG], "terminal": _STOP}}
_HANDLER_STOP = {"kind": "try_catch", "scope": "connector",
                 "try_body": {"steps": [_GET], "terminal": _STOP},
                 "catch_body": {"steps": [_MSG], "terminal": _STOP}}
_ROOT_FORMS = {
    "serialized_region_chain": ([_GET], [_GET, _HANDLER_CONTINUE, _HANDLER_STOP]),
    "listener_connector_call": ([_atom("listener"), _GET], [_GET, _STOP]),
    "listener_endpoint": ([_atom("listener")], [_atom("target"), _STOP]),
    "passthrough": ([_atom("passthrough")], [_STOP]),
    "connector_call_sequence": ([_GET], [_GET, _STOP]),
    "call_free_read_led": ([{"kind": "cache_get", "cache_ref": "$ref:CACHE", "external_writer": True}], [_STOP]),
    "legacy_source_target": ([_atom("source")], [_atom("target"), _STOP]),
}


def _root_steps(form, action_node, mode, successor=None):
    head, tail = _ROOT_FORMS[form]
    if mode == "carrier":
        return head + [action_node] + tail
    if mode == "last":
        return head + [action_node]
    if mode == "step":
        return head + [action_node, _atom(successor)] + tail
    if mode == "terminal":
        return head + [action_node, _atom(successor)]
    raise AssertionError(mode)


def _root_model(steps):
    head, tail = _ROOT_FORMS["connector_call_sequence"]
    ir = parse_process_ir_v1(_doc(*(head + [_MSG] + tail)))
    ir.body.steps = [_NODE.validate_python(copy.deepcopy(s)) for s in steps]
    return ir


@functools.lru_cache(maxsize=None)
def _root_control_admits(form, mode, successor):
    """The root cell exists iff the same document with a FORWARDING step in place of the
    cache action is legal: the grammar, not emission, decides the rest."""
    try:
        parse_process_ir_v1(_doc(*_root_steps(form, _MSG, mode, successor)))
    except model.ProcessIRValidationError:
        return False
    return True


def _check_root_matrix(form, action):
    successors = sorted(set(process_ir_v1_node_kinds()) - {"sequence"})
    assert _root_control_admits(form, "carrier", None), (form, "the carrier itself is illegal")
    cells = [("carrier", None)] + [(mode, s) for mode in ("step", "terminal") for s in successors
                                    if _root_control_admits(form, mode, s)]
    head, _tail = _ROOT_FORMS[form]
    for mode, successor in cells:
        steps = _root_steps(form, _atom(action), mode, successor)
        verdict = _verdict(_root_model(steps))
        expected = _expected_first_emission_refusal(
            [s["kind"] for s in steps], ["/body/steps/{0}".format(i) for i in range(len(steps))])
        if expected is None:
            assert not _emission_refusals(verdict), (form, action, mode, successor, verdict)
        else:
            assert _first(verdict) == expected, (form, action, mode, successor, verdict)
    # A cache action as the root's last element: no root grammar ends on one.
    last = _verdict(_root_model(_root_steps(form, _atom(action), "last")))
    assert last[0] == "REFUSED", (form, action, last)
    return cells


@pytest.mark.parametrize("action", _CACHE_ACTIONS)
@pytest.mark.parametrize("form", sorted(_ROOT_FORMS))
def test_root_placement_matrix(form, action):
    """Every root grammar x cache action x successor the grammar admits, at both entry points.

    The successor space is every authored kind, as a step before the carrier's tail or as
    the new terminal; a cell exists where the grammar admits a forwarding step there.
    The root's last element is its terminal, and no root grammar admits a cache action
    there.
    """
    cells = _check_root_matrix(form, action)
    assert len(cells) > 1, (form, "only the carrier cell exists — the successor sweep is vacuous")


def test_every_root_grammar_runs_the_one_verdict(monkeypatch):
    """The root forms are bound to the verdict's call sites, in both directions.

    Read from ``models/process_ir.py``'s AST: every ``_check_terminal_cache_actions`` call
    with ``followed=False`` is a root grammar. A spy records which site each carrier
    reaches; the carriers reach every site, one site each.
    """
    tree = ast.parse(Path(model.__file__).read_text(encoding="utf-8"))
    sites = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "_check_terminal_cache_actions":
            followed = {kw.arg: ast.literal_eval(kw.value) for kw in node.keywords}["followed"]
            sites.setdefault(followed, set()).add(node.lineno)
    assert sites.get(False) and sites.get(True), sites
    real = model._check_terminal_cache_actions
    hits = []

    def spy(steps, *, followed):
        hits.append((sys._getframe(1).f_lineno, followed))
        return real(steps, followed=followed)

    monkeypatch.setattr(model, "_check_terminal_cache_actions", spy)
    reached = {}
    for form in sorted(_ROOT_FORMS):
        hits.clear()
        steps = _root_steps(form, _atom("cache_put"), "carrier")
        with pytest.raises(model.ProcessIRValidationError):
            parse_process_ir_v1(_doc(*steps))
        root_sites = {line for line, followed in hits if not followed}
        assert len(root_sites) == 1, (form, root_sites)
        reached[form] = root_sites.pop()
    assert set(reached.values()) == sites[False], {
        "unreached_sites": sorted(sites[False] - set(reached.values()))}
    assert len(set(reached.values())) == len(_ROOT_FORMS)


def _check_cache_stage_paths_match_terminal_unions():
    stage = invariants._CACHE_STAGE_PATHS
    rows = emission.DOCUMENT_EMISSION_V1
    assert set(stage) == set(emission.ZERO_EMISSION_SEMANTIC_KINDS), {
        "stage_paths": sorted(stage), "zero_emission_semantic_kinds": sorted(emission.ZERO_EMISSION_SEMANTIC_KINDS)}
    for kind in emission.ZERO_EMISSION_KINDS:
        semantic = rows[kind].semantic_kind
        assert invariants._ALLOWED_EXIT_ROLES[semantic] == ("cache_stage",), semantic
        admitted_somewhere = False
        for context in _BODY_CONTEXTS:
            pointer = _body_pointer(context) + "/terminal"
            admitted = kind in bc.BODY_CAPABILITIES_V1[(context, bc.TERMINAL_SLOT)]
            admitted_somewhere |= admitted
            assert bool(stage[semantic].search(pointer)) == admitted, (kind, context, pointer)
        assert admitted_somewhere, (kind, "no terminal union admits it — it is unauthorable")


def test_cache_stage_paths_are_what_the_terminal_unions_admit():
    """``invariants._CACHE_STAGE_PATHS`` equals the model's terminal unions.

    Its keys are the zero-emission semantic kinds. For each such kind and each body
    context, its pattern matches the context's terminal pointer (read from the parser)
    exactly when that terminal union admits the kind. That is a put in branch-leg and
    catch terminals, and a remove in branch-leg terminals only.
    """
    _check_cache_stage_paths_match_terminal_unions()


@pytest.mark.parametrize("kind", sorted(emission.ZERO_EMISSION_KINDS))
def test_the_compile_route_refuses_a_forged_cache_terminal_outside_its_union(kind):
    """The compile route's own check agrees with the unions on a CFG the parser never saw.

    For each context whose terminal union does not admit the kind, forging the compiled
    cache node's source path into that terminal is refused
    ``PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW`` at the forged pointer.
    """
    cfg, _plan, _xml = _compile_and_emit(_terminal_positive_payload(bc.BRANCH_LEG, kind))
    index = next(i for i, node in enumerate(cfg.nodes) if node.exit_role == "cache_stage")
    refused = [c for c in _BODY_CONTEXTS if kind not in bc.BODY_CAPABILITIES_V1[(c, bc.TERMINAL_SLOT)]]
    assert refused, (kind, "admitted everywhere — nothing to forge")
    for context in refused:
        forged_pointer = _body_pointer(context) + "/terminal"
        nodes = list(cfg.nodes)
        nodes[index] = nodes[index].model_copy(update={"source_path": forged_pointer})
        with pytest.raises(ProcessIRCompileError) as exc:
            invariants.check_cfg_invariants(cfg.model_copy(update={"nodes": tuple(nodes)}))
        assert [(d.code, d.path) for d in exc.value.diagnostics][:1] == [
            (PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW, forged_pointer)], (kind, context, exc.value.diagnostics)


# ---------------------------------------------------------------------------
# 3b. Served placements are the placements that compile (QA-184-s1-r14-01)
# ---------------------------------------------------------------------------

_PRIVATE_BODY_CONTEXT = {public: context for context, public in bc.PUBLIC_BODY_CONTEXTS.items()}


@functools.lru_cache(maxsize=1)
def _compiling_placements():
    """``{(context, slot, kind): bool}``: does SOME document placing ``kind`` there compile?

    Measured with the matrix harness, at both entry points. In a terminal slot the kind
    is the body's terminal. In a step slot it is followed by every successor the
    context's slots admit. The element after a step is always one of them, so the sweep
    is exhaustive over what can follow it. Call this before any mutant patches.
    """
    found = {}
    for context in _BODY_CONTEXTS:
        for kind in _CACHE_ACTIONS:
            terminal_doc = _body_model(context, [_MSG], _atom(kind))
            found[(context, bc.TERMINAL_SLOT, kind)] = _verdict(terminal_doc) == ("ACCEPTED",)
            compiled = False
            for position, slot, successor in _body_cells(context, kind):
                if position == "terminal":
                    continue
                if slot == bc.STEP_SLOT:
                    steps, terminal = [_atom(kind), _atom(successor)], _atom("stop")
                else:
                    steps, terminal = [_atom(kind)], _atom(successor)
                if _verdict(_body_model(context, steps, terminal)) == ("ACCEPTED",):
                    compiled = True
                    break
            found[(context, bc.STEP_SLOT, kind)] = compiled
    return found


def _check_served_placements_are_the_placements_that_compile():
    compiles = _compiling_placements()
    served = {(_PRIVATE_BODY_CONTEXT[c], s): set(k) for c, s, k in bc.body_placement_rows()}
    withheld = {(_PRIVATE_BODY_CONTEXT[c], s): set(k) for c, s, k in bc.withheld_body_placement_rows()}
    matrix = {key: set(kinds) for key, kinds in bc.BODY_CAPABILITIES_V1.items()}
    assert set(served) == set(matrix), sorted(set(served) ^ set(matrix))
    assert len(compiles) == len(_BODY_CONTEXTS) * 2 * len(_CACHE_ACTIONS)
    for (context, slot, kind), compiled in sorted(compiles.items()):
        assert (kind in served[(context, slot)]) == compiled, (context, slot, kind, "compiles" if compiled else "refused")
    for (context, slot), kinds in served.items():
        if slot == bc.STEP_SLOT:
            assert not kinds & emission.ZERO_EMISSION_KINDS, (context, sorted(kinds & emission.ZERO_EMISSION_KINDS))
        assert kinds == matrix[(context, slot)] - withheld.get((context, slot), set()), (context, slot)
    for (context, slot), kinds in withheld.items():
        assert slot == bc.STEP_SLOT and kinds <= matrix[(context, slot)] & emission.ZERO_EMISSION_KINDS, (context, slot)
    count = sum(len(kinds) for kinds in withheld.values())
    assert count == sum(len(matrix[(c, bc.STEP_SLOT)] & emission.ZERO_EMISSION_KINDS) for c in _BODY_CONTEXTS)
    assert count >= 10, count


def test_served_placements_are_the_placements_that_compile():
    """The served placement rows are exactly the placements a document can compile with.

    ``body_placement_rows()`` feeds the ``process_ir_authoring`` placements and the
    revision. For every body context, every cache kind (zero-emission and
    triggered-replacement) and both slots, a served ``(context, slot, kind)`` row exists
    exactly when some document placing that kind there compiles, at both entry points.
    No zero-emission kind is served in a step slot. The served rows are the type matrix
    minus exactly the withheld step placements. Floor: ten are withheld today, both
    zero-emission kinds in each of the five step slots.
    """
    _check_served_placements_are_the_placements_that_compile()


def _check_served_placement_entries():
    from boomi_mcp.authoring.process_ir_projection import build_process_ir_authoring_entries

    served = {entry.contract_entry_id: entry for entry in build_process_ir_authoring_entries()}
    withheld = {(context, slot): kinds for context, slot, kinds in bc.withheld_body_placement_rows()}
    placements = [entry for entry in served.values() if entry.entry_type == "placement"]
    naming_cardinality = 0
    for entry in placements:
        context, slot = entry.subject.split(".")
        assert PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY in entry.diagnostic_codes, entry.contract_entry_id
        names = PROCESS_IR_SCHEMA_INVALID_CARDINALITY in entry.diagnostic_codes
        assert names == bool(withheld.get((context, slot))), (entry.contract_entry_id, entry.diagnostic_codes)
        assert not set(entry.node_kinds) & set(withheld.get((context, slot), ())), entry.contract_entry_id
        naming_cardinality += names
    assert 0 < naming_cardinality < len(placements), naming_cardinality
    for kind in sorted(emission.ZERO_EMISSION_KINDS):
        node = served["node." + kind]
        assert node.placements and {p.slot for p in node.placements} == {bc.TERMINAL_SLOT}, node.placements
        fact = next(f for f in node.ordering_facts if f.startswith("Control-body placement:"))
        named = "admitted as " + ", ".join(sorted("{0} {1}".format(p.context, p.slot) for p in node.placements)) + ";"
        assert named in fact, (kind, fact)


def test_served_placement_entries_name_the_code_a_withheld_kind_gets():
    """The served placement entries stay true after the withholding.

    A slot that withholds a type-admitted kind refuses that kind with
    ``PROCESS_IR_SCHEMA_INVALID_CARDINALITY``, measured in ``test_body_placement_matrix``.
    Its entry therefore names that code beside the body-placement code, and an entry
    that withholds nothing does not. The choice is read from the withheld rows, never
    from the slot name. Each zero-emission node serves only terminal placements, and its
    derived "Control-body placement" sentence names exactly those.
    """
    _check_served_placement_entries()


def test_mutant_a_zero_emission_kind_served_in_a_step_slot_is_caught(monkeypatch):
    """Mutant: ``body_capabilities`` stops withholding one zero-emission kind.

    Patched where ``body_placement_rows`` reads the authority, the module-level
    ``ZERO_EMISSION_KINDS`` binding in ``compiler/process_ir/body_capabilities.py``.
    The kind returns to every served step slot. The compile measurement is unchanged,
    so ``test_served_placements_are_the_placements_that_compile`` fails.
    """
    _compiling_placements()
    _check_served_placements_are_the_placements_that_compile()
    restored = sorted(emission.ZERO_EMISSION_KINDS)[0]
    monkeypatch.setattr(bc, "ZERO_EMISSION_KINDS", emission.ZERO_EMISSION_KINDS - {restored})
    assert all(restored in kinds for _c, slot, kinds in bc.body_placement_rows() if slot == bc.STEP_SLOT)
    _expect_failure(_check_served_placements_are_the_placements_that_compile)
    monkeypatch.undo()
    _check_served_placements_are_the_placements_that_compile()


# ---------------------------------------------------------------------------
# 4. Positives: a terminal cache action compiles with no outgoing wire
# ---------------------------------------------------------------------------

_TERMINAL_POSITIVES = tuple(sorted(
    (context, kind)
    for context in _BODY_CONTEXTS
    for kind in emission.ZERO_EMISSION_KINDS
    if kind in bc.BODY_CAPABILITIES_V1[(context, bc.TERMINAL_SLOT)]
))


def _terminal_positive_payload(context, kind):
    action = _atom(kind)
    if context == bc.BRANCH_LEG:
        return _doc({"kind": "branch", "legs": [
            {"steps": [_GET], "terminal": action},
            {"steps": [_GET, _DPP], "terminal": _STOP}]})
    if context == bc.CATCH_BODY:
        return _doc({"kind": "try_catch", "scope": "process",
                     "try_body": {"steps": [_GET], "terminal": _STOP},
                     "catch_body": {"steps": [], "terminal": action}})
    raise AssertionError("no compilable carrier for a terminal {0} in {1}: the union widened".format(kind, context))


def _check_terminal_cache_action_compiles(context, kind):
    payload = _terminal_positive_payload(context, kind)
    cfg, plan, xml = _compile_and_emit(payload)
    row = emission.DOCUMENT_EMISSION_V1[kind]
    staged = [node for node in cfg.nodes if node.semantic.semantic_kind == row.semantic_kind]
    assert [(n.exit_role, _pointer(payload, n.source_path)["kind"]) for n in staged] == [("cache_stage", kind)]
    assert invariants._CACHE_STAGE_PATHS[row.semantic_kind].search(staged[0].source_path)
    rendered = [node for node in plan.nodes if node.source_path == staged[0].source_path]
    assert len(rendered) == 1 and rendered[0].emitter_input.emitter_kind in row.emitter_kinds
    assert rendered[0].outgoing == ()
    shape_type, wires, has_dragpoints = _outbound(xml)[rendered[0].shape_id]
    assert shape_type in row.emitter_kinds and has_dragpoints and wires == [], (shape_type, wires)
    assert graph_verifier.verify_process_graph(xml)["errors"] == []
    return plan


def test_every_zero_emission_kind_has_an_admitted_terminal():
    assert {kind for _c, kind in _TERMINAL_POSITIVES} == emission.ZERO_EMISSION_KINDS, _TERMINAL_POSITIVES


@pytest.mark.parametrize("context,kind", _TERMINAL_POSITIVES)
def test_a_terminal_cache_action_compiles_with_no_outgoing_wire(context, kind):
    """A successful terminal put and remove. The CFG node carries ``cache_stage``, the plan
    node has no transition, and the rendered shape has an empty ``<dragpoints/>`` that the
    graph verifier accepts."""
    _check_terminal_cache_action_compiles(context, kind)


def _check_an_outgoing_cache_wire_is_refused_before_bytes():
    forged_keys = set()
    for context, kind in _TERMINAL_POSITIVES:
        if context != bc.BRANCH_LEG:
            continue
        _cfg, plan, _xml = _compile_and_emit(_terminal_positive_payload(context, kind))
        nodes = list(plan.nodes)
        stop = next(n for n in nodes if n.emitter_input.emitter_kind == "stop")
        index = next(i for i, n in enumerate(nodes) if n.emitter_input.emitter_kind in emission.ZERO_EMISSION_EMITTER_KINDS)
        sink = nodes[index]
        wire = EmissionTransitionV1(local_ordinal=1, dragpoint_name=sink.shape_id + ".dragpoint1",
                                    to_shape_id=stop.shape_id, x=1.0, y=1.0, provenance="cfg_edge")
        nodes[index] = sink.model_copy(update={"outgoing": (wire,)})
        reg = registry.registration_for(sink.emitter_input.emitter_kind)
        assert not registry._cardinality_ok(reg.outgoing, sink.emitter_input, nodes[index])
        with pytest.raises(ProcessIRCompileError) as exc:
            registry.emit_process(plan.model_copy(update={"nodes": tuple(nodes)}), _symbols())
        assert [(d.code, d.phase, d.path) for d in exc.value.diagnostics][:1] == [
            (PROCESS_IR_COMPILE_EMITTER_INPUT_INVALID, "xml_emission", sink.source_path)], exc.value.diagnostics
        forged_keys.add(sink.emitter_input.emitter_kind)
    assert forged_keys == emission.ZERO_EMISSION_EMITTER_KINDS, forged_keys


def test_an_outgoing_cache_wire_is_refused_at_emitter_preflight():
    """A forged plan wiring a cache sink onward is refused by the registry's preflight,
    before any bytes exist, at the sink's pointer. The verifier is not the first guard."""
    _check_an_outgoing_cache_wire_is_refused_before_bytes()


# ---------------------------------------------------------------------------
# 4b. Active goldens: no cache sink carries a wire (amendment 3 §12)
# ---------------------------------------------------------------------------


def _local(tag):
    return tag.rsplit("}", 1)[-1]


def _sink_dragpoints(xml_bytes):
    """``[(shape name, shape type, dragpoint count)]`` for every zero-emission sink shape.

    Sink types are the authority's ``ZERO_EMISSION_EMITTER_KINDS``, which are also the
    platform shape types. Any ``<dragpoint>`` under a sink's ``<dragpoints>`` is an
    outgoing wire the platform never follows.
    """
    sinks = []
    for element in ET.fromstring(xml_bytes).iter():
        if _local(element.tag) == "shape" and element.get("shapetype") in emission.ZERO_EMISSION_EMITTER_KINDS:
            count = sum(1 for dragpoints in element if _local(dragpoints.tag) == "dragpoints"
                        for dragpoint in dragpoints if _local(dragpoint.tag) == "dragpoint")
            sinks.append((element.get("name"), element.get("shapetype"), count))
    return sinks


@functools.lru_cache(maxsize=1)
def _golden_manifest():
    """The golden manifest, read through the wave gate's own strict parser (one parser)."""
    spec = importlib.util.spec_from_file_location("_issue184_wave_gate", _ROOT / "scripts" / "wave_gate.py")
    gate = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gate)
    return gate.parse_manifest((_ROOT / gate.GOLDENS_MANIFEST).read_bytes(), "goldens")


def test_no_active_golden_wires_a_cache_sink_onward():
    """Amendment 3 §12: scan every ACTIVE golden's expected XML for cache-sink wires.

    Rows come from ``tests/fixtures/wave_gate/goldens.jsonl``; no sink shape may carry a
    dragpoint. Floors: the scanned count may not fall below the manifest's own
    ``minimum_active``, every sink type occurs in some golden, and the two #184
    replacement goldens that carry a removal are among those scanned.
    """
    manifest = _golden_manifest()
    active = list(manifest.active)
    assert len(active) >= manifest.header["minimum_active"] > 0, (len(active), manifest.header)
    with_sinks = {}
    for row in active:
        sinks = _sink_dragpoints((_ROOT / row["expected_file"]).read_bytes())
        if sinks:
            with_sinks[row["id"]] = sinks
    wired = {gid: [s for s in sinks if s[2]] for gid, sinks in with_sinks.items() if any(s[2] for s in sinks)}
    assert not wired, wired
    assert {kind for sinks in with_sinks.values() for _name, kind, _count in sinks} == emission.ZERO_EMISSION_EMITTER_KINDS
    assert {"golden-000082", "golden-000083"} <= set(with_sinks), sorted(with_sinks)


def test_the_golden_sink_scan_sees_a_forged_wire():
    """Negative control: an in-memory copy of an active golden with its sink wired onward
    fails the scan's predicate, and the graph verifier reports the same shape. The
    committed file is never written."""
    row = next(r for r in _golden_manifest().active if r["id"] == "golden-000082")
    path = _ROOT / row["expected_file"]
    original = path.read_bytes()
    assert [count for _name, _kind, count in _sink_dragpoints(original)] == [0]
    root = ET.fromstring(original)
    shapes = [element for element in root.iter() if _local(element.tag) == "shape"]
    sink = next(s for s in shapes if s.get("shapetype") in emission.ZERO_EMISSION_EMITTER_KINDS)
    other = next(s for s in shapes if s is not sink)
    dragpoints = next(child for child in sink if _local(child.tag) == "dragpoints")
    ET.SubElement(dragpoints, dragpoints.tag[: -len("dragpoints")] + "dragpoint",
                  {"name": sink.get("name") + ".dragpoint1", "toShape": other.get("name"), "x": "1", "y": "1"})
    forged = ET.tostring(root)
    assert [count for _name, _kind, count in _sink_dragpoints(forged)] == [1]
    errors = graph_verifier.verify_process_graph(forged.decode("utf-8"))["errors"]
    assert ("TERMINAL_SHAPE_HAS_OUTBOUND", sink.get("name")) in {(e["code"], e["shape"]) for e in errors}, errors
    assert path.read_bytes() == original


# ---------------------------------------------------------------------------
# 5. Trigger: a read needs an arriving document
# ---------------------------------------------------------------------------


def _check_a_read_after_an_exhausted_path_is_refused():
    pointer = _body_pointer(bc.BRANCH_LEG)
    checked = 0
    for zero in sorted(emission.ZERO_EMISSION_KINDS):
        for read in sorted(emission.TRIGGERED_REPLACEMENT_KINDS):
            verdict = _verdict(_body_model(bc.BRANCH_LEG, [_GET, _atom(zero), _atom(read), _PATCH], _STOP))
            assert _first(verdict) == (_CARD, pointer + "/steps/1/cache_ref"), (zero, read, verdict)
            checked += 1
    assert checked == len(emission.ZERO_EMISSION_KINDS) * len(emission.TRIGGERED_REPLACEMENT_KINDS) > 0


def test_a_read_after_an_exhausted_path_is_refused():
    """A write or removal exhausts the path, so a read after it has nothing to trigger it
    and never restarts the path (captures cap184-cache-put-successor rows 0-1,
    cap184-cache-remove-read row 0). Refused at the exhausting step at both entry points."""
    _check_a_read_after_an_exhausted_path_is_refused()


def _triggered_read_payload(read):
    return _doc({"kind": "branch", "legs": [
        {"steps": [_GET], "terminal": _atom("cache_put")},
        {"steps": [_atom(read), _PATCH], "terminal": _STOP}]})


def _check_a_triggered_read_compiles(read):
    payload = _triggered_read_payload(read)
    _cfg, plan, xml = _compile_and_emit(payload)
    shapes = _outbound(xml)
    by_path = {node.source_path: node for node in plan.nodes}
    read_node = by_path["/body/steps/0/legs/1/steps/0"]
    assert read_node.emitter_input.emitter_kind in _ROWS[read].emitter_kinds
    assert len(read_node.outgoing) == 1 and len(shapes[read_node.shape_id][1]) == 1
    sink = by_path["/body/steps/0/legs/0/terminal"]
    assert shapes[sink.shape_id][1] == []


@pytest.mark.parametrize("read", sorted(emission.TRIGGERED_REPLACEMENT_KINDS))
def test_a_read_on_a_triggered_path_compiles_with_its_one_wire(read):
    """A read in a later Branch leg receives that leg's copy of the arriving document, so
    it runs. Both aliases compile, and each renders with exactly one outgoing wire."""
    _check_a_triggered_read_compiles(read)


_MAP_STEP = {"kind": "map_ref", "map_ref": "$ref:MAP"}
#: A map whose source profile (P2) contradicts what the leg's call hands on (P1). A cache
#: stream nothing proves AND a cache stream proven to carry P1 are both refused at it,
#: so a sink that stops exhausting the path is caught whether it clears the content (a
#: removal) or stages the call's documents (a put).
_MISMATCHED_MAP_STEP = {"kind": "map_ref", "map_ref": "$ref:MAP2"}
_LEG = "/body/steps/0/legs/0"


def _behind_the_parser(steps):
    """A Branch leg the parser refuses, lowered and prepared WITHOUT re-validation.

    The walks behind the parser are unreachable through either public entry point: the
    parser refuses a sink's successor first, then ``check_cfg_invariants``. They are
    called directly here. ``prepare_validation_context`` re-validates its input, so the
    lineage context is assembled from the same parts it builds.
    """
    symbols = SymbolTableV1(symbols=tuple(_symbols().symbols) + (
        ComponentSymbolV1(ref="$ref:MAP", component_id="MAP", component_type="transform.map",
                          input_profile_ref="$ref:P1", output_profile_ref="$ref:P1"),
        ComponentSymbolV1(ref="$ref:P2", component_id="P2", component_type="profile.json"),
        ComponentSymbolV1(ref="$ref:MAP2", component_id="MAP2", component_type="transform.map",
                          input_profile_ref="$ref:P2", output_profile_ref="$ref:P1"),))
    ir = parse_process_ir_v1(_doc({"kind": "branch", "legs": [
        {"steps": [_GET, _MSG], "terminal": _STOP}, {"steps": [_GET, _DPP], "terminal": _STOP}]}))
    ir.body.steps[0].legs[0].steps = [_NODE.validate_python(copy.deepcopy(s)) for s in steps]
    cfg = validation_context.canonical_cache_cfg(
        lowering.lower_process_ir_to_cfg(ir), validation_context.canonical_cache_refs(symbols))
    return validation_context.PreparedProcessValidationV1(
        ir=ir, cfg=cfg, symbols=symbols, node_by_id={node.node_id: node for node in cfg.nodes},
        outgoing=validation_context._edge_index(cfg.edges, "source_node_id"),
        incoming=validation_context._edge_index(cfg.edges, "target_node_id"),
        symbol_by_ref={symbol.ref: symbol for symbol in symbols.symbols},
    )


def _walk_findings(prepared):
    try:
        connector_resolution.validate_connector_calls(prepared.cfg, prepared.symbols)
    except ProcessIRCompileError as exc:
        return [(d.code, d.path) for d in exc.diagnostics]
    return []


def _lineage_profile_findings(prepared):
    return [(d.code, d.path) for d in lineage.collect_lineage_findings(prepared)
            if d.code == PROCESS_IR_SEMANTIC_PROFILE_MISMATCH]


@pytest.mark.parametrize("zero", sorted(emission.ZERO_EMISSION_KINDS))
def test_behind_the_parser_the_connector_walk_does_not_restart_an_exhausted_path(zero, monkeypatch):
    """``connector_resolution._walk_paths``: after a sink the read produces nothing.

    The same leg without the sink passes the walk. With it, the map after the read has
    no producer and is refused ``PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH`` at its
    ``/map_ref``. With the walk's own ``ZERO_EMISSION_SEMANTIC_KINDS`` emptied, the
    refusal disappears, so the branch that reads the authority is what refuses.
    """
    assert _walk_findings(_behind_the_parser([_GET, _atom("cache_get"), _MAP_STEP, _PATCH])) == []
    exhausted = _behind_the_parser([_GET, _atom(zero), _atom("cache_get"), _MAP_STEP, _PATCH])
    assert _walk_findings(exhausted) == [(PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH, _LEG + "/steps/3/map_ref")]
    monkeypatch.setattr(connector_resolution, "ZERO_EMISSION_SEMANTIC_KINDS", frozenset())
    assert _walk_findings(exhausted) == []


@pytest.mark.parametrize("zero", sorted(emission.ZERO_EMISSION_KINDS))
def test_behind_the_parser_lineage_invents_no_stream_after_a_sink(zero):
    """``lineage._advance_stream``: a read on an absent stream invents nothing.

    Without the sink, the read yields a cache stream nothing proves, and the map after
    it is refused ``PROCESS_IR_SEMANTIC_PROFILE_MISMATCH``. After a sink the stream is
    absent, the read never runs, and no stream reaches the map to judge.
    """
    control = _behind_the_parser([_GET, _atom("cache_get"), _MISMATCHED_MAP_STEP, _PATCH])
    assert _lineage_profile_findings(control) == [(PROCESS_IR_SEMANTIC_PROFILE_MISMATCH, _LEG + "/steps/2/map_ref")]
    exhausted = _behind_the_parser([_GET, _atom(zero), _atom("cache_get"), _MISMATCHED_MAP_STEP, _PATCH])
    assert _lineage_profile_findings(exhausted) == []


def test_lineage_reads_the_authority_for_every_sink(monkeypatch):
    """Non-vacuity of the lineage import, for EVERY sink kind.

    With lineage's ``ZERO_EMISSION_SEMANTIC_KINDS`` emptied, no sink exhausts the path
    any more, and the read runs. After a removal it yields a cache stream nothing proves;
    after a put, the staged P1 documents. Either contradicts the P2 map, whose refusal
    returns. A sink branch that returned an absent stream on its own kind name would stay
    silent here (measured: the pre-fix put branch did). The map must contradict the
    staged profile. A matching map passes the put's proven stream and would not see it.
    """
    exhausted = {zero: _behind_the_parser([_GET, _atom(zero), _atom("cache_get"), _MISMATCHED_MAP_STEP, _PATCH])
                 for zero in sorted(emission.ZERO_EMISSION_KINDS)}
    assert all(not _lineage_profile_findings(prepared) for prepared in exhausted.values())
    monkeypatch.setattr(lineage, "ZERO_EMISSION_SEMANTIC_KINDS", frozenset())
    reached = {zero for zero, prepared in exhausted.items()
               if _lineage_profile_findings(prepared) == [(PROCESS_IR_SEMANTIC_PROFILE_MISMATCH, _LEG + "/steps/3/map_ref")]}
    assert reached == emission.ZERO_EMISSION_KINDS, {"not_reached": sorted(emission.ZERO_EMISSION_KINDS - reached)}


# ---------------------------------------------------------------------------
# 6. Mutants, measured
# ---------------------------------------------------------------------------


def _targets(stmt):
    targets = stmt.targets if isinstance(stmt, ast.Assign) else [stmt.target]
    return [t.id for t in targets if isinstance(t, ast.Name)]


def _names(node):
    return {n.id for n in ast.walk(node) if isinstance(n, ast.Name)}


def _called(node):
    return {n.func.id for n in ast.walk(node) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)}


def _reevaluate_dependents(monkeypatch, module, dirty):
    """Re-evaluate the module-level values composed from ``dirty`` names, in source order.

    A value depends on a name it reads, or on a module function it CALLS whose body reads
    one (transitively). The expression is evaluated in the module's own namespace.
    """
    tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
    functions = {s.name: s for s in tree.body if isinstance(s, ast.FunctionDef)}
    dirty = set(dirty)
    reached = []

    def dependent_functions():
        found = set()
        changed = True
        while changed:
            changed = False
            for name, fn in functions.items():
                if name not in found and (_names(fn) & (dirty | found)):
                    found.add(name)
                    changed = True
        return found

    for stmt in tree.body:
        if not isinstance(stmt, (ast.Assign, ast.AnnAssign)) or stmt.value is None:
            continue
        if _names(stmt.value) & dirty or _called(stmt.value) & dependent_functions():
            value = eval(compile(ast.Expression(stmt.value), module.__file__, "eval"), vars(module))  # noqa: S307
            for target in _targets(stmt):
                monkeypatch.setattr(module, target, value)
                dirty.add(target)
                reached.append("{0}.{1}".format(module.__name__, target))
    return reached


def _rewire_authority(monkeypatch, rows):
    """Re-derive the authority from ``rows`` with its own code; rebind every copy of it.

    1. The authority module is executed afresh, ``_ROWS`` replaced, and every statement
       after ``_ROWS`` re-run: the derived sets come from the module's own code.
    2. Every loaded ``boomi_mcp`` module global that IS one of the old derived objects is
       rebound to the new one (``from ... import X as Y`` copies, and
       ``ROOT_ENTRY_READ_KINDS = TRIGGERED_REPLACEMENT_KINDS`` aliases).
    3. In each module touched, module-level values composed from those names are
       re-evaluated (the verifier's shape sets, the legacy builder's kind set, the
       registry's registrations).

    Returns the bindings reached, so a test can prove every consumer was reached.
    """
    source = Path(emission.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    split = next(i for i, s in enumerate(tree.body) if isinstance(s, ast.Assign) and "_ROWS" in _targets(s))
    derivations = tree.body[split + 1:]
    namespace = {"__name__": emission.__name__, "__file__": emission.__file__}
    exec(compile(source, emission.__file__, "exec"), namespace)  # noqa: S102
    namespace["_ROWS"] = tuple(rows)
    exec(compile(ast.Module(body=derivations, type_ignores=[]), emission.__file__, "exec"), namespace)  # noqa: S102
    derived = [name for s in derivations if isinstance(s, (ast.Assign, ast.AnnAssign))
               for name in _targets(s) if name != "__all__"]
    pairs = [(getattr(emission, name), namespace[name]) for name in derived]
    monkeypatch.setattr(emission, "_ROWS", tuple(rows))
    for name in derived:
        monkeypatch.setattr(emission, name, namespace[name])
    reached = []
    for module_name, module in sorted(sys.modules.items()):
        if not module_name.startswith("boomi_mcp") or module is emission or module is None:
            continue
        dirty = set()
        for attr, value in list(vars(module).items()):
            for old, new in pairs:
                if value is old:
                    monkeypatch.setattr(module, attr, new)
                    dirty.add(attr)
                    reached.append("{0}.{1}".format(module_name, attr))
        if dirty and getattr(module, "__file__", None):
            reached += _reevaluate_dependents(monkeypatch, module, dirty)
    for name, (_module, imports) in _authority_consumer_modules().items():
        for _source_name, local_name in imports:
            assert "{0}.{1}".format(name, local_name) in reached, (name, local_name, "not rewired")
    return reached


def _rows_with(transform):
    return [row for row in (transform(r) for r in emission._ROWS) if row is not None]


def _expect_failure(check, *args):
    """The named check fails: an assertion, or the compiler refusing what it must admit.

    Deliberately not ``Exception``: a harness defect (a ``NameError``, a ``KeyError``)
    must surface as itself, never pass as a caught mutant.
    """
    with pytest.raises((AssertionError, ProcessIRCompileError, model.ProcessIRValidationError)):
        check(*args)


def test_mutant_inverted_consumption_is_caught(monkeypatch):
    """Mutant: ``cache_put`` → ``RESULT_FORWARDS``.

    Reaches the model verdict, lowering, the connector walk, lineage, the registry
    cardinality, the verifier's terminal sets and the legacy builder, all at once.
    Fails: ``test_each_measured_row_states_what_its_evidence_decides`` (the row
    contradicts ``cache_load.emits_zero_documents``),
    ``test_cache_stage_paths_are_what_the_terminal_unions_admit`` and
    ``test_a_terminal_cache_action_compiles_with_no_outgoing_wire``. Downstream
    derivations agree with the inverted table, so a
    matrix whose expectations come from the table cannot see it. The evidence binding
    and the hand-kept stage paths can, and do.
    """
    positive = (bc.BRANCH_LEG, "cache_put")
    _check_measured_rows_state_their_evidence()
    _check_cache_stage_paths_match_terminal_unions()
    _check_terminal_cache_action_compiles(*positive)

    reached = _rewire_authority(monkeypatch, _rows_with(
        lambda r: dataclasses.replace(r, result=emission.RESULT_FORWARDS) if r.kind == "cache_put" else r))
    assert "cache_put" not in model.ZERO_EMISSION_KINDS
    assert registry.registration_for("doccacheload").outgoing == registry.EXACT_ONE
    assert "doccacheload" not in graph_verifier._ALWAYS_TERMINAL_SHAPE_TYPES
    assert "cache_put" not in legacy_builder._LEGACY_ZERO_EMISSION_KINDS
    assert "{0}._REGISTRY".format(registry.__name__) in reached
    _expect_failure(_check_measured_rows_state_their_evidence)
    _expect_failure(_check_cache_stage_paths_match_terminal_unions)
    _expect_failure(_check_terminal_cache_action_compiles, *positive)

    monkeypatch.undo()
    _check_measured_rows_state_their_evidence()
    _check_cache_stage_paths_match_terminal_unions()
    _check_terminal_cache_action_compiles(*positive)


def test_mutant_omitted_alias_is_caught(monkeypatch):
    """Mutant: drop the ``document_cache_retrieve`` row.

    Reaches ``TRIGGERED_REPLACEMENT_KINDS`` and its semantic twin, and through them the
    model's ``ROOT_ENTRY_READ_KINDS``, the connector walk and lineage. Fails:
    ``test_every_authored_kind_has_a_row``,
    ``test_every_semantic_kind_has_a_row_and_every_row_names_a_real_one``,
    ``test_every_emitter_key_has_a_row_and_aliases_state_one_physical_step`` and
    ``test_a_read_on_a_triggered_path_compiles_with_its_one_wire[document_cache_retrieve]``.
    The emitter-key half of that third check passes on its own, because ``cache_get``
    keeps ``doccacheretrieve`` covered. It fails only on its known-member alias floor,
    which is why an alias needs its own row check.
    """
    _check_every_authored_kind_has_a_row()
    _check_every_semantic_kind_has_a_row()
    _check_a_triggered_read_compiles("document_cache_retrieve")

    reached = _rewire_authority(monkeypatch, _rows_with(
        lambda r: None if r.kind == "document_cache_retrieve" else r))
    assert "document_cache_retrieve" not in model.ROOT_ENTRY_READ_KINDS
    assert "{0}.ROOT_ENTRY_READ_KINDS".format(model.__name__) in reached
    assert registry.registry_keys() == {k for r in emission.DOCUMENT_EMISSION_V1.values() for k in r.emitter_kinds}
    _expect_failure(_check_emitter_keys_and_aliases)
    _expect_failure(_check_every_authored_kind_has_a_row)
    _expect_failure(_check_every_semantic_kind_has_a_row)
    _expect_failure(_check_a_triggered_read_compiles, "document_cache_retrieve")

    monkeypatch.undo()
    _check_every_authored_kind_has_a_row()
    _check_every_semantic_kind_has_a_row()
    _check_a_triggered_read_compiles("document_cache_retrieve")


def test_mutant_restored_read_after_sink_exception_is_caught(monkeypatch):
    """Mutant: the withdrawn rule that a cache read straight after a sink restarts the path.

    Patched where every caller reads it: ``models.process_ir.terminal_cache_action_verdict``,
    looked up by ``_check_terminal_cache_actions`` at all of its call sites. Both entry
    points reach it, the compile route through its re-parse. Fails:
    ``test_body_placement_matrix`` (every zero-emission cell whose successor is a read)
    and ``test_a_read_after_an_exhausted_path_is_refused``. Measured next line of defence:
    a FULL compile is still refused by ``check_cfg_invariants``
    (``PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW`` at the sink step), because a role-less sink
    has no admitted exit role.
    """
    real = model.terminal_cache_action_verdict

    def restored(steps, *, followed):
        verdict = real(steps, followed=followed)
        if verdict is None:
            return None
        index = verdict[0][1]
        nxt = steps[index + 1] if index + 1 < len(steps) else None
        if getattr(nxt, "kind", None) in emission.TRIGGERED_REPLACEMENT_KINDS:
            return None
        return verdict

    def read_successor_cells():
        for context in _BODY_CONTEXTS:
            for action in sorted(emission.ZERO_EMISSION_KINDS):
                for position, slot, successor in _body_cells(context, action):
                    if successor in emission.TRIGGERED_REPLACEMENT_KINDS and slot == bc.STEP_SLOT:
                        _check_body_cell(context, action, position, slot, successor)

    read_successor_cells()
    _check_a_read_after_an_exhausted_path_is_refused()

    monkeypatch.setattr(model, "terminal_cache_action_verdict", restored)
    _expect_failure(read_successor_cells)
    _expect_failure(_check_a_read_after_an_exhausted_path_is_refused)
    payload = _doc({"kind": "branch", "legs": [
        {"steps": [_GET, _atom("cache_put"), _atom("cache_get"), _PATCH], "terminal": _STOP},
        {"steps": [_GET, _DPP], "terminal": _STOP}]})
    with pytest.raises(ProcessIRCompileError) as exc:
        _compile_and_emit(payload)
    assert [(d.code, d.path) for d in exc.value.diagnostics][:1] == [
        (PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW, "/body/steps/0/legs/0/steps/1")], exc.value.diagnostics

    monkeypatch.undo()
    read_successor_cells()
    _check_a_read_after_an_exhausted_path_is_refused()


def test_mutant_outgoing_cache_wire_in_registry_is_caught(monkeypatch):
    """Mutant: restore the withdrawn ``zero_or_one`` cardinality for the cache sinks.

    Patched in ``compiler/process_ir/emitter_registry.py``: ``_cache_cardinality`` and
    ``_cardinality_ok``, then the registrations and ``_REGISTRY`` re-evaluated from them.
    Fails: ``test_registry_cardinality_is_the_rows_continuation`` and
    ``test_an_outgoing_cache_wire_is_refused_at_emitter_preflight``. Measured: the forged
    wire then passes preflight and is caught only after rendering, by the graph verifier
    (``PROCESS_IR_COMPILE_VERIFIER_FAILED``). A terminal sink with no wire still compiles,
    so the mutant PERMITS a wire rather than requiring one.
    """
    _check_registry_cardinality_is_the_rows_continuation()
    _check_an_outgoing_cache_wire_is_refused_before_bytes()

    zero_or_one = registry.OutgoingCardinality("zero_or_one", 1)
    real_ok = registry._cardinality_ok

    def cache_cardinality(key):
        return zero_or_one if key in registry.ZERO_EMISSION_EMITTER_KINDS else registry.EXACT_ONE

    def cardinality_ok(card, inp, node):
        return len(node.outgoing) <= 1 if card.kind == "zero_or_one" else real_ok(card, inp, node)

    monkeypatch.setattr(registry, "_cache_cardinality", cache_cardinality)
    monkeypatch.setattr(registry, "_cardinality_ok", cardinality_ok)
    reached = _reevaluate_dependents(monkeypatch, registry, {"_cache_cardinality"})
    assert "{0}._REGISTRY".format(registry.__name__) in reached, reached
    assert registry.registration_for("doccacheload").outgoing == zero_or_one
    _expect_failure(_check_registry_cardinality_is_the_rows_continuation)
    _expect_failure(_check_an_outgoing_cache_wire_is_refused_before_bytes)
    for context, kind in _TERMINAL_POSITIVES:
        _check_terminal_cache_action_compiles(context, kind)

    monkeypatch.undo()
    _check_registry_cardinality_is_the_rows_continuation()
    _check_an_outgoing_cache_wire_is_refused_before_bytes()
