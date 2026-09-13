"""The document-emission authority for ProcessIR v1 (#184 amendment 3 §1).

ONE table states what each step does with documents. Before it, the fact lived in
separate hand-written rules that disagreed with the platform:
- the model required a cache read straight after a cache write;
- the connector walk treated a cache read as a producer from nothing;
- the emitter registry let a cache load or remove continue to a successor;
- the graph verifier called a load able to continue;
- the legacy builder wired a Stop or an Exception after a cache load.

The platform skips any step that receives no documents, and the run still reads
COMPLETE.

Each row separates three things an emitter's outgoing-edge count used to stand in for:

* **trigger** — what must arrive for the step to execute;
* **result** — what the step hands on: the arriving documents, a replacement set,
  nothing, or an operation-dependent count;
* **continuation** — the outgoing graph structure the step may have.

Evidence status is recorded per row. Only the cache rows and the scheduled start
were measured by #184. Their deciding captures are bound, rule by rule, in
``docs/architecture/evidence/issue-184/document_emission/CAPTURE_INDEX.json``. Every
other row restates behaviour already shipped and verified elsewhere, and is labelled
``established``, never ``measured``. Connector output stays with the connector
capability authority, which resolves ``operation_dependent`` per operation.

LEAF MODULE: it imports nothing from this package, so the model, the compiler, the
emitters, the graph verifier and the legacy builder can all read it without a cycle.
Coverage is pinned in both directions by ``tests/test_issue_184_document_emission.py``:
- every model kind, semantic kind and emitter key has a row;
- every row names a real kind;
- every measured row cites indexed evidence.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import FrozenSet, Mapping, Optional, Tuple

# --- trigger -----------------------------------------------------------------
#: Executes only when at least one document arrives. The scheduled Start's single
#: empty document counts as an arriving document.
TRIGGER_ARRIVING_DOCUMENT = "arriving_document"
#: Runs when the process starts; nothing needs to arrive.
TRIGGER_PROCESS_START = "process_start"
#: Not a step: the root container.
TRIGGER_CONTAINER = "container"

# --- result ------------------------------------------------------------------
#: Supplies the process's first documents. A scheduled start supplies one empty
#: document, a listener the inbound request, and a passthrough entry the caller's group.
RESULT_SUPPLIES = "supplies"
#: Hands on the arriving documents, possibly rewritten one for one.
RESULT_FORWARDS = "forwards"
#: Replaces the payload with the documents it retrieves. An empty source hands on nothing.
RESULT_REPLACES = "replaces"
#: Emits zero documents: whatever follows it on the same path never runs.
RESULT_CONSUMES = "consumes"
#: The count depends on the configured operation (a connector's declared output,
#: a data process's split or combine), resolved by that step's own authority.
RESULT_OPERATION_DEPENDENT = "operation_dependent"
#: Routes the arriving documents into its bodies.
RESULT_ROUTES = "routes"
#: Ends the path it is on (the documents stop, return to a caller, raise, or are
#: handed to a called process).
RESULT_ENDS = "ends"
#: Not a step.
RESULT_CONTAINER = "container"

# --- continuation ------------------------------------------------------------
#: No outgoing edge.
CONTINUATION_NONE = "none"
#: Exactly one sequential successor.
CONTINUATION_ONE = "one"
#: One edge per authored body (branch legs, decision arms, try and catch).
CONTINUATION_CONTROL = "control"
#: Not a step.
CONTINUATION_CONTAINER = "container"

# --- evidence ----------------------------------------------------------------
MEASURED = "measured"
ESTABLISHED = "established"
OPEN = "open"


@dataclass(frozen=True)
class DocumentEmissionV1:
    """What one step does with documents."""

    kind: str
    trigger: str
    result: str
    continuation: str
    evidence: str
    #: The compiler's semantic kind this authored kind lowers to, or None for a
    #: node that lowers to no semantic node of its own.
    semantic_kind: Optional[str]
    #: The emitter keys that may render it. Two authored kinds naming the same key
    #: are aliases of one physical step.
    emitter_kinds: Tuple[str, ...]
    #: Rule ids in the capture index that decide a measured row.
    capture_rules: Tuple[str, ...] = ()


def _row(kind, trigger, result, continuation, evidence, semantic_kind, emitter_kinds, capture_rules=()):
    return DocumentEmissionV1(
        kind, trigger, result, continuation, evidence, semantic_kind, tuple(emitter_kinds), tuple(capture_rules)
    )


_RETRIEVE_RULES = (
    "cache_retrieve.requires_arriving_document",
    "cache_retrieve.empty_cache_emits_no_documents",
    "cache_retrieve.replaces_payload_with_cached_documents",
)

_ROWS = (
    # The scheduled start is synthesized, never authored; its row states what the
    # read-led call-free root relies on.
    _row("start", TRIGGER_PROCESS_START, RESULT_SUPPLIES, CONTINUATION_ONE, MEASURED, None,
         ("start_noaction",), ("start.scheduled_supplies_one_empty_document",)),
    _row("listener", TRIGGER_PROCESS_START, RESULT_SUPPLIES, CONTINUATION_ONE, ESTABLISHED, "listener",
         ("start_listen",)),
    _row("passthrough", TRIGGER_PROCESS_START, RESULT_SUPPLIES, CONTINUATION_ONE, ESTABLISHED, "passthrough",
         ("start_passthrough",)),
    _row("source", TRIGGER_ARRIVING_DOCUMENT, RESULT_OPERATION_DEPENDENT, CONTINUATION_ONE, ESTABLISHED, "connector",
         ("connectoraction_source",)),
    _row("target", TRIGGER_ARRIVING_DOCUMENT, RESULT_OPERATION_DEPENDENT, CONTINUATION_ONE, ESTABLISHED, "connector",
         ("connectoraction_target",)),
    _row("connector_call", TRIGGER_ARRIVING_DOCUMENT, RESULT_OPERATION_DEPENDENT, CONTINUATION_ONE, ESTABLISHED,
         "connector_call", ("connectoraction_source", "connectoraction_target")),
    _row("map_ref", TRIGGER_ARRIVING_DOCUMENT, RESULT_FORWARDS, CONTINUATION_ONE, ESTABLISHED, "map", ("map",)),
    _row("message", TRIGGER_ARRIVING_DOCUMENT, RESULT_FORWARDS, CONTINUATION_ONE, ESTABLISHED, "message",
         ("message",)),
    _row("data_process", TRIGGER_ARRIVING_DOCUMENT, RESULT_OPERATION_DEPENDENT, CONTINUATION_ONE, ESTABLISHED,
         "data_process", ("dataprocess",)),
    _row("flow_control", TRIGGER_ARRIVING_DOCUMENT, RESULT_FORWARDS, CONTINUATION_ONE, ESTABLISHED, "flow_control",
         ("flowcontrol",)),
    _row("set_ddp", TRIGGER_ARRIVING_DOCUMENT, RESULT_FORWARDS, CONTINUATION_ONE, ESTABLISHED, "set_property",
         ("setproperties_step",)),
    _row("set_dpp", TRIGGER_ARRIVING_DOCUMENT, RESULT_FORWARDS, CONTINUATION_ONE, ESTABLISHED, "set_property",
         ("setproperties_step",)),
    _row("notify", TRIGGER_ARRIVING_DOCUMENT, RESULT_FORWARDS, CONTINUATION_ONE, ESTABLISHED, "notify",
         ("notify",)),
    _row("cache_put", TRIGGER_ARRIVING_DOCUMENT, RESULT_CONSUMES, CONTINUATION_NONE, MEASURED, "cache_put",
         ("doccacheload",), ("cache_load.emits_zero_documents", "cache_load.terminal_executes")),
    _row("cache_remove", TRIGGER_ARRIVING_DOCUMENT, RESULT_CONSUMES, CONTINUATION_NONE, MEASURED, "cache_remove",
         ("doccacheremove",), ("cache_remove_all.emits_zero_documents",)),
    _row("cache_get", TRIGGER_ARRIVING_DOCUMENT, RESULT_REPLACES, CONTINUATION_ONE, MEASURED, "cache_get",
         ("doccacheretrieve",), _RETRIEVE_RULES),
    _row("document_cache_retrieve", TRIGGER_ARRIVING_DOCUMENT, RESULT_REPLACES, CONTINUATION_ONE, MEASURED,
         "document_cache_retrieve", ("doccacheretrieve",), _RETRIEVE_RULES),
    _row("branch", TRIGGER_ARRIVING_DOCUMENT, RESULT_ROUTES, CONTINUATION_CONTROL, ESTABLISHED, "branch",
         ("branch",)),
    _row("decision", TRIGGER_ARRIVING_DOCUMENT, RESULT_ROUTES, CONTINUATION_CONTROL, ESTABLISHED, "decision",
         ("decision",)),
    _row("try_catch", TRIGGER_ARRIVING_DOCUMENT, RESULT_ROUTES, CONTINUATION_CONTROL, ESTABLISHED, "try_catch",
         ("catcherrors",)),
    _row("stop", TRIGGER_ARRIVING_DOCUMENT, RESULT_ENDS, CONTINUATION_NONE, ESTABLISHED, "stop", ("stop",)),
    _row("return_documents", TRIGGER_ARRIVING_DOCUMENT, RESULT_ENDS, CONTINUATION_NONE, ESTABLISHED,
         "return_documents", ("returndocuments",)),
    _row("exception", TRIGGER_ARRIVING_DOCUMENT, RESULT_ENDS, CONTINUATION_NONE, ESTABLISHED, "exception",
         ("exception",)),
    _row("process_call", TRIGGER_ARRIVING_DOCUMENT, RESULT_ENDS, CONTINUATION_NONE, ESTABLISHED, "process_call",
         ("processcall",)),
    # A protected path's `continue` hands its documents to the next handler. It
    # lowers to that edge, not to a node of its own.
    _row("continue", TRIGGER_ARRIVING_DOCUMENT, RESULT_FORWARDS, CONTINUATION_ONE, ESTABLISHED, None, ()),
    _row("sequence", TRIGGER_CONTAINER, RESULT_CONTAINER, CONTINUATION_CONTAINER, ESTABLISHED, None, ()),
)

DOCUMENT_EMISSION_V1: Mapping[str, DocumentEmissionV1] = MappingProxyType({row.kind: row for row in _ROWS})


def _kinds(predicate) -> FrozenSet[str]:
    return frozenset(row.kind for row in _ROWS if predicate(row))


#: Authored kinds that always emit zero documents: nothing authored may follow
#: them on their path, so their only legal placement is a terminal slot.
ZERO_EMISSION_KINDS: FrozenSet[str] = _kinds(lambda row: row.result == RESULT_CONSUMES)

#: Authored kinds that replace the payload when a document triggers them. They
#: never restart a path that has run out of documents.
TRIGGERED_REPLACEMENT_KINDS: FrozenSet[str] = _kinds(lambda row: row.result == RESULT_REPLACES)

#: Emitter keys, which are also the platform shape types, whose shape has no
#: outgoing wire because the step emits no documents.
ZERO_EMISSION_EMITTER_KINDS: FrozenSet[str] = frozenset(
    emitter for row in _ROWS if row.result == RESULT_CONSUMES for emitter in row.emitter_kinds
)

#: Semantic kinds of the same steps, for the compiler's walks.
ZERO_EMISSION_SEMANTIC_KINDS: FrozenSet[str] = frozenset(
    row.semantic_kind for row in _ROWS if row.result == RESULT_CONSUMES
)
TRIGGERED_REPLACEMENT_SEMANTIC_KINDS: FrozenSet[str] = frozenset(
    row.semantic_kind for row in _ROWS if row.result == RESULT_REPLACES
)

#: The platform step names used in served and diagnostic prose.
STEP_DISPLAY_NAMES: Mapping[str, str] = MappingProxyType(
    {
        "cache_put": "Add to Cache",
        "cache_remove": "Remove from Cache",
        "cache_get": "Retrieve from Cache",
        "document_cache_retrieve": "Retrieve from Cache",
    }
)


def emission_of(kind: str) -> Optional[DocumentEmissionV1]:
    """The row for an authored kind, or None for a kind the table does not know."""
    return DOCUMENT_EMISSION_V1.get(kind)


def emits_zero_documents(kind: str) -> bool:
    row = DOCUMENT_EMISSION_V1.get(kind)
    return row is not None and row.result == RESULT_CONSUMES


__all__ = [
    "CONTINUATION_CONTAINER",
    "CONTINUATION_CONTROL",
    "CONTINUATION_NONE",
    "CONTINUATION_ONE",
    "DOCUMENT_EMISSION_V1",
    "DocumentEmissionV1",
    "ESTABLISHED",
    "MEASURED",
    "OPEN",
    "RESULT_CONSUMES",
    "RESULT_CONTAINER",
    "RESULT_ENDS",
    "RESULT_FORWARDS",
    "RESULT_OPERATION_DEPENDENT",
    "RESULT_REPLACES",
    "RESULT_ROUTES",
    "RESULT_SUPPLIES",
    "STEP_DISPLAY_NAMES",
    "TRIGGERED_REPLACEMENT_KINDS",
    "TRIGGERED_REPLACEMENT_SEMANTIC_KINDS",
    "TRIGGER_ARRIVING_DOCUMENT",
    "TRIGGER_CONTAINER",
    "TRIGGER_PROCESS_START",
    "ZERO_EMISSION_EMITTER_KINDS",
    "ZERO_EMISSION_KINDS",
    "ZERO_EMISSION_SEMANTIC_KINDS",
    "emission_of",
    "emits_zero_documents",
]
