"""The compiler-revision behaviour corpus (#184 correction batch 21b, CDX-184-r20-02).

WHAT THIS IS. A packaged, deduplicated set of the INPUTS the covered tests hand to the
compiler's entry points, and the code that replays them. ``compiler_revision`` folds in
the replayed VERDICTS, so a behaviour change a covered test can observe through what an
entry point RETURNS moves the served revision — wherever in the compiler the change was
made, not only at a decision someone chose to sample. THE BOUND below says what a verdict
is and what it is not; ``AUTHORING_WORKFLOW_V1.md`` §4 states the same bound for callers.

WHY IT EXISTS. The defect class ``revision-oracle-omits-changed-behaviour`` recurred
after three structural fixes, each of which added hand-picked scenarios: a carry inside
the lineage walk's closures (the ride-on cache surviving a stream replacement) changed
what the server accepts, tests pinned the change, and the served revision stood still.
A hand-picked oracle can always miss the next carry. The inputs the tests exercise are
not picked for the revision; they are what the behaviour is tested on.

WHAT IT IS NOT. Not a source hash. Two packages whose compiler answers the same corpus
the same way serve the same revision (``AUTHORING_WORKFLOW_V1.md`` §4). The corpus holds
inputs only, never an expected verdict, so no verdict is packaged: each one is computed
by the code that serves it.

THE BOUND, in two parts. (1) Behaviour no corpus input exhibits is not covered here. That
is what the per-correction witness rule covers: a correction adds a test, the in-suite
guard (``tests/_revision_corpus.py``, registered by ``tests/conftest.py``) refuses a
covered test's input that is absent from the corpus, and the producer
(``tests/_revision_corpus.py --write``) puts it in. Which entry kinds the packaged corpus
actually carries is derived, never described: ``corpus_verdict_row()["records"]`` counts
them, and a kind with no covered caller is simply absent (``validate_lowered`` is, today).
Only the OUTERMOST entry-point call of a covered test is recorded; a call nested inside
another, and any call made while ``_compiler_revision_payload`` computes or this corpus
replays, is excluded — those are revision material already. (2) A verdict is everything the
entry point RETURNED, projected through the authority of each class it is made of
(:func:`declared_fields`: a model's fields and computed fields, a named tuple's fields, a
slotted object's slots, and a readable property the package declares) and digested, with a
container's TYPE part of the verdict — nothing is enumerated per kind, so a field added to a
report, a resolution, a walk or a compile result rides the day it exists. A compile
acceptance carries its canonical emission plan's SHA-256 in a slot of its own, so an
unreadable value elsewhere degrades that slot alone. A raise that is not a compile refusal
is recorded by TYPE only, so its message moves nothing here.

EXACT, NOT APPROXIMATE. An input is recorded as the object graph it IS — every model by
its class, its field values and its explicitly-set fields; a tuple as a tuple, a list as a
list; a mapping in its insertion order — and rebuilt with ``model_construct``, which runs
no validator. So a model a test mutated behind the parser replays as that mutated model,
not as a repaired one. The producer checks it: every recorded call's replayed verdict
must equal the verdict the test observed.
"""

from __future__ import annotations

import base64
import copy
import gzip
import hashlib
import importlib
import inspect
import io
import json
import math
from enum import Enum
from types import MappingProxyType
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Tuple

CORPUS_RESOURCE = "revision_corpus_v1.json.gz"
CORPUS_FORMAT = "1"
#: How the packaged corpus is rewritten. Declared here, where the corpus is read, and used by
#: the producer itself and by every refusal that asks for a regeneration, so one command is
#: named everywhere.
PRODUCER_COMMAND = "PYTHONPATH=src .venv/bin/python tests/_revision_corpus.py --write"

#: Entry kind -> ``(module under boomi_mcp, function name)``: the effect resolver that
#: derives every child-entry contract, the unified validator, the three compile entries,
#: and the lineage walk's two public views (tests read its state sets directly, and the
#: child contract is derived from them).
ENTRY_POINTS: Mapping[str, Tuple[str, str]] = MappingProxyType({
    "resolve": ("authoring.process_ir_effects", "resolve_process_ir_effect_declarations"),
    "validate": ("compiler.process_ir.semantic_validation.pipeline", "validate_process_ir"),
    "validate_lowered": (
        "compiler.process_ir.semantic_validation.pipeline", "validate_lowered_process_ir"),
    "compile": ("compiler.process_ir.pipeline", "compile_process_ir_v1"),
    "compile_model": ("compiler.process_ir.pipeline", "compile_process_ir_model_v1"),
    "parse_and_compile": ("compiler.process_ir.pipeline", "parse_and_compile_process_ir_v1"),
    "walk_lineage": ("compiler.process_ir.semantic_validation.lineage", "walk_lineage"),
    "collect_lineage_findings": (
        "compiler.process_ir.semantic_validation.lineage", "collect_lineage_findings"),
})

COMPILE_KINDS = frozenset({"compile", "compile_model", "parse_and_compile"})
WALK_KINDS = frozenset({"walk_lineage", "collect_lineage_findings"})
REPORT_KINDS = frozenset({"validate", "validate_lowered"})

#: Every tag is a single-key object. A plain mapping that could be read as one is stored
#: as ``$map`` instead, so a document is never mistaken for a tag.
_TAGS = frozenset({
    "$model", "$tuple", "$set", "$frozenset", "$map", "$proxy", "$enum", "$bytes",
    "$policy", "$script", "$root", "$calls", "$ref",
    # …and the projection's own tags: past the depth limit a verdict's mapping is encoded,
    # and a mapping that spells one of these would otherwise forge it
    # (CDX-184-r8-R4-PROJ-TAGSET-04).
    "$declared", "$items", "$unset",
})


class Unrecordable(Exception):
    """An input the corpus cannot represent exactly. Carries a value-free reason."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def canonical_json(value: Any) -> str:
    """The one serialization every digest here is taken over.

    Keys are NOT sorted: a mapping's insertion order is part of the input (a caller
    iterating it sees that order), and every record's own keys are written in one order.
    """
    return json.dumps(value, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("ascii")).hexdigest()


# ---------------------------------------------------------------------------
# encoding
# ---------------------------------------------------------------------------


def _class_path(cls: type) -> str:
    """``module:qualname`` in the bare package spelling.

    The suite imports the package both as ``boomi_mcp.*`` and ``src.boomi_mcp.*``; one
    class reached through either spelling is one class to the corpus.
    """
    module = cls.__module__
    if module.startswith("src.boomi_mcp."):
        module = module[len("src."):]
    if not module.startswith("boomi_mcp.") or "<locals>" in cls.__qualname__:
        raise Unrecordable("foreign-type")
    return module + ":" + cls.__qualname__


def _resolve_class(path: str) -> type:
    module, _, qualname = path.partition(":")
    if not module.startswith("boomi_mcp."):
        raise ValueError("foreign type in the revision corpus")
    target: Any = importlib.import_module(module)
    for part in qualname.split("."):
        target = getattr(target, part)
    return target


def encode_value(value: Any) -> Any:
    """The exact, tagged JSON form of ``value``; :func:`decode_value` rebuilds it.

    Types are dispatched EXACTLY and before anything is read, so a value of any other
    type — a one-shot iterable, a scalar subclass carrying its own dunders — is refused
    without being iterated or converted, and the call it belongs to runs untouched.
    """
    return _encode(value, 0)


def _encode(value: Any, depth: int) -> Any:
    if depth > 400:
        raise Unrecordable("too-deep")
    kind = type(value)
    if value is None or kind is str or kind is bool or kind is int:
        return value
    if kind is float:
        if not math.isfinite(value):
            raise Unrecordable("non-finite-float")
        return value
    if kind is list:
        return [_encode(item, depth + 1) for item in value]
    if kind is tuple:
        return {"$tuple": [_encode(item, depth + 1) for item in value]}
    if kind is frozenset or kind is set:
        items = sorted((_encode(item, depth + 1) for item in value), key=canonical_json)
        return {("$frozenset" if kind is frozenset else "$set"): items}
    if kind is dict:
        if all(type(key) is str for key in value) and not (
            len(value) == 1 and next(iter(value)) in _TAGS
        ):
            return {key: _encode(item, depth + 1) for key, item in value.items()}
        return {"$map": [[_encode(key, depth + 1), _encode(item, depth + 1)]
                         for key, item in value.items()]}
    if kind is MappingProxyType:
        return {"$proxy": _encode(dict(value), depth + 1)}
    if kind is bytes:
        return {"$bytes": base64.b64encode(value).decode("ascii")}
    if isinstance(value, Enum):
        return {"$enum": [_class_path(kind), _encode(value.value, depth + 1)]}
    from pydantic import BaseModel

    if isinstance(value, BaseModel):
        return _encode_model(value, kind, depth)
    # Two immutable, slotted server types, matched by NAME so the `src.boomi_mcp` copy of
    # each is the same type to the corpus. Their constructors normalize nothing an instance
    # does not already hold in normal form, so their state rebuilds them exactly.
    if _same_type(kind, _POLICY_PATH):
        return {"$policy": [value.adapter, sorted(value._exemptions)]}
    if _same_type(kind, _SCRIPT_PATH):
        return {"$script": [_encode(getattr(value, slot), depth + 1) for slot in kind.__slots__]}
    raise Unrecordable("type:" + kind.__name__)


_POLICY_PATH = "boomi_mcp.compiler.process_ir.semantic_validation.validation_policy:LegacyValidationPolicyV1"
_SCRIPT_PATH = "boomi_mcp.authoring.vetted_scripts:VettedScriptContractV1"


def _same_type(kind: type, path: str) -> bool:
    try:
        return _class_path(kind) == path
    except Unrecordable:
        return False


def _policy_type() -> type:
    return _resolve_class(_POLICY_PATH)


def _script_type() -> type:
    return _resolve_class(_SCRIPT_PATH)


def _encode_model(value: Any, kind: type, depth: int) -> Any:
    path = _class_path(kind)
    fields = kind.model_fields
    state = value.__dict__
    if set(state) != set(fields):
        raise Unrecordable("model-state")
    if getattr(value, "__pydantic_extra__", None) or getattr(value, "__pydantic_private__", None):
        raise Unrecordable("model-hidden-state")
    fields_set = value.__pydantic_fields_set__
    if type(fields_set) is not set or any(type(name) is not str for name in fields_set):
        raise Unrecordable("model-fields-set")
    return {"$model": [
        path,
        {name: _encode(state[name], depth + 1) for name in fields},
        sorted(fields_set),
    ]}


def decode_value(data: Any) -> Any:
    """Rebuild what :func:`encode_value` recorded, running no validator."""
    kind = type(data)
    if kind is list:
        return [decode_value(item) for item in data]
    if kind is not dict:
        return data
    if len(data) == 1:
        ((tag, body),) = data.items()
        if tag == "$model":
            path, fields, fields_set = body
            return _resolve_class(path).model_construct(
                _fields_set=set(fields_set),
                **{name: decode_value(item) for name, item in fields.items()},
            )
        if tag == "$tuple":
            return tuple(decode_value(item) for item in body)
        if tag == "$frozenset":
            return frozenset(decode_value(item) for item in body)
        if tag == "$set":
            return {decode_value(item) for item in body}
        if tag == "$map":
            return {decode_value(key): decode_value(item) for key, item in body}
        if tag == "$proxy":
            return MappingProxyType(decode_value(body))
        if tag == "$bytes":
            return base64.b64decode(body.encode("ascii"))
        if tag == "$enum":
            path, item = body
            return _resolve_class(path)(decode_value(item))
        if tag == "$policy":
            adapter, exemptions = body
            return _policy_type()(adapter, tuple(exemptions))
        if tag == "$script":
            language, source, *rest = (decode_value(item) for item in body)
            return _script_type()(language, source, **dict(zip(_script_type().__slots__[2:], rest)))
    return {key: decode_value(item) for key, item in data.items()}


# ---------------------------------------------------------------------------
# calls
# ---------------------------------------------------------------------------


def bound_arguments(function: Callable, args: tuple, kwargs: dict) -> Dict[str, Any]:
    """Every parameter of one call by name, defaults applied, however it was spelled."""
    bound = inspect.signature(function).bind(*args, **kwargs)
    bound.apply_defaults()
    return dict(bound.arguments)


def _prepared_from_graph(ir: Any, cfg: Any, symbols: Any) -> Any:
    """A prepared context over an ALREADY-built graph, indexed as the validator indexes one."""
    from ..compiler.process_ir.semantic_validation.context import (
        PreparedProcessValidationV1,
        _edge_index,
    )

    return PreparedProcessValidationV1(
        ir=ir,
        cfg=cfg,
        symbols=symbols,
        node_by_id={node.node_id: node for node in cfg.nodes},
        outgoing=_edge_index(cfg.edges, "source_node_id"),
        incoming=_edge_index(cfg.edges, "target_node_id"),
        symbol_by_ref={symbol.ref: symbol for symbol in symbols.symbols},
    )


def _same_graph(candidate: Any, prepared: Any) -> bool:
    """Whether ``candidate`` is the prepared context ``prepared`` IS, encoding and all.

    CDX-184-r5-CORPUS-03: pydantic ``==`` compares field VALUES, and
    ``prepare_validation_context`` re-validates a dump, which marks every field as
    explicitly set. One packaged record was stored as "derived" although the rebuilt IR
    carried a fuller ``__pydantic_fields_set__`` than the one the test handed over — so the
    record was not the object graph it claimed to be. The encoding IS the exactness
    criterion this module states, so it is what decides.
    """
    try:
        return encode_value(candidate) == encode_value(prepared)
    except Exception:  # noqa: BLE001 - a context that cannot be encoded is not a match
        return False


def encode_symbols_for(calls: Iterable[Tuple[Any, Any]]) -> Dict[str, Any]:
    """Every ``(key, table)`` a resolver's ``symbols_for`` callable answered, in call order.

    A callable is not data, but the answers it gave are, and replay hands them back. One
    key answered two ways is not one function of its key, and is refused.
    """
    answers: Dict[str, Any] = {}
    for key, table in calls:
        if type(key) is not str:
            raise Unrecordable("symbols-for-key")
        encoded = encode_value(table)
        if key in answers and answers[key] != encoded:
            raise Unrecordable("symbols-for-inconsistent")
        answers[key] = encoded
    return {"$calls": [[key, table] for key, table in answers.items()]}


def encode_call(kind: str, function: Callable, args: tuple, kwargs: dict) -> Dict[str, Any]:
    """The exact record of one entry-point call's inputs. Raises :class:`Unrecordable`.

    Read BEFORE the call. A resolver's ``symbols_for`` answers exist only after it: the
    record carries ``{"$calls": None}`` in their place until :func:`encode_symbols_for`
    fills it.
    """
    arguments = bound_arguments(function, args, kwargs)
    if kind == "resolve":
        roots = arguments["process_roots"]
        if type(roots) not in (list, tuple) or any(
            type(pair) is not tuple or len(pair) != 2 or type(pair[0]) is not str for pair in roots
        ):
            raise Unrecordable("roots-shape")
        child_roots = arguments["child_roots"]
        encoded_children = None
        if child_roots is not None:
            if type(child_roots) is not dict or any(type(ref) is not str for ref in child_roots):
                raise Unrecordable("child-roots-shape")
            # A child that IS one of the roots is recorded as that root, so replay hands
            # the resolver the same object twice, exactly as the caller did.
            encoded_children = [
                [ref, next(({"$root": index} for index, pair in enumerate(roots) if pair[1] is child),
                           None) or encode_value(child)]
                for ref, child in child_roots.items()
            ]
        return {
            "entry": kind,
            "process_roots": [
                "list" if type(roots) is list else "tuple",
                [[key, encode_value(ir)] for key, ir in roots],
            ],
            "declarations": encode_value(arguments["declarations"]),
            "symbols": encode_value(arguments["symbols"]),
            "components": encode_value(arguments["components"]),
            "child_roots": encoded_children,
            "script_registry": encode_value(arguments["script_registry"]),
            "conflict_policy": encode_value(arguments["conflict_policy"]),
            "literal_indexes": encode_value(arguments["literal_indexes"]),
            "symbols_for": None if arguments["symbols_for"] is None else {"$calls": None},
        }
    if kind == "validate":
        return {
            "entry": kind,
            "ir": encode_value(arguments["ir"]),
            "symbols": encode_value(arguments["symbol_table"]),
            "capabilities": encode_value(arguments["capabilities"]),
        }
    if kind == "validate_lowered":
        return {
            "entry": kind,
            "ir": encode_value(arguments["ir"]),
            "cfg": encode_value(arguments["cfg"]),
            "symbols": encode_value(arguments["symbol_table"]),
            "capabilities": encode_value(arguments["capabilities"]),
        }
    if kind in WALK_KINDS:
        # A prepared context is DERIVED — from its document and symbols, or, for a graph
        # assembled behind the parser, from the graph itself. It is recorded as whichever
        # inputs rebuild exactly it, and refused when neither does.
        from ..compiler.process_ir.semantic_validation.context import prepare_validation_context

        prepared = arguments["prepared"]
        record = {
            "entry": kind,
            "ir": encode_value(prepared.ir),
            "symbols": encode_value(prepared.symbols),
            "capabilities": encode_value(arguments["capabilities"]),
        }
        try:
            derived = _same_graph(prepare_validation_context(prepared.ir, prepared.symbols), prepared)
        except Exception:  # noqa: BLE001 - a document that does not lower is not its own graph
            derived = False
        if not derived:
            if not _same_graph(_prepared_from_graph(prepared.ir, prepared.cfg, prepared.symbols), prepared):
                raise Unrecordable("prepared-context-not-derived")
            record["cfg"] = encode_value(prepared.cfg)
        return record
    if kind in COMPILE_KINDS:
        return {
            "entry": kind,
            "ir": encode_value(arguments["payload" if kind == "parse_and_compile" else "ir"]),
            "symbols": encode_value(arguments["symbols"]),
            "validation_policy": encode_value(arguments["validation_policy"]),
            "capabilities": encode_value(arguments["capabilities"]),
        }
    raise ValueError(kind)


# ---------------------------------------------------------------------------
# verdicts
# ---------------------------------------------------------------------------


def _entry(kind: str) -> Callable:
    module, name = ENTRY_POINTS[kind]
    return getattr(importlib.import_module("boomi_mcp." + module), name)


#: ``{class: its own attribute names, sorted}``. Never enumerated here: what an object
#: carries is read from the class that declares it — pydantic's ``model_fields`` and its
#: COMPUTED fields, the ``__slots__`` chain, a named tuple's ``_fields`` — so a field added
#: to any verdict class is projected the day it exists, and one removed disappears by name.
#:
#: The defect class this closes took three rounds to leave: CDX-184-r5-CORPUS-01 widened the
#: projection from ``(code, path)`` to a hand-list; CDX-184-r6-R2-PROJ-01 derived a FINDING's
#: fields from its class but left the VERDICT CONTAINERS hand-listed, and
#: CDX-184-r7-PROJ3-CONTAINER-01 measured the consequence: inverting the served
#: ``ValidationReportV1.is_valid`` — a computed field two covered tests assert — left every
#: served revision byte-identical. Nothing is enumerated now: a verdict is what the entry
#: point RETURNED, projected by the authority of each class it is made of.
_DECLARED_FIELDS: Dict[type, Tuple[str, ...]] = {}

#: What a projected row carries where a class declares a field it never assigned. A TAG, not
#: a string: a real value can never forge it, because every mapping a verdict holds is
#: projected under ``$items`` and every string as itself, so "unassigned", ``None`` and the
#: string ``"$unset"`` are three different verdicts (CDX-184-r7-UNSET3-COLLIDE-01). Emitted
#: as a fresh ``dict``; this one is read-only.
UNSET: Mapping[str, bool] = MappingProxyType({"$unset": True})

#: The missing-attribute marker, private and unforgeable: no verdict can hold this object.
_ABSENT = object()

#: How deep the projection walks before handing the rest to :func:`encode_value`, which has
#: its own limit. Verdict graphs are shallow; this only bounds a pathological one.
_PROJECTION_DEPTH = 60


def declared_fields(kind: type) -> Tuple[str, ...]:
    """Every attribute ``kind`` declares, from its own authority, sorted and cached.

    A pydantic model's ``model_fields`` AND its ``model_computed_fields`` (pydantic serves
    both; ``model_fields`` alone misses ``is_valid``), a named tuple's ``_fields``, and every
    ``__slots__`` along the MRO.
    """
    if kind not in _DECLARED_FIELDS:
        names = set(getattr(kind, "model_fields", None) or ())
        names.update(getattr(kind, "model_computed_fields", None) or ())
        names.update(getattr(kind, "_fields", None) or ())
        for klass in getattr(kind, "__mro__", ()):
            slots = getattr(klass, "__slots__", ()) or ()
            names.update((slots,) if isinstance(slots, str) else slots)
            for name, member in vars(klass).items():
                # A plain ``@property`` is served like any other member — `EffectResolutionV1.ok`
                # is one, asserted on 25 lines of a covered test, and inverting it moved no
                # corpus row (CDX-184-r8-R4-PROJ-PLAINPROPERTY-03). Only properties whose
                # accessor is DEFINED IN THIS PACKAGE are read: the projection calls them, and
                # a foreign descriptor is not ours to run. ``cached_property`` is included by
                # the same rule; the values it caches are the values the caller sees.
                if isinstance(member, property) and member.fget is not None:
                    accessor = member.fget
                elif type(member).__name__ == "cached_property" and getattr(member, "func", None):
                    accessor = member.func
                else:
                    continue
                if _in_package(getattr(accessor, "__module__", "") or ""):
                    names.add(name)
        _DECLARED_FIELDS[kind] = tuple(sorted(name for name in names if not name.startswith("__")))
    return _DECLARED_FIELDS[kind]


def _projected(value: Any, depth: int = 0) -> Any:
    """What a caller can see of ``value``, read from the authority that declares it.

    A model, a named tuple or a slotted object by every field its class declares, each field
    projected the same way and the model tagged with its class (two classes with the same
    field values are not the same verdict); a container item by item; anything else exactly,
    through :func:`encode_value`, which refuses a type it cannot read rather than collapsing
    it. ``__pydantic_fields_set__`` rides with a model, so a field that stops being set is a
    changed verdict too.
    """
    kind = type(value)
    if depth < _PROJECTION_DEPTH and not (value is None or kind in (str, bool, int, float)):
        fields = declared_fields(kind)
        if fields:
            projected: List[Any] = []
            for name in fields:
                member = _read(value, name)
                projected.append([name, dict(UNSET) if member is _ABSENT
                                  else _projected(member, depth + 1)])
            fields_set = getattr(value, "__pydantic_fields_set__", None)
            if fields_set is not None:
                projected.append(["$set", sorted(fields_set)])
            return {"$declared": [_class_path(kind), projected]}
        # The container TYPE rides, exactly as the encoder records it: dropping the
        # `tuple(...)` normalisation in `EffectResolutionV1.__init__` — which three covered
        # tests kill — left every revision byte-identical while tuple and list projected the
        # same (CDX-184-r8-R4-PROJ-CONTAINERTYPE-01).
        if kind is list:
            return [_projected(item, depth + 1) for item in value]
        if kind is tuple:
            return {"$tuple": [_projected(item, depth + 1) for item in value]}
        if kind is set or kind is frozenset:
            return {"$" + kind.__name__: sorted(
                (_projected(item, depth + 1) for item in value), key=canonical_json)}
        if kind is dict:
            return {"$items": _projected_items(value, depth)}
        if kind is MappingProxyType:
            return {"$proxy": _projected_items(value, depth)}
    return encode_value(value)


def _exception_path(kind: type) -> str:
    """``module:qualname``: two exception classes that share a bare name are two verdicts
    (CDX-184-r8-R4-PROJ-RAISENAME-05). The message is still never recorded."""
    return "{0}:{1}".format(getattr(kind, "__module__", "?"), getattr(kind, "__qualname__", "?"))


def _read(value: Any, name: str) -> Any:
    """One declared attribute, or :data:`_ABSENT` when the object simply does not have it.

    An accessor that RAISES is not an absent field: `getattr`'s default swallows only
    AttributeError, so a package property whose body raised one projected exactly as a
    declared-but-never-assigned field and two different states read as one verdict
    (CDX-184-r9-R8V5-ATTRERR-PROPERTY-04). A name the class declares through a descriptor is
    read WITHOUT a default, so whatever it raises reaches :func:`_slot` and degrades loudly —
    the way every other unreadable value does.
    """
    if isinstance(getattr(type(value), name, None), property) or type(
            getattr(type(value), name, None)).__name__ == "cached_property":
        return getattr(value, name)
    return getattr(value, name, _ABSENT)


def _projected_items(mapping: Any, depth: int) -> List[Any]:
    return [[_projected(key, depth + 1), _projected(item, depth + 1)]
            for key, item in mapping.items()]


def _slot(project: Callable[[], Any]) -> Any:
    """One slot of a verdict: its digest, or a token naming why it could not be read.

    Degrades PER SLOT and per record: one unencodable field on a returned graph used to
    collapse every acceptance in the corpus into a single constant token, taking the
    emission-plan hash with it (CDX-184-r7-ACC3-COUPLING-01). Loud, never silent:
    `test_every_packaged_input_replays_to_a_verdict` refuses a corpus that produces one.
    """
    try:
        return digest(_projected(project()))
    except Exception as exc:  # noqa: BLE001 - the reason IS the verdict for this slot
        return {"unencodable": type(exc).__name__ + (
            ":" + exc.reason if isinstance(exc, Unrecordable) else "")}


def project_outcome(kind: str, result: Any = None, exc: Optional[BaseException] = None) -> Any:
    """The verdict of one call: everything the entry point RETURNED, digested.

    Nothing is enumerated per kind: the returned object is projected by the authority of each
    class it is made of (:func:`_projected`) and digested, so a field added to a report, a
    resolution, a walk or a compile result rides the day it exists. A compile acceptance
    additionally carries the SHA-256 of its canonical emission plan — the served bytes a
    compile hash and a materialization fingerprint are taken over — in its own slot, so one
    unreadable field elsewhere in the returned tuple cannot cost the plan its discrimination.
    A raise other than a compile refusal is recorded by TYPE only: its message can carry an
    address. A refusal carries its diagnostics, which are value-free by contract.
    """
    from ..compiler.process_ir.diagnostics import ProcessIRCompileError

    if exc is not None:
        if isinstance(exc, ProcessIRCompileError):
            refused = _slot(lambda: exc.diagnostics)
            if kind in COMPILE_KINDS:
                return {"refused": refused}
            return {"raised": _exception_path(type(exc)), "refused": refused}
        return {"raised": _exception_path(type(exc))}
    if kind in COMPILE_KINDS:
        from ..compiler.process_ir.contracts import canonical_emission_plan_json

        plan = canonical_emission_plan_json(result[-1]).encode("utf-8")
        return {"compiled": hashlib.sha256(plan).hexdigest(),
                "returned": _slot(lambda: tuple(result[:-1]))}
    if kind in ENTRY_POINTS:
        return {"returned": _slot(lambda: result)}
    raise ValueError(kind)


def replay(record: Mapping[str, Any]) -> Any:
    """The verdict one record receives from the code serving it. Never raises."""
    kind = record["entry"]
    try:
        function, args = _rebuild(record)
    except Exception as exc:  # noqa: BLE001 - an input that cannot be rebuilt is a verdict
        return {"unreplayable": type(exc).__name__}
    try:
        result = function(*args)
    except Exception as exc:  # noqa: BLE001 - a raise IS the verdict
        return project_outcome(kind, exc=exc)
    try:
        return project_outcome(kind, result)
    except Exception as exc:  # noqa: BLE001
        return {"unprojectable": type(exc).__name__}


def _rebuild(record: Mapping[str, Any]) -> Tuple[Callable, tuple]:
    """The entry point and the positional arguments of the recorded call."""
    kind = record["entry"]
    function = _entry(kind)
    if kind == "validate":
        return function, (decode_value(record["ir"]), decode_value(record["symbols"]),
                          decode_value(record["capabilities"]))
    if kind == "validate_lowered":
        return function, (decode_value(record["ir"]), decode_value(record["cfg"]),
                          decode_value(record["symbols"]), decode_value(record["capabilities"]))
    if kind in COMPILE_KINDS:
        return _keywords(function, (
            decode_value(record["ir"]), decode_value(record["symbols"])), {
            "validation_policy": decode_value(record["validation_policy"]),
            "capabilities": decode_value(record["capabilities"]),
        })
    if kind in WALK_KINDS:
        from ..compiler.process_ir.semantic_validation.context import prepare_validation_context

        ir, symbols = decode_value(record["ir"]), decode_value(record["symbols"])
        prepared = (_prepared_from_graph(ir, decode_value(record["cfg"]), symbols)
                    if "cfg" in record else prepare_validation_context(ir, symbols))
        return function, (prepared, decode_value(record["capabilities"]))
    if kind == "resolve":
        container, pairs = record["process_roots"]
        roots_list = [(key, decode_value(ir)) for key, ir in pairs]
        roots = roots_list if container == "list" else tuple(roots_list)
        child_roots = None
        if record["child_roots"] is not None:
            child_roots = {
                ref: (roots_list[child["$root"]][1]
                      if type(child) is dict and set(child) == {"$root"} else decode_value(child))
                for ref, child in record["child_roots"]
            }
        symbols_for = None
        if record["symbols_for"] is not None:
            answers = {key: decode_value(table) for key, table in record["symbols_for"]["$calls"]}

            def symbols_for(key, _root):
                return answers[key]

        return function, (
            roots,
            decode_value(record["declarations"]),
            decode_value(record["symbols"]),
            decode_value(record["components"]),
            child_roots,
            decode_value(record["script_registry"]),
            decode_value(record["conflict_policy"]),
            decode_value(record["literal_indexes"]),
            symbols_for,
        )
    raise ValueError(kind)


def _keywords(function: Callable, args: tuple, keywords: Dict[str, Any]) -> Tuple[Callable, tuple]:
    def call(*positional):
        return function(*positional, **keywords)

    return call, args


# ---------------------------------------------------------------------------
# the packaged corpus
# ---------------------------------------------------------------------------

#: An encoded model at least this long is stored once and referenced.
_INTERN_MIN = 256


def _intern(value: Any, objects: List[Any], index: Dict[str, int]) -> Any:
    """Post-order, so an object's children are interned before it and every reference
    points backwards."""
    kind = type(value)
    if kind is list:
        return [_intern(item, objects, index) for item in value]
    if kind is not dict:
        return value
    inner = {key: _intern(item, objects, index) for key, item in value.items()}
    if len(inner) == 1 and "$model" in inner:
        text = canonical_json(inner)
        if len(text) >= _INTERN_MIN:
            if text not in index:
                index[text] = len(objects)
                objects.append(inner)
            return {"$ref": index[text]}
    return inner


def _expand(value: Any, objects: List[Any], expanded: Dict[int, Any]) -> Any:
    kind = type(value)
    if kind is list:
        return [_expand(item, objects, expanded) for item in value]
    if kind is not dict:
        return value
    if len(value) == 1 and "$ref" in value:
        ref = value["$ref"]
        if ref not in expanded:
            expanded[ref] = _expand(objects[ref], objects, expanded)
        return expanded[ref]
    return {key: _expand(item, objects, expanded) for key, item in value.items()}


def serialize_corpus(records: Iterable[Mapping[str, Any]]) -> bytes:
    """Deterministic gzip bytes for a record set.

    Records are deduplicated and ordered by digest, shared models are stored once in
    first-use order, and the gzip header carries no file name and no time.
    """
    unique = {digest(record): record for record in records}
    objects: List[Any] = []
    index: Dict[str, int] = {}
    stored = [_intern(unique[key], objects, index) for key in sorted(unique)]
    body = canonical_json({"format": CORPUS_FORMAT, "objects": objects, "records": stored})
    buffer = io.BytesIO()
    with gzip.GzipFile(filename="", mode="wb", fileobj=buffer, compresslevel=9, mtime=0) as handle:
        handle.write(body.encode("ascii"))
    return buffer.getvalue()


def parse_corpus(raw: bytes) -> List[Dict[str, Any]]:
    data = json.loads(gzip.decompress(raw).decode("ascii"))
    if data.get("format") != CORPUS_FORMAT:
        raise ValueError("unsupported revision corpus format")
    objects = data["objects"]
    expanded: Dict[int, Any] = {}
    return [_expand(record, objects, expanded) for record in data["records"]]


def load_corpus() -> List[Dict[str, Any]]:
    """The packaged corpus, read the way the package reads its other data files."""
    from importlib import resources

    return parse_corpus(resources.files(__package__).joinpath(CORPUS_RESOURCE).read_bytes())


class CorpusNotPackaged(RuntimeError):
    """The packaged corpus did not ship, or did not parse. Raised only by
    :func:`assert_packaged`, never on the served path."""


def assert_packaged() -> int:
    """The number of packaged inputs, or :class:`CorpusNotPackaged`.

    A BUILD-time gate (the Dockerfile runs it beside the tool-import gate), because losing
    this asset is silent at runtime: ``compiler_revision`` keeps being served, the row
    degrades to the same "unavailable" every decorative row uses, and the digest then stands
    still whatever the compiler does — the exact blindness this corpus exists to remove
    (CDX-184-r5-DOC-21B-06).
    """
    try:
        records = load_corpus()
    except Exception as exc:  # noqa: BLE001 - the build must read the reason
        raise CorpusNotPackaged(
            "the compiler-revision behaviour corpus {0}/{1} is missing or unreadable ({2}: {3}); "
            "compiler_revision would be served without it. Regenerate it with {4}".format(
                __package__, CORPUS_RESOURCE, type(exc).__name__, exc, PRODUCER_COMMAND)) from exc
    if not records:
        raise CorpusNotPackaged("the packaged behaviour corpus holds no inputs")
    return len(records)


def replayed_verdicts(records: Iterable[Mapping[str, Any]]) -> List[List[Any]]:
    """``[input digest, verdict]`` for every record, in digest order."""
    return sorted(([digest(record), replay(record)] for record in records), key=lambda row: row[0])


#: ``{sha256 of the packaged bytes: (records, their digests)}``: the parsed DATA, which
#: only the packaged bytes decide. The bytes are re-read on every call.
_PARSED: Dict[str, Tuple[List[Dict[str, Any]], List[str]]] = {}


def _packaged() -> Tuple[str, List[Dict[str, Any]], List[str]]:
    from importlib import resources

    raw = resources.files(__package__).joinpath(CORPUS_RESOURCE).read_bytes()
    key = hashlib.sha256(raw).hexdigest()
    if key not in _PARSED:
        records = parse_corpus(raw)
        _PARSED.clear()
        _PARSED[key] = (records, [digest(record) for record in records])
    return (key,) + _PARSED[key]


# ---------------------------------------------------------------------------
# the row, memoized by the identity of the live code it replays
# ---------------------------------------------------------------------------
#
# Replaying the corpus costs about a second, and a process asks for the row many times: the
# manifest once, but every revision comparison and every test that recomputes the payload
# again. The row is a pure function of the packaged bytes and of the code the replay runs,
# so it is memoized on exactly those two things — never on a flag someone must remember to
# clear.
#
# "The code the replay runs" is taken as the IDENTITY of every live object in the package a
# replay can reach through a name: in every loaded ``boomi_mcp`` module (both spellings),
# each module attribute; for a function, its code object, defaults, keyword defaults and
# closure cells; for a class defined in the package, every attribute in its own namespace;
# for a mutable module-level container, its keys and items; for any other object with an
# instance namespace, its attributes. A monkeypatch of any of those, an in-place ``setitem``
# on a table, a replaced method, a swapped ``__code__`` — each changes an identity and misses.
# In production nothing is patched, so the first computation is the only one.
#
# A module-level container is walked whether or not it can be mutated: a tuple's or
# frozenset's members are held exactly like a list's, because a lambda inside a tuple of
# rules is code the replay runs (`pipeline._SCALAR_SLOTS`), and holding it costs about a
# millisecond of the key (CDX-184-r5-STALE-02, measured below).
#
# Outside the package the key holds the callable surface of every MODULE the replay was
# measured to call into (:data:`_NON_PACKAGE_MODULES`): each module attribute, and for a
# class defined there, its own namespace. Rebinding one is how a test stubs a boundary, and
# a stale row would then be served (CDX-184-r5-STALE-01, measured on
# `pydantic.BaseModel.model_dump` and `json.dumps`). The module SET is the measurement and
# the guard re-runs it; naming callables instead left `JSONEncoder.iterencode`,
# `BaseModel.__init__` and `BaseModel.__eq__` serving stale rows (CDX-184-r6-MEMO-R2-01),
# which is the same hand-enumeration defect one layer down.
#
# Two kinds of binding are left out, each with its evidence below: those the replay does not
# read through that name (measured with a raising sentinel, re-measured by a test on every
# run), and the parsed corpus, whose content the key already holds as the bytes' digest.
#
# THE BOUND, stated: a mutation BELOW that depth is not seen — an item of a container held
# inside a module-level container, an attribute of an object held in a container, and, in a
# listed NON-package module, any container's items (`copy._copy_dispatch[dict]` is the one
# live instance measured to move a replayed row while the memo still hits,
# CDX-184-r7-R3-NONPKGCONTAINER-05; walking those containers was measured and rejected: they
# hold 7,004 items across 105 containers, and `re._cache` grows from 12 to 135 entries DURING
# one replay and is cleared when it fills, so the key would differ from itself between two
# calls in the same state and the memo would stop hitting). Nor is a module outside the package that the replay was
# never measured to call into. The packaged bytes, the one
# non-package input the replay reads through a patchable module (`importlib.resources`),
# are re-read and hashed on every call instead.

_PACKAGE_PREFIXES = ("boomi_mcp.", "src.boomi_mcp.")
_CONTAINERS = (dict, list, set, frozenset, tuple, MappingProxyType)
_MAPPINGS = (dict, MappingProxyType)

#: Every non-package module the replay was MEASURED to call into — derived by profiling one
#: full replay of the packaged corpus and taking the defining module of each call, not
#: chosen. `tests/test_issue_184_revision_corpus.py` RE-RUNS that profile on every run and
#: fails naming any module this list lacks, so the measurement is the authority rather than
#: a sentence about a measurement. The union over the interpreters the server runs on: 3.11
#: reaches `io` where 3.12 reaches `_io`, and a module that is not imported contributes
#: nothing, so listing both costs nothing and keeps one list true on both. Bound: a module
#: here is held by its callable surface
#: (module attributes, and the namespace of each class defined in it), never by walking its
#: containers — `sys.modules`, `typing`'s caches and the like are held by identity only.
_NON_PACKAGE_MODULES = (
    "_abc",
    "_hashlib",
    "_io",
    "abc",
    "builtins",
    "collections",
    "contextlib",
    "copy",
    "enum",
    "functools",
    "hashlib",
    "importlib",
    "importlib._bootstrap",
    "importlib._bootstrap_external",
    "importlib.resources._adapters",
    "importlib.resources._common",
    "importlib.resources.readers",
    "io",
    "json",
    "json.encoder",
    "pathlib",
    "posix",
    "posixpath",
    "pydantic._internal._model_construction",
    "pydantic._internal._repr",
    "pydantic._internal._utils",
    "pydantic.main",
    "pydantic_core._pydantic_core",
    "re",
    "sys",
    "typing",
    "weakref",
)


def _non_package_identity() -> List[Any]:
    """The callable surface of :data:`_NON_PACKAGE_MODULES`, in a deterministic order.

    Read out of each owner's ``__dict__``: a classmethod or a slot wrapper fetched with
    ``getattr`` is a new object on every access, which would make the key differ from
    itself. A module that is not imported contributes nothing (it cannot have been read).
    """
    import sys

    held: List[Any] = []
    for module_name in _NON_PACKAGE_MODULES:
        module = sys.modules.get(module_name)
        namespace = getattr(module, "__dict__", None)
        if type(namespace) is not dict:
            continue
        for attribute, member in sorted(namespace.items()):
            held.append(member)
            if isinstance(member, type) and getattr(member, "__module__", None) == module_name:
                for name, value in sorted(vars(member).items(), key=lambda pair: pair[0]):
                    held.append(getattr(value, "__func__", value))
    return held

#: Bindings the replay does not read THROUGH THESE NAMES, so rebinding one cannot change a
#: verdict and the key leaves it out. Each was MEASURED, not assumed: rebound (in both module
#: spellings) to a sentinel that raises a ``BaseException`` on any use — which no
#: ``except Exception`` on the replay path can absorb — the whole corpus replayed to the
#: identical row. `tests/test_issue_184_revision_corpus.py` re-measures all of them on every
#: run, so a corpus input that starts reading one fails there by name. They are the manifest's
#: own registries and caches, network boundaries tests stub, and authorities the replay reaches
#: only through another module's binding (which stays in the key, with its code and items).
_UNREAD_BY_THE_REPLAY = frozenset({
    ("boomi_mcp.authoring.connector_resolution_snapshot", "rest_route_decision"),
    ("boomi_mcp.authoring.contract", "AUTHORING_ACTIONS"),
    ("boomi_mcp.authoring.contract", "AUTHORING_CAPABILITY_REGISTRY"),
    ("boomi_mcp.authoring.contract", "AUTHORING_SCHEMA_REGISTRY"),
    ("boomi_mcp.authoring.contract", "AUTHORING_SUPPORT_MATRIX"),
    ("boomi_mcp.authoring.contract", "_MANIFEST_CACHE"),
    ("boomi_mcp.authoring.contract", "_REASON_CODES"),
    ("boomi_mcp.authoring.contract", "_inherited_schema_digest"),
    ("boomi_mcp.authoring.contract", "_replay_ids"),
    ("boomi_mcp.authoring.contract", "_replay_registry"),
    ("boomi_mcp.authoring.contract", "_schema_bundle"),
    ("boomi_mcp.authoring.contract", "list_archetype_registry"),
    ("boomi_mcp.authoring.process_ir_projection", "_CACHE"),
    ("boomi_mcp.authoring", "process_ir_projection"),
    ("boomi_mcp.categories.components._shared", "component_get_xml"),
    ("boomi_mcp.categories.components.connection_reuse", "component_get_xml"),
    ("boomi_mcp.categories.components.connectors", "component_get_xml"),
    ("boomi_mcp.categories.components.process_component_materializer", "_PROFILE_OPTIONS"),
    ("boomi_mcp.categories.components.query_components", "component_get_xml"),
    ("boomi_mcp.categories.integration_builder", "_execute_component"),
    ("boomi_mcp.categories.integration_builder", "component_get_xml"),
    ("boomi_mcp.categories.integration_builder", "component_write_conflicts"),
    ("boomi_mcp.categories.integration_builder", "component_writes_existing"),
    ("boomi_mcp.categories.integration_builder", "create_component"),
    ("boomi_mcp.categories.integration_builder", "declared_bindings_for_components"),
    ("boomi_mcp.categories.integration_builder", "paginate_metadata"),
    ("boomi_mcp.categories.integration_builder", "planned_existing_ids"),
    ("boomi_mcp.categories.integration_builder", "reused_keys_for_components"),
    ("boomi_mcp.categories.integration_builder", "smart_merge_would_change"),
    ("boomi_mcp.compiler.process_ir.connector_resolution", "profile_bound_input_types"),
    ("boomi_mcp.compiler.process_ir.contracts", "LISTENER_CONNECTOR_TYPES"),
    ("boomi_mcp.compiler.process_ir.execution_profile", "derive_process_execution_profile"),
    ("boomi_mcp.compiler.process_ir.semantic_validation.lineage", "STATE_VISIBILITY_V1"),
    ("boomi_mcp.compiler.process_ir.semantic_validation", "validation_policy"),
    ("boomi_mcp.connector_replay.digests", "_UNRESERVED"),
    ("boomi_mcp.connector_replay.digests", "_normalize_percent_encoding"),
    ("boomi_mcp.connector_replay.digests", "comparable_path"),
    ("boomi_mcp.models.process_ir", "PROCESS_IR_V1_CAPABILITIES"),
    ("boomi_mcp.models.process_ir_document_semantics", "DOCUMENT_EMISSION_V1"),
    ("boomi_mcp.models", "PROCESS_IR_V1_CAPABILITIES"),
    ("boomi_mcp.recipes", "production_registry"),
})

#: Bindings the replay DOES read whose whole content is a function of something the key
#: already holds by content: the parsed corpus is a function of the packaged bytes, whose
#: SHA-256 leads every key, and the projection's field cache is CLEARED at the start of every
#: row (:func:`_computed_row`), so it can never answer for a state other than the one being
#: measured, and within a row it is a pure function of classes the key walks. (Both also GROW
#: while a row is computed, and a cache that grows is not a state change: without this the
#: store would refuse every first replay — CDX-184-r7-MEMO3-FIRSTREPLAY-01.)
_KEYED_BY_CONTENT = frozenset({
    ("boomi_mcp.authoring.revision_corpus", "_PARSED"),
    ("boomi_mcp.authoring.revision_corpus", "_DECLARED_FIELDS"),
})


def _in_package(name: str) -> bool:
    return name in ("boomi_mcp", "src.boomi_mcp") or name.startswith(_PACKAGE_PREFIXES)


def _live_identity(labels: Optional[List[str]] = None) -> List[Any]:
    """Every object the key is taken over, in a deterministic order. Holding this list keeps
    each of them alive, so no identity in the key can be reused by another object.

    ``labels``, when given, receives one human-readable label per held object, in step, so
    a changed identity can be named. Labels are built only then: the key is taken on every
    request for the row, and formatting them would be most of its cost.

    """
    import sys
    from types import FunctionType

    held: List[Any] = []
    expanded: set = set()
    naming = labels is not None
    append = held.append

    def hold(obj: Any, label: Any) -> None:
        append(obj)
        if naming:
            labels.append(label() if callable(label) else label)

    def function_parts(function: Any, label: Any) -> None:
        hold(function.__code__, lambda: _name(label) + ".__code__")
        # The default TABLES are walked, not just held: a keyword default set in place is a
        # behaviour change the identity of the table cannot see, and one live instance
        # (`lowering._transition.__kwdefaults__["provenance"]`, which the emission plan a
        # compile acceptance is digested over consumes) served a stale row
        # (CDX-184-r6-MEMO-R2-02) — the same defect the module-level container walk closes.
        value(function.__defaults__, (lambda: _name(label) + ".__defaults__") if naming else None)
        value(function.__kwdefaults__, (lambda: _name(label) + ".__kwdefaults__") if naming else None)
        for index, cell in enumerate(function.__closure__ or ()):
            try:
                contents = cell.cell_contents
            except ValueError:  # an empty cell
                contents = None
            cell_label = (lambda index=index: "{0}.<cell {1}>".format(_name(label), index)) if naming else None
            hold(contents, cell_label)
            if type(contents) is FunctionType and id(contents) not in expanded:
                expanded.add(id(contents))
                function_parts(contents, cell_label)

    def expand_function(member: Any, label: Any) -> None:
        if type(member) is FunctionType and id(member) not in expanded:
            expanded.add(id(member))
            function_parts(member, label)

    def value(item: Any, label: Any) -> None:
        hold(item, label)
        kind = type(item)
        if kind is FunctionType:
            expand_function(item, label)
            return
        if id(item) in expanded or kind in (str, int, float, bool, bytes, type(None)):
            return
        expanded.add(id(item))
        if kind in _CONTAINERS:
            pairs = item.items() if kind in _MAPPINGS else ((None, x) for x in item)
            for position, (key, member) in enumerate(list(pairs)):
                member_label = (lambda position=position: "{0}[{1}]".format(_name(label), position)) if naming else None
                hold(key, (lambda member_label=member_label: _name(member_label) + ".key") if naming else None)
                if type(member) in (tuple, frozenset):
                    # A rules table is usually a tuple of pairs, so the code it dispatches is
                    # one level further down: an immutable container member is WALKED, not
                    # just held (`pipeline._SCALAR_SLOTS[4][1]` is such a lambda).
                    value(member, member_label)
                    continue
                hold(member, member_label)
                expand_function(member, member_label)
            return
        if isinstance(item, type):
            if not _in_package(getattr(item, "__module__", "") or ""):
                return
            for attribute, member in list(vars(item).items()):
                member_label = (lambda attribute=attribute: "{0}.{1}".format(_name(label), attribute)) if naming else None
                hold(member, member_label)
                expand_function(getattr(member, "__func__", None) or member, member_label)
                if isinstance(member, property):
                    for accessor in (member.fget, member.fset, member.fdel):
                        hold(accessor, member_label)
                        expand_function(accessor, member_label)
                elif type(member) in _CONTAINERS:
                    value(member, member_label)
            return
        namespace = getattr(item, "__dict__", None)
        if type(namespace) is dict and not isinstance(item, type(sys)):
            for attribute, member in list(namespace.items()):
                member_label = (lambda attribute=attribute: "{0}.{1}".format(_name(label), attribute)) if naming else None
                hold(member, member_label)
                expand_function(member, member_label)

    for name, module in list(sys.modules.items()):
        if module is None or not _in_package(name):
            continue
        hold(name, name + " <name>")
        hold(module, name + " <module>")
        namespace = getattr(module, "__dict__", None)
        if type(namespace) is not dict:
            continue
        bare = name[len("src."):] if name.startswith("src.") else name
        for attribute, item in list(namespace.items()):
            if (bare, attribute) in _UNREAD_BY_THE_REPLAY or (bare, attribute) in _KEYED_BY_CONTENT:
                continue
            value(item, "{0}:{1}".format(name, attribute) if naming else None)
    for index, member in enumerate(_non_package_identity()):
        hold(member, (lambda index=index: "<non-package #{0}>".format(index)) if naming else None)
    return held


def _name(label: Any) -> str:
    return label() if callable(label) else str(label)


#: How many (key, row) entries the memo keeps. A perturbation test alternates between the
#: baseline and one perturbed state, so a few entries turn the baseline's recomputation after
#: every undo into a hit.
MEMO_ENTRIES = 4


class _RowMemo:
    """The last ``MEMO_ENTRIES`` keys, most recent last, each with the objects it was taken
    over (held, so none of its identities can be reused) and its row."""

    __slots__ = ("entries", "hits", "computations")

    def __init__(self) -> None:
        self.entries: List[Tuple[Tuple[Any, ...], List[Any], Dict[str, Any]]] = []
        self.hits = 0
        self.computations = 0

    def find(self, key: Tuple[Any, ...]) -> Optional[int]:
        for index, (stored, _held, _row) in enumerate(self.entries):
            if stored == key:
                return index
        return None


_MEMO = _RowMemo()


def _memo_key(corpus_key: str) -> Tuple[Tuple[Any, ...], List[Any]]:
    held = _live_identity()
    return (corpus_key,) + tuple(map(id, held)), held


def _computed_row(records: List[Dict[str, Any]], digests: List[str]) -> Dict[str, Any]:
    # The projection's field cache never outlives a row. It is derived from classes whose own
    # namespaces the key walks, but it is never invalidated, so a cache built in one state
    # would answer for another: widening a class in place after a replay left the row
    # byte-identical while the key had moved (CDX-184-r8-B21B-R4-FIELDCACHE-01). Cleared
    # here, a row always reads the authority as it is now, and the cache stays out of the key
    # because within one row it is a pure function of classes the key holds.
    _DECLARED_FIELDS.clear()
    kinds: Dict[str, int] = {}
    for record in records:
        kinds[record["entry"]] = kinds.get(record["entry"], 0) + 1
    verdicts = sorted(([key, replay(record)] for key, record in zip(digests, records)),
                      key=lambda row: row[0])
    return {
        "records": dict(sorted(kinds.items())),
        "verdicts": "sha256:" + digest(verdicts),
    }


#: Whether the replay path has been walked once in this process.
_SETTLED: List[bool] = []


def _settle(records: List[Dict[str, Any]]) -> None:
    """Walk everything a replay reaches BEFORE the key is taken, once per process.

    A replay imports the modules it needs and makes the standard library swap a couple of its
    own lazily specialised functions (measured on 3.12: ``typing._check_generic`` and
    ``typing._collect_parameters``). Both happen while a row is being computed, so the key
    that row started under is not the key it ends under. Round 7 admitted the difference when
    it was only new modules — and CDX-184-r8-B21B-R4-STORE-01 measured the hole that left: the
    20 in-package modules a replay imports were outside the walk that decided it, so a
    perturbation inside one of them (`map_builder.MAP_BUILDERS`, measured) produced a row no
    state ever made, stored and served. The window is closed instead of trusted: one record of
    every entry kind, every class path the corpus names, and the package subtrees the replay
    reaches are all imported here, so the key is settled and a row is stored only when nothing
    at all changed while it was computed.
    """
    if _SETTLED:
        return
    _SETTLED.append(True)
    import pkgutil

    seen = set()
    for record in records:
        if record["entry"] in seen:
            continue
        seen.add(record["entry"])
        replay(record)
    for record in records:
        for path in _class_paths(record):
            try:
                _resolve_class(path)
            except Exception:  # noqa: BLE001 - a path that will not resolve is a verdict, later
                pass
    for subtree in _SETTLE_SUBTREES:
        package = importlib.import_module(subtree)
        for module in pkgutil.walk_packages(package.__path__, subtree + "."):
            try:
                importlib.import_module(module.name)
            except Exception:  # noqa: BLE001 - a module that will not import is not replay material
                pass


#: Package subtrees a replay reaches through a registry rather than an import, measured by
#: profiling one replay: settling by entry kind and by class path still left these imported
#: mid-row (CDX-184-r8-B21B-R4-STORE-01).
_SETTLE_SUBTREES = ("boomi_mcp.categories", "boomi_mcp.recipes")


def _class_paths(value: Any) -> Iterable[str]:
    """Every class path a record names, so replaying it imports nothing new."""
    if isinstance(value, dict):
        for key, item in value.items():
            if key in ("$model", "$enum", "$policy", "$script") and isinstance(item, list) and item:
                if isinstance(item[0], str):
                    yield item[0]
            for path in _class_paths(item):
                yield path
    elif isinstance(value, list):
        for item in value:
            for path in _class_paths(item):
                yield path


def memo_would_hit() -> bool:
    """Whether :func:`corpus_verdict_row` would answer from its memo right now. Computes the
    key only — no replay — so a witness can ask it under every perturbation."""
    corpus_key, _records, _digests = _packaged()
    key, _held = _memo_key(corpus_key)
    return _MEMO.find(key) is not None


def fresh_corpus_verdict_row() -> Dict[str, Any]:
    """The row replayed now, bypassing the memo and leaving it untouched."""
    _corpus_key, records, digests = _packaged()
    return _computed_row(records, digests)


def corpus_verdict_row(records: Optional[Iterable[Mapping[str, Any]]] = None) -> Dict[str, Any]:
    """The compiler-revision row: how many inputs of each kind, and a digest of their verdicts.

    Each verdict is bound to its input's digest, so the same verdicts on other inputs are
    another row. For the packaged corpus the row is memoized by the identity of the live
    code (see above); explicit ``records`` are always replayed.
    """
    if records is not None:
        records = list(records)
        return _computed_row(records, [digest(record) for record in records])
    corpus_key, packaged, digests = _packaged()
    _settle(packaged)
    key, held = _memo_key(corpus_key)
    found = _MEMO.find(key)
    if found is not None:
        _MEMO.hits += 1
        _MEMO.entries.append(_MEMO.entries.pop(found))
        return copy.deepcopy(_MEMO.entries[-1][2])
    row = _computed_row(packaged, digests)
    _MEMO.computations += 1
    after, held_after = _memo_key(corpus_key)
    if after == key and _MEMO.find(key) is None:
        # Stored only when NOTHING changed while the row was computed — not "nothing that was
        # already loaded", which is what round 7 asked and what left a window over the modules
        # a replay imports (CDX-184-r8-B21B-R4-STORE-01). :func:`_settle` empties that window
        # by importing them first, so this plain equality is the whole condition again. And
        # only once per key: two callers computing the same cold row concurrently must not
        # occupy two of the four entries (CDX-184-r5-STALE-03, measured at four threads).
        _MEMO.entries.append((key, held, copy.deepcopy(row)))
        del _MEMO.entries[:-MEMO_ENTRIES]
    return row


__all__ = [
    "COMPILE_KINDS",
    "CORPUS_FORMAT",
    "CORPUS_RESOURCE",
    "ENTRY_POINTS",
    "PRODUCER_COMMAND",
    "CorpusNotPackaged",
    "Unrecordable",
    "WALK_KINDS",
    "assert_packaged",
    "bound_arguments",
    "corpus_verdict_row",
    "decode_value",
    "digest",
    "encode_call",
    "encode_symbols_for",
    "encode_value",
    "fresh_corpus_verdict_row",
    "load_corpus",
    "memo_would_hit",
    "parse_corpus",
    "project_outcome",
    "replay",
    "replayed_verdicts",
    "serialize_corpus",
]
