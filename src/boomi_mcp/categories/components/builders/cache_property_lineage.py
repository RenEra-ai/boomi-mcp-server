"""Issue #123 (M11.4, epic #118) — cache/property lineage + scope validation.

Plan-time (pre-mutation) validation that DDP/DPP/Document-Cache handoffs
declared through the M11 authored vocabulary are provably safe:

* a ``cache_get`` has an upstream same-cache writer in an earlier reachable
  path (or declares ``external_writer: true`` — the live subprocess pattern
  where a parent process populated the cache);
* a ``set_ddp`` / ``set_dpp`` source or Decision operand that READS a
  DDP/DPP has an upstream writer whose value is actually visible at the
  read point;
* DDP scope is honored: a DDP written inside one branch leg is NOT visible
  to sibling legs (each leg processes its own copy of the pre-branch
  documents), while a trunk write is visible in every leg;
* branch order is honored: Boomi branch legs execute sequentially in leg
  order, so a leg-2 write feeding a leg-1 read fails; decision legs are
  mutually exclusive, so a write that lives only in the sibling exclusive
  leg cannot be the reader's provable writer.

Cardinality coverage maps onto these rules rather than a separate mechanism
(the M11.4 acceptance classes): 1:1 same-branch carry-forward = trunk
write → downstream read; 1:N broadcast = one DPP/cache write consumed by
multiple later legs; split = DDP written before a split/flow-control step
(the property travels per-document copy); N:1 merge/join = multiple leg
writes into one cache consumed by a later ``cache_get`` / map join. The
negative of each class is one of the errors above.

Backward-compatibility contract (hard constraint from the M11 plan):

* Only the NEW authored kinds opt into lineage enforcement. Legacy
  ``doccacheretrieve`` / ``doccacheremove`` steps and transform modes stay
  exempt — the standalone-retrieve subprocess pattern is live-verified
  (#119 census: "Process Patches from Cache" retrieves with no in-process
  writer) and predates this contract, so enforcing it would reject real
  flows and break the M10 goldens.
* ``dataprocess`` (custom scripting) and ``map_ref`` steps count as
  WILDCARD property writers at their position: live processes overwhelmingly
  set DPPs from Groovy (``ExecutionUtil.setDynamicProcessProperty``) and
  maps can set document properties via property-set functions, neither of
  which is inspectable from the process config. Treating them as opaque
  writers keeps this validator free of false positives at the cost of not
  proving reads that follow a script — an explicitly accepted trade.
  Scripts cannot Add to Cache, so cache lineage stays precise.
* A ``set_ddp``/``set_dpp`` source that carries an explicit ``default_value``
  declares "absence is fine" and is exempt from read-before-write.

PipelineSpec-level lineage is intentionally NOT wired here: every M11 stage
kind is reserved-without-lowering in ``pipeline_models`` (no PipelineSpec →
XML path exists to validate), so the graph walked is the executable
process-config one. When M8 composition lowers pipelines, the same event
model attaches there.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from .connector_builder import BuilderValidationError
from .profile_generation import (
    PROCESS_LINEAGE_AMBIGUOUS_LAST_WRITE,
    PROCESS_LINEAGE_BRANCH_ORDER_INVALID,
    PROCESS_LINEAGE_CACHE_WRITER_MISSING,
    PROCESS_LINEAGE_DDP_SCOPE_INVALID,
    PROCESS_LINEAGE_PROPERTY_READ_BEFORE_WRITE,
)


@dataclass(frozen=True)
class LineageEvent:
    """One read/write of execution state at a position in the process graph.

    ``seq`` is the depth-first emission order, which equals execution order
    because Boomi branch legs run sequentially in leg order. ``branch_path``
    is the stack of (branch ordinal, leg index) pairs — DDP visibility
    requires the writer's branch_path to be a prefix of the reader's (per-leg
    document copies do not share DDPs). ``excl_path`` is the stack of
    (decision ordinal, leg) pairs — a writer is provable only when every
    exclusive choice it sits under is also implied at the read point.
    ``scope`` is ``ddp`` / ``dpp`` / ``cache`` (``wildcard`` writers stand in
    for script/map property writes). For cache events ``name`` is the
    document_cache_id verbatim (literal or $ref token).
    """

    event: str  # "write" | "read"
    scope: str  # "ddp" | "dpp" | "cache" | "wildcard"
    name: str
    seq: int
    branch_path: Tuple[Tuple[int, int], ...] = ()
    excl_path: Tuple[Tuple[int, str], ...] = ()
    step_field: str = ""
    external: bool = False  # reader declared external_writer: true
    has_default: bool = False  # reader carries an explicit default_value
    # strict=False readers (legacy Decision operands, which emit
    # defaultValue="" on the wire — absence is well-defined at runtime, and
    # the pattern predates this contract, e.g. the #117 goldens) only fail
    # when a writer EXISTS but is provably invisible; never on mere absence.
    strict: bool = True


@dataclass
class _Walker:
    events: List[LineageEvent] = field(default_factory=list)
    seq: int = 0
    # Optional in-spec component context (integration-level pass): lets the
    # walker read a $ref'd map's document_cache_joins. None on the
    # process-local pass inside ProcessFlowBuilder.validate_config.
    components_by_key: Optional[Dict[str, Any]] = None

    def add(self, **kwargs: Any) -> None:
        self.events.append(LineageEvent(seq=self.seq, **kwargs))
        self.seq += 1

    def tick(self) -> None:
        """Advance the sequence for opaque steps that emit no event."""
        self.seq += 1


_PROPERTY_PREFIXES = (
    ("dynamicdocument.", "ddp"),
    ("process.", "dpp"),
)


def _property_read_from_operand(
    walker: _Walker,
    operand: Any,
    branch_path: Tuple[Tuple[int, int], ...],
    excl_path: Tuple[Tuple[int, str], ...],
    step_field: str,
) -> None:
    """Record a Decision operand's DDP/DPP read (track operands only)."""
    if not isinstance(operand, dict):
        return
    if str(operand.get("value_type") or "").strip() != "track":
        return
    property_id = str(operand.get("property_id") or "").strip()
    for prefix, scope in _PROPERTY_PREFIXES:
        if property_id.startswith(prefix):
            walker.add(
                event="read",
                scope=scope,
                name=property_id[len(prefix):],
                branch_path=branch_path,
                excl_path=excl_path,
                step_field=step_field,
                strict=False,
            )
            return


def _walk_set_properties_step(
    walker: _Walker,
    step: Dict[str, Any],
    kind: str,
    branch_path: Tuple[Tuple[int, int], ...],
    excl_path: Tuple[Tuple[int, str], ...],
    step_field: str,
) -> None:
    # Source reads happen before the write lands.
    for j, source in enumerate(step.get("source_values") or []):
        if not isinstance(source, dict):
            continue
        value_type = str(source.get("value_type") or "").strip()
        if value_type in ("ddp", "dpp"):
            walker.add(
                event="read",
                scope=value_type,
                name=str(source.get("property_name") or "").strip(),
                branch_path=branch_path,
                excl_path=excl_path,
                step_field=f"{step_field}.source_values[{j}]",
                has_default=source.get("default_value") is not None,
            )
    walker.add(
        event="write",
        scope="ddp" if kind == "set_ddp" else "dpp",
        name=str(step.get("name") or "").strip(),
        branch_path=branch_path,
        excl_path=excl_path,
        step_field=step_field,
    )


def _add_map_join_reads(
    walker: _Walker,
    map_ref_value: Any,
    branch_path: Tuple[Tuple[int, int], ...],
    excl_path: Tuple[Tuple[int, str], ...],
    step_field: str,
) -> None:
    """Record the cache READS an in-spec joined map performs (companion P2).

    Requires the walker's component context; the seq of the reads is the map
    position itself (walker.seq was already advanced past the map's wildcard
    write, so anchor one back).
    """
    if walker.components_by_key is None:
        return
    map_ref = str(map_ref_value or "").strip()
    if not map_ref.startswith("$ref:"):
        return
    target = walker.components_by_key.get(map_ref[len("$ref:"):])
    target_config = getattr(target, "config", None) or {}
    joins = target_config.get("document_cache_joins")
    if not isinstance(joins, list):
        return
    for join in joins:
        if not isinstance(join, dict):
            continue
        walker.events.append(
            LineageEvent(
                event="read",
                scope="cache",
                name=str(join.get("document_cache_id") or "").strip(),
                seq=walker.seq - 1,  # at the map step's position
                branch_path=branch_path,
                excl_path=excl_path,
                step_field=f"{step_field} -> document_cache_joins",
                external=bool(join.get("external_writer", False)),
            )
        )


def _walk_steps(
    walker: _Walker,
    steps: Any,
    branch_path: Tuple[Tuple[int, int], ...],
    excl_path: Tuple[Tuple[int, str], ...],
    field_prefix: str,
) -> None:
    if not isinstance(steps, list):
        return
    for i, step in enumerate(steps):
        if not isinstance(step, dict):
            continue
        kind = str(step.get("kind") or "").strip()
        step_field = f"{field_prefix}[{i}]"
        if kind in ("set_ddp", "set_dpp"):
            _walk_set_properties_step(
                walker, step, kind, branch_path, excl_path, step_field
            )
        elif kind in ("cache_put", "doccacheload"):
            walker.add(
                event="write",
                scope="cache",
                name=str(step.get("document_cache_id") or "").strip(),
                branch_path=branch_path,
                excl_path=excl_path,
                step_field=step_field,
            )
        elif kind == "cache_get":
            walker.add(
                event="read",
                scope="cache",
                name=str(step.get("document_cache_id") or "").strip(),
                branch_path=branch_path,
                excl_path=excl_path,
                step_field=step_field,
                external=bool(step.get("external_writer", False)),
            )
        elif kind in ("dataprocess", "map_ref"):
            # Wildcard property writer (scripts / map property-set functions
            # can write DDPs and DPPs invisibly to this walker).
            walker.add(
                event="write",
                scope="wildcard",
                name="*",
                branch_path=branch_path,
                excl_path=excl_path,
                step_field=step_field,
            )
            # Companion review P2 (#123 follow-up): an in-spec map that
            # declares document_cache_joins READS those caches at map time —
            # count each join as a cache read at this step's position so a
            # join against a never-written cache fails like any other read.
            if kind == "map_ref":
                _add_map_join_reads(
                    walker,
                    step.get("map_ref"),
                    branch_path,
                    excl_path,
                    f"{step_field}.map_ref",
                )
        elif kind == "decision":
            decision_ordinal = walker.seq
            for side in ("left", "right"):
                _property_read_from_operand(
                    walker,
                    step.get(side),
                    branch_path,
                    excl_path,
                    f"{step_field}.{side}",
                )
            walker.tick()
            _walk_steps(
                walker,
                step.get("true_steps") or [],
                branch_path,
                excl_path + ((decision_ordinal, "true"),),
                f"{step_field}.true_steps",
            )
            _walk_steps(
                walker,
                step.get("false_steps") or [],
                branch_path,
                excl_path + ((decision_ordinal, "false"),),
                f"{step_field}.false_steps",
            )
        elif kind == "branch":
            branch_ordinal = walker.seq
            walker.tick()
            for leg_index, leg in enumerate(step.get("legs") or []):
                if not isinstance(leg, dict):
                    continue
                _walk_steps(
                    walker,
                    leg.get("steps") or [],
                    branch_path + ((branch_ordinal, leg_index),),
                    excl_path,
                    f"{step_field}.legs[{leg_index}].steps",
                )
        else:
            # doccacheretrieve / doccacheremove (legacy-exempt), message,
            # flow_control, exception — no lineage events.
            walker.tick()


def collect_lineage_events(
    config: Dict[str, Any],
    components_by_key: Optional[Dict[str, Any]] = None,
) -> List[LineageEvent]:
    """Collect DDP/DPP/cache lineage events from a process config.

    Walks the executable order: the legacy transform slot, then the composed
    ``flow_sequence``, then the target's ``dynamic_path`` Set Properties
    (which both reads its ddp/dpp segments and writes its ``ddp_name`` DDP
    immediately before the target connector step). DLQ catch-path cache
    writes are deliberately NOT collected — the catch leg executes only on
    failure, so it can never be a main-row read's provable writer.
    """
    walker = _Walker(components_by_key=components_by_key)

    transform = config.get("transform")
    if isinstance(transform, dict):
        mode = str(transform.get("mode") or "").strip()
        if mode in ("dataprocess", "map_ref"):
            walker.add(event="write", scope="wildcard", name="*", step_field="transform")
            # QA Bug #145: the legacy single-slot map is the SAME joined-map
            # read surface — and since transform cannot combine with a
            # flow_sequence in v1, no in-process cache writer can exist
            # there, so a joined cache must declare external_writer: true.
            if mode == "map_ref":
                _add_map_join_reads(walker, transform.get("map_ref"), (), (), "transform.map_ref")
        else:
            # doccacheretrieve / doccacheremove stay legacy-exempt; other
            # modes carry no lineage events.
            walker.tick()

    _walk_steps(walker, config.get("flow_sequence"), (), (), "flow_sequence")

    target = config.get("target")
    if isinstance(target, dict):
        dynamic_path = target.get("dynamic_path")
        if isinstance(dynamic_path, dict):
            for j, seg in enumerate(dynamic_path.get("segments") or []):
                if not isinstance(seg, dict):
                    continue
                seg_type = str(seg.get("type") or "").strip()
                if seg_type in ("ddp", "dpp"):
                    walker.add(
                        event="read",
                        scope=seg_type,
                        name=str(seg.get("property_name") or "").strip(),
                        step_field=f"target.dynamic_path.segments[{j}]",
                    )
            walker.add(
                event="write",
                scope="ddp",
                name=str(dynamic_path.get("ddp_name") or "").strip(),
                step_field="target.dynamic_path",
            )

    return walker.events


def _is_path_prefix(prefix: Tuple, path: Tuple) -> bool:
    return len(prefix) <= len(path) and path[: len(prefix)] == prefix


def _writer_visible(writer: LineageEvent, reader: LineageEvent) -> bool:
    """True when the writer's value is provably present at the read point."""
    if writer.seq >= reader.seq:
        return False
    # Every exclusive (decision) choice the writer sits under must also hold
    # at the read point.
    if not _is_path_prefix(writer.excl_path, reader.excl_path):
        return False
    if writer.scope == "ddp" or (writer.scope == "wildcard" and reader.scope == "ddp"):
        # DDPs travel WITH the document: a leg-local write only reaches reads
        # in the same leg subtree; trunk writes reach every leg's copies.
        return _is_path_prefix(writer.branch_path, reader.branch_path)
    return True


def validate_cache_property_lineage(
    events: List[LineageEvent],
) -> Optional[BuilderValidationError]:
    """Return the first unprovable read as a structured error, else None."""
    for reader in events:
        if reader.event != "read":
            continue
        if reader.scope == "cache":
            if reader.external:
                continue
            named = [
                w
                for w in events
                if w.event == "write" and w.scope == "cache" and w.name == reader.name
            ]
            wildcards: List[LineageEvent] = []  # scripts cannot Add to Cache
        else:
            if reader.has_default:
                continue
            named = [
                w
                for w in events
                if w.event == "write"
                and w.scope == reader.scope
                and w.name == reader.name
            ]
            wildcards = [
                w for w in events if w.event == "write" and w.scope == "wildcard"
            ]
        if any(_writer_visible(w, reader) for w in named + wildcards):
            continue

        # No provable writer. Diagnoses below consider NAMED writers only —
        # a wildcard (script/map) can satisfy a read but never condemns one,
        # since its actual property writes are unknowable here.
        candidates = named
        if not candidates:
            if not reader.strict:
                # Legacy-lenient reader: absence is a defined runtime value.
                continue
            if reader.scope == "cache":
                return BuilderValidationError(
                    f"{reader.step_field} reads Document Cache "
                    f"{reader.name!r} but no step writes it in this process.",
                    error_code=PROCESS_LINEAGE_CACHE_WRITER_MISSING,
                    field=reader.step_field,
                    hint=(
                        "Add an upstream cache_put (or Add to Cache) for the "
                        "same cache, or declare external_writer=true when a "
                        "parent process populates it (caches are shared "
                        "within one execution)."
                    ),
                    details={"document_cache_id": reader.name},
                )
            return BuilderValidationError(
                f"{reader.step_field} reads {reader.scope.upper()} "
                f"{reader.name!r} before any step writes it.",
                error_code=PROCESS_LINEAGE_PROPERTY_READ_BEFORE_WRITE,
                field=reader.step_field,
                hint=(
                    "Write the property upstream (set_ddp / set_dpp, a "
                    "script, or a map property-set function), or declare an "
                    "explicit default_value to accept absence."
                ),
                details={"scope": reader.scope, "name": reader.name},
            )

        ddp_sibling = reader.scope == "ddp" and any(
            w.seq < reader.seq
            and _is_path_prefix(w.excl_path, reader.excl_path)
            and not _is_path_prefix(w.branch_path, reader.branch_path)
            for w in candidates
        )
        if ddp_sibling:
            return BuilderValidationError(
                f"{reader.step_field} reads DDP {reader.name!r} whose only "
                "writer sits in a SIBLING branch leg — DDPs travel with each "
                "leg's own document copies and never cross legs.",
                error_code=PROCESS_LINEAGE_DDP_SCOPE_INVALID,
                field=reader.step_field,
                hint=(
                    "Move the write to the trunk before the Branch, or hand "
                    "the value off with execution scope instead (set_dpp or "
                    "a typed Document Cache)."
                ),
                details={"name": reader.name},
            )

        later_writer = any(w.seq >= reader.seq for w in candidates)
        exclusive_only = any(
            w.seq < reader.seq
            and not _is_path_prefix(w.excl_path, reader.excl_path)
            for w in candidates
        )
        if later_writer and not exclusive_only:
            return BuilderValidationError(
                f"{reader.step_field} reads {reader.scope.upper() if reader.scope != 'cache' else 'Document Cache'} "
                f"{reader.name!r} whose writer runs LATER in the flow — "
                "branch legs execute sequentially in leg order.",
                error_code=PROCESS_LINEAGE_BRANCH_ORDER_INVALID,
                field=reader.step_field,
                hint=(
                    "Reorder the legs/steps so the write precedes the read, "
                    "or move the write to the trunk."
                ),
                details={"scope": reader.scope, "name": reader.name},
            )
        return BuilderValidationError(
            f"{reader.step_field} reads {reader.scope.upper() if reader.scope != 'cache' else 'Document Cache'} "
            f"{reader.name!r} whose only writer sits on a mutually exclusive "
            "decision path — the value is not provably present at this read.",
            error_code=PROCESS_LINEAGE_AMBIGUOUS_LAST_WRITE,
            field=reader.step_field,
            hint=(
                "Write the value on the trunk before the Decision, write it "
                "on the SAME leg as the read, or declare an explicit "
                "default_value / external_writer as appropriate."
            ),
            details={"scope": reader.scope, "name": reader.name},
        )
    return None


def validate_config_lineage(
    config: Dict[str, Any],
    components_by_key: Optional[Dict[str, Any]] = None,
) -> Optional[BuilderValidationError]:
    """Collect + validate in one call.

    ProcessFlowBuilder calls it context-less (process-local pass);
    integration_builder._build_plan calls it again WITH components_by_key so
    in-spec map document_cache_joins count as cache readers (companion P2).
    """
    return validate_cache_property_lineage(
        collect_lineage_events(config, components_by_key)
    )
