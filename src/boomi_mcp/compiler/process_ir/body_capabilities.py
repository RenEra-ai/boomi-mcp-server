"""Closed control-body capability registry (issue #141, M12.6).

The registry answers exactly one question per authored slot: *may this node kind
appear in this Branch-leg / Decision-arm position?* It is a closed ALLOWLIST —
a ``(context, slot, kind)`` triple that is not a row here is unsupported, full
stop. That is the same fail-closed shape #140's connector registry uses, and it
means no gated-row table is load-bearing: nested ``branch`` in a Branch leg,
``process_call`` on a Decision FALSE arm, ``return_documents`` anywhere in a body
and every future node kind are rejected by *absence*, not by remembering to list
them.

Why a registry when the Pydantic unions already reject these:

* the unions state the rule *structurally*, the registry states it *as data* —
  so the shipped matrix can be tested, documented and diffed in one place rather
  than read out of five ``Annotated[Union[...]]`` aliases;
* it is the enforcement point for the rules a union cannot express (the control
  DEPTH bound), and it runs before lowering, so every body defect precedes any
  CFG, emission plan, emitter — and therefore any component mutation;
* ``test_body_capability_registry_matches_the_model_unions`` pins the two against
  each other in BOTH directions, so a union that gains a kind without a
  deliberate registry change fails the build, and vice versa.

Evidence for every admitted placement is recorded in
``.codex/plans/issue-141-live-captures.md``; the per-row citations live in the
matrix below.

**Charter (amended by #146).** This module stays compiler-internal: it is never
exported from ``compiler.process_ir.__all__``, and no MCP tool, schema, or
builder may reach it — with ONE named exception. ``boomi_mcp.authoring.
process_ir_projection`` may READ :data:`BODY_CAPABILITIES_V1` (through
:func:`body_placement_rows`) to derive the read-only ``process_ir_authoring``
contract that ``get_schema_template`` serves.

That exception is deliberate and bounded, and it does not reopen ADR-001 §6:

* the projection is OUTPUT ONLY. Nothing a caller sends can re-enter the
  compiler as capability context, so this table remains the sole enforcement
  authority and the projection cannot override or mutate it;
* it projects the SEMANTIC FACT (which node kinds a slot admits), never the
  compiler's representation of it. The served vocabulary is a distinct public
  one — the internal context name ``branch_leg`` is published as
  ``branch_path`` — so no internal identifier crosses the boundary;
* a caller had no other way to learn these rules. Before #146 a rejected
  placement pointed at this file by name, which no MCP tool can fetch.

Enforcement types, the walk functions, and the diagnostic machinery stay
internal. No CFG edge, node id, layout coordinate, shape id, or XML is projected.
"""

from __future__ import annotations

import collections.abc
from types import MappingProxyType
from typing import (
    Annotated,
    Any,
    Dict,
    FrozenSet,
    List,
    Mapping,
    Tuple,
    Union,
    get_args,
    get_origin,
)

from ...errors import (
    PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED,
    PROCESS_IR_CAPABILITY_UNSUPPORTED,
    PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY,
    PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED,
    PROCESS_IR_SEMANTIC_CATCH_UNTERMINATED,
    PROCESS_IR_SEMANTIC_NESTING_LIMIT,
)
from ...models.process_ir import (
    _CONNECTOR_KINDS,
    PLACEMENT_CONNECTOR_MIXING,
    PLACEMENT_ROOT_CONNECTOR_MIXING,
    PROCESS_CALL_PLACEMENT_CONTEXT_LABELS,
    PROCESS_IR_V1_MAX_CONTROL_DEPTH,
    BranchLegV1,
    DecisionFalseArmV1,
    DecisionTrueArmV1,
    ProcessIRV1,
    TryCatchCatchBodyV1,
    TryCatchTryBodyV1,
    process_call_placement_verdict,
    process_call_root_verdict,
)
from .diagnostics import raise_compile_error

_SEMANTIC_PHASE = "semantic_lowering"

#: Body contexts, spelled exactly as the authored JSON pointer segments they
#: correspond to (``/legs/N``, ``/true_arm``, ``/false_arm``).
BRANCH_LEG = "branch_leg"
DECISION_TRUE_ARM = "decision_true_arm"
DECISION_FALSE_ARM = "decision_false_arm"
#: #142 Try/Catch body contexts, spelled as their authored pointer segments.
TRY_BODY = "try_body"
CATCH_BODY = "catch_body"

STEP_SLOT = "step"
TERMINAL_SLOT = "terminal"

#: The closed matrix. Keyed by ``(context, slot)``; the value is the exact set of
#: admitted ``kind`` discriminators.
#:
#: Live evidence (`.codex/plans/issue-141-live-captures.md`):
#:
#: * ``connector_call`` in every body  -> capability ``connector_call_in_control_body``.
#: * ``process_call`` as a Branch-leg TERMINAL -> §2.2, seven Branch legs whose
#:   bodies run process calls. #141 read those as ProcessCall STEPS ending in a
#:   ``stop``; #175 corrected that: the capture attests the control edge landing
#:   ON the call, and the platform projects a call's outbound connection from the
#:   CALLED process's return-document shapes — four of the five captured calls
#:   have none and no outgoing edge at all. So the call IS the end of the leg.
#: * ``process_call`` as a TRUE-arm TERMINAL -> §2.2, ``decision ->true->
#:   processcall``, twice. Absent from the FALSE arm in BOTH slots: the capture
#:   attests TRUE outcomes only.
#: * ``decision`` as a Branch-leg terminal -> §2.1, leg 2 routes into a Decision.
#: * ``branch`` as a Branch-leg terminal   -> DELIBERATELY ABSENT. It appears
#:   nowhere in either captured process, and fail-closed is the rule for an
#:   unproven placement.
#: * bare ``stop`` terminals -> §2.1 proves a Decision FALSE outcome may route
#:   straight to a Stop. The model additionally requires a Branch leg / TRUE arm
#:   to do some work first (the empty-leg question is UNPROVEN, capture §2.4).
#: The SLOT AUTHORITY TABLE. Maps each ``(context, slot)`` to the pydantic field
#: that DEFINES what the slot admits. It carries no node kinds at all — that is
#: the point: the kinds live in the model unions, and this table only names which
#: field is the authority for which slot.
#:
#: #154 introduced it because the matrix below used to hand-list its rows. Five
#: of them were literal kind sets, and one of those five —
#: ``(TRY_BODY, TERMINAL_SLOT) = {"stop"}`` — was the row the issue had to widen.
#: A hand-copy of a fact whose authority is a model field is the duplicate-authority
#: defect class ADR-001 §6 removes, and it had already produced one drift (the
#: Try/Catch step vocabulary silently lacking ``flow_control``/``data_process``).
#: Deriving the matrix means a kind added to a union CANNOT silently lack a
#: placement row.
BODY_SLOT_AUTHORITIES_V1: Mapping[Tuple[str, str], Tuple[Any, str]] = MappingProxyType(
    {
        (BRANCH_LEG, STEP_SLOT): (BranchLegV1, "steps"),
        (BRANCH_LEG, TERMINAL_SLOT): (BranchLegV1, "terminal"),
        (DECISION_TRUE_ARM, STEP_SLOT): (DecisionTrueArmV1, "steps"),
        (DECISION_TRUE_ARM, TERMINAL_SLOT): (DecisionTrueArmV1, "terminal"),
        (DECISION_FALSE_ARM, STEP_SLOT): (DecisionFalseArmV1, "steps"),
        (DECISION_FALSE_ARM, TERMINAL_SLOT): (DecisionFalseArmV1, "terminal"),
        (TRY_BODY, STEP_SLOT): (TryCatchTryBodyV1, "steps"),
        (TRY_BODY, TERMINAL_SLOT): (TryCatchTryBodyV1, "terminal"),
        (CATCH_BODY, STEP_SLOT): (TryCatchCatchBodyV1, "steps"),
        (CATCH_BODY, TERMINAL_SLOT): (TryCatchCatchBodyV1, "terminal"),
    }
)


def _kinds_from_annotation(annotation: Any) -> FrozenSet[str]:
    """Every ``kind`` discriminator reachable from a slot's field annotation.

    Deliberately NOT ``models.process_ir._kinds_of``. That helper assumes the
    exact shape ``Annotated[Union[...], FieldInfo]`` and indexes into it
    positionally, which raises ``IndexError`` on the two shapes this table must
    read:

    * ``List[ControlBodyStepV1]`` — a step slot is a list, so the union sits one
      container deeper;
    * ``Annotated[Union[StopNodeV1], ...]`` — pydantic collapses a single-member
      ``Union`` to the bare class and strips the ``Annotated``, so the try-body
      terminal annotation is literally ``StopNodeV1``.

    The second case is why this walks the annotation instead of pattern-matching
    it. A terminal union that happens to hold exactly one member is not a special
    case to remember; it is just a union with one member, and a reader that
    handles the general shape never has to know which slots are currently
    single-membered.
    """
    origin = get_origin(annotation)
    if origin is Annotated:
        return _kinds_from_annotation(get_args(annotation)[0])
    if origin is Union:
        out: set = set()
        for member in get_args(annotation):
            out |= _kinds_from_annotation(member)
        return frozenset(out)
    if origin in (list, tuple, set, frozenset, collections.abc.Sequence):
        args = get_args(annotation)
        return _kinds_from_annotation(args[0]) if args else frozenset()
    field = getattr(annotation, "model_fields", {}).get("kind")
    if field is None:
        return frozenset()
    literals = get_args(field.annotation)
    return frozenset(literals[:1])


def derive_body_capabilities_v1(
    authorities: Mapping[Tuple[str, str], Tuple[Any, str]] = None,
) -> Mapping[Tuple[str, str], FrozenSet[str]]:
    """Build the placement matrix by READING the model fields.

    Fail-closed twice over: an authority whose field yields no kinds at all is a
    derivation that silently denies everything, so it raises here rather than
    shipping an empty row that ``is_allowed`` would read as "nothing allowed".
    """
    table = BODY_SLOT_AUTHORITIES_V1 if authorities is None else authorities
    derived = {}
    for key, (model, field_name) in table.items():
        field = model.model_fields.get(field_name)
        if field is None:
            raise RuntimeError(
                "body slot authority {0!r} names no field {1!r} on {2}".format(
                    key, field_name, model.__name__
                )
            )
        kinds = _kinds_from_annotation(field.annotation)
        if not kinds:
            raise RuntimeError(
                "body slot authority {0!r} derived an EMPTY kind set from {1}.{2} — "
                "an empty row denies every placement".format(key, model.__name__, field_name)
            )
        derived[key] = kinds
    return MappingProxyType(derived)


#: The closed matrix. Keyed by ``(context, slot)``; the value is the exact set of
#: admitted ``kind`` discriminators — DERIVED from the model fields named in
#: :data:`BODY_SLOT_AUTHORITIES_V1`, never hand-listed.
#:
#: Live evidence (`.codex/plans/issue-141-live-captures.md`), retained because it
#: is the audit trail for why each union member is admitted at all:
#:
#: * ``connector_call`` in every body  -> capability ``connector_call_in_control_body``.
#: * ``process_call`` as a Branch-leg TERMINAL -> §2.2, seven Branch legs whose
#:   bodies run process calls. #141 read those as ProcessCall STEPS ending in a
#:   ``stop``; #175 corrected that: the capture attests the control edge landing
#:   ON the call, and the platform projects a call's outbound connection from the
#:   CALLED process's return-document shapes — four of the five captured calls
#:   have none and no outgoing edge at all. So the call IS the end of the leg.
#: * ``process_call`` as a TRUE-arm TERMINAL -> §2.2, ``decision ->true->
#:   processcall``, twice. Absent from the FALSE arm in BOTH slots: the capture
#:   attests TRUE outcomes only.
#: * ``decision`` as a Branch-leg terminal -> §2.1, leg 2 routes into a Decision.
#: * ``branch`` as a Branch-leg terminal   -> DELIBERATELY ABSENT. It appears
#:   nowhere in either captured process, and fail-closed is the rule for an
#:   unproven placement.
#: * bare ``stop`` terminals -> §2.1 proves a Decision FALSE outcome may route
#:   straight to a Stop. The model additionally requires a Branch leg / TRUE arm
#:   to do some work first (the empty-leg question is UNPROVEN, capture §2.4).
#:
#: #142 M12.7, amended by #154. Both Try/Catch bodies share ONE step vocabulary —
#: a caught document is an ordinary document. The TERMINAL sets differ, and that
#: asymmetry is the design:
#:
#: * a Try body ends on ``stop``, or on ``return_documents`` (#154: the legacy
#:   builder's Return Documents terminal is inside the wrapped flow). An
#:   ``exception`` raised inside a protected path would be caught by that same
#:   path's own handler, and no evidence covers that loop; a staging ``cache_put``
#:   is a recovery shape, not a success one.
#: * a Catch body ends on ``stop``, ``exception``, or a staging ``cache_put`` —
#:   stop the document, raise it explicitly, or hand it to a downstream sink.
#:
#: No control kind appears in either terminal set: nesting is gated (a composed
#: handler silently rewrites the outer one's effective error selection —
#: `.codex/plans/issue-142-live-captures.md` §G6), and ``process_call`` and
#: ``target`` are absent for want of evidence. All of them are rejected by ABSENCE.
BODY_CAPABILITIES_V1: Mapping[Tuple[str, str], FrozenSet[str]] = derive_body_capabilities_v1()


def is_allowed(context: str, slot: str, kind: str) -> bool:
    """Absence is denial — there is no wildcard and no "known kind" fallback."""
    return kind in BODY_CAPABILITIES_V1.get((context, slot), frozenset())


def _check(context: str, slot: str, node: Any, path: str) -> None:
    kind = getattr(node, "kind", None)
    if not is_allowed(context, slot, str(kind)):
        raise raise_compile_error(
            PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY,
            _SEMANTIC_PHASE,
            path,
            message="this node kind is not admitted in this control-body slot",
        )


def _pointer_escape(token: str) -> str:
    return token.replace("~", "~0").replace("/", "~1")


def _join(base: str, *parts: Any) -> str:
    out = base
    for part in parts:
        out += "/" + _pointer_escape(str(part))
    return out


def _walk_body(
    steps: List[Any],
    terminal: Any,
    context: str,
    path: str,
    depth: int,
    connector_above: bool,
    process_call_above: bool = False,
) -> None:
    kinds = [getattr(step, "kind", None) for step in steps]
    # THIS body's own connectors count too, not just an ancestor's. The model
    # path-mode rule already forbids a mixed body, but ``ProcessIRV1`` is exported
    # AND mutable: a caller can validate a legal process-call leg, append a
    # connector call, and hand the model straight to the compiler. An
    # "independent enforcement point" that trusts the model's own invariant is
    # not independent.
    connector_in_body = any(k in _CONNECTOR_KINDS for k in kinds) or (
        getattr(terminal, "kind", None) in _CONNECTOR_KINDS
    )
    # The rule is SYMMETRIC — "these two may not share a path" — so both
    # directions must propagate. Carrying only the connector direction caught
    # `connector -> ... -> process_call` but not `process_call -> ... ->
    # connector`, which a mutable model can express just as easily.
    connector_index = next(
        (i for i, k in enumerate(kinds) if k in _CONNECTOR_KINDS), None
    )
    # #175 moved ``process_call`` into the TERMINAL slot, so every rule that used
    # to read ``kinds`` alone has to read the terminal too. Missing one of these
    # would not merely lose a diagnostic: it would let the widened slot smuggle
    # the mixed path that #141 gated straight past the compiler.
    terminal_is_call = getattr(terminal, "kind", None) == "process_call"
    call_index = kinds.index("process_call") if "process_call" in kinds else None
    if process_call_above and (connector_index is not None or
                               getattr(terminal, "kind", None) in _CONNECTOR_KINDS):
        raise raise_compile_error(
            PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY,
            _SEMANTIC_PHASE,
            _join(path, "steps", connector_index) if connector_index is not None
            else _join(path, "terminal"),
            message=(
                "a connector step may not share a root-to-leaf path with a process_call "
                "(process_call_connector_mixing is gated)"
            ),
        )
    if (
        (call_index is not None or terminal_is_call)
        and connector_above
        # ...and only where this body ADMITS a call at all. Round 6: a root
        # connector above a Decision FALSE arm whose terminal was mutated to a
        # call made this branch answer first, so both paths served the same code
        # at the same pointer with DIFFERENT MESSAGES — ancestor mixing here,
        # generic slot admission there. Where the slot never admitted the kind,
        # the slot check is the true diagnosis, and that is as true of the
        # cross-nesting rule as of the body-local ones.
        and is_allowed(context, TERMINAL_SLOT, "process_call")
    ):
        # The CROSS-NESTING half of ``process_call_connector_mixing``: a connector
        # on an ANCESTOR path. It has no same-body verdict to render — the body
        # cannot see its ancestors — so this branch keeps its own diagnosis.
        # ``models.process_ir`` states the rule too, but only from
        # ``parse_process_ir_v1`` — and ``ProcessIRV1`` is EXPORTED, so a caller
        # can build one with ``model_validate`` and hand it straight to the
        # compiler. A gate that only one of two public entry points enforces is
        # not a gate.
        raise raise_compile_error(
            PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY,
            _SEMANTIC_PHASE,
            _join(path, "steps", call_index) if call_index is not None
            else _join(path, "terminal"),
            message=(
                "a process_call may not share a root-to-leaf path with a connector step "
                "(process_call_connector_mixing is gated)"
            ),
        )
    # SAME-BODY placement is decided by ONE authority, in the model, and merely
    # RENDERED here. Both public entry points reach this rule — authoring through
    # `parse_process_ir_v1`, and a mutated exported model handed straight to
    # `compile_process_ir_v1` — and while each carried its own copy the two
    # disagreed on four documents (#175 round 3, sibling sweep): the prefix
    # pointer, the connector-with-call-terminal pointer, and two orderings where
    # one path reported mixing and the other the return-path rule. The verdict
    # also fixes the ORDER, so "which rule fires" is decided once.
    # The WHOLE verdict is guarded by admissibility, mixing included. Where the
    # slot never admitted a process_call — a Decision FALSE arm, a Try/Catch body
    # — the parser reports the slot check at `/terminal`, and a body-local verdict
    # would answer a question the caller never got to ask: round 5 measured the
    # compiler pointing at the legal connector step while the parser pointed at
    # the illegal terminal. Where the kind is not admitted, the slot check is the
    # true diagnosis and must win, exactly as it does for the return-path reasons.
    verdict = (
        process_call_placement_verdict(
            steps, terminal, context=PROCESS_CALL_PLACEMENT_CONTEXT_LABELS[context]
        )
        if is_allowed(context, TERMINAL_SLOT, "process_call")
        else None
    )
    if verdict is not None:
        reason, at, message = verdict
        if reason == PLACEMENT_CONNECTOR_MIXING:
            raise raise_compile_error(
                PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY,
                _SEMANTIC_PHASE,
                _join(path, *at),
                message=message,
            )
        raise raise_compile_error(
            PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED,
            _SEMANTIC_PHASE,
            _join(path, *at),
            message=message,
        )
    for index, step in enumerate(steps):
        _check(context, STEP_SLOT, step, _join(path, "steps", index))
    terminal_path = _join(path, "terminal")
    _check(context, TERMINAL_SLOT, terminal, terminal_path)
    _walk_control(
        terminal,
        terminal_path,
        depth,
        connector_above or connector_in_body,
        process_call_above or call_index is not None or terminal_is_call,
    )


def _walk_control(
    node: Any,
    path: str,
    depth: int,
    connector_above: bool = False,
    process_call_above: bool = False,
) -> None:
    """Recurse into a control node, counting depth as we go.

    ``depth`` is the number of control nodes ALREADY entered on this root-to-leaf
    path, so the check fires at the first offending node and names its exact
    authored pointer rather than the document root.
    """
    kind = getattr(node, "kind", None)
    if kind == "try_catch":
        _walk_try_catch(node, path, depth, connector_above, process_call_above)
        return
    if kind not in ("branch", "decision"):
        return
    depth += 1
    if depth > PROCESS_IR_V1_MAX_CONTROL_DEPTH:
        raise raise_compile_error(
            PROCESS_IR_SEMANTIC_NESTING_LIMIT,
            _SEMANTIC_PHASE,
            path,
            message=(
                "branch/decision nesting exceeds the ProcessIR v1 maximum control "
                "depth (a compiler bound, not a Boomi platform limit)"
            ),
        )
    if kind == "branch":
        for leg_index, leg in enumerate(node.legs):
            _walk_body(
                list(leg.steps),
                leg.terminal,
                BRANCH_LEG,
                _join(path, "legs", leg_index),
                depth,
                connector_above,
                process_call_above,
            )
        return
    _walk_body(
        list(node.true_arm.steps),
        node.true_arm.terminal,
        DECISION_TRUE_ARM,
        _join(path, "true_arm"),
        depth,
        connector_above,
        process_call_above,
    )
    _walk_body(
        list(node.false_arm.steps),
        node.false_arm.terminal,
        DECISION_FALSE_ARM,
        _join(path, "false_arm"),
        depth,
        connector_above,
        process_call_above,
    )


def _walk_try_catch(
    node: Any,
    path: str,
    depth: int,
    connector_above: bool,
    process_call_above: bool,
) -> None:
    """Re-check a Try/Catch INDEPENDENTLY of the authored model (#142).

    Every rule here is also stated by ``TryCatchNodeV1``'s own validators — and
    that duplication is the point, for the same reason ``_walk_body`` re-checks
    process-call mixing: ``ProcessIRV1`` is exported and NOT frozen, so a caller
    can validate a legal document, mutate it, and hand the model straight to
    ``compile_process_ir_v1``. A gate only ``parse_process_ir_v1`` enforces is not
    a gate.

    ``try_catch`` deliberately does not increment ``depth``. Depth bounds control
    NESTING, and a Try/Catch can neither nest nor contain a control node, so it
    can never lengthen a control chain.
    """
    catch_body = getattr(node, "catch_body", None)
    if catch_body is None or getattr(catch_body, "terminal", None) is None:
        raise raise_compile_error(
            PROCESS_IR_SEMANTIC_CATCH_UNTERMINATED,
            _SEMANTIC_PHASE,
            _join(path, "catch_body", "terminal"),
            message="the catch body does not reach a terminal",
        )

    try_body = getattr(node, "try_body", None)
    if try_body is None or getattr(try_body, "terminal", None) is None:
        raise raise_compile_error(
            PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED,
            _SEMANTIC_PHASE,
            _join(path, "try_body", "terminal"),
            message="the try body does not reach a terminal",
        )

    scope = getattr(node, "scope", None)
    if scope not in ("process", "connector"):
        raise raise_compile_error(
            PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED,
            _SEMANTIC_PHASE,
            _join(path, "scope"),
            message="unsupported error scope",
        )

    try_steps = list(try_body.steps)
    try_kinds = [getattr(step, "kind", None) for step in try_steps]
    if scope == "connector":
        # The verified target-local topology: property preparation, then exactly
        # the one call being protected.
        if (
            not try_kinds
            or try_kinds[-1] != "connector_call"
            or try_kinds.count("connector_call") != 1
            or any(k not in ("set_ddp", "set_dpp") for k in try_kinds[:-1])
        ):
            raise raise_compile_error(
                PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED,
                _SEMANTIC_PHASE,
                _join(path, "try_body", "steps"),
                message=(
                    "a connector-scoped try body must be optional property steps "
                    "followed by exactly the one connector_call it protects"
                ),
            )
    elif not try_kinds or try_kinds[0] != "connector_call":
        raise raise_compile_error(
            PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED,
            _SEMANTIC_PHASE,
            _join(path, "try_body", "steps"),
            message=(
                "a process-scoped try body must begin with the connector_call that "
                "produces the flow's documents"
            ),
        )

    _walk_body(
        try_steps,
        try_body.terminal,
        TRY_BODY,
        _join(path, "try_body"),
        depth,
        connector_above,
        process_call_above,
    )
    _walk_body(
        list(catch_body.steps),
        catch_body.terminal,
        CATCH_BODY,
        _join(path, "catch_body"),
        depth,
        connector_above,
        process_call_above,
    )


def _check_try_catch_placement(ir: ProcessIRV1) -> None:
    """Re-check WHERE a Try/Catch sits, independently of the model (#142).

    Placement is part of the error-scope contract — each scope names a topology
    the compiler has a verified shape for — so it needs the same mutable-model
    defence as the body rules above.
    """
    steps = list(ir.body.steps)
    kinds = [getattr(step, "kind", None) for step in steps]
    for index, step in enumerate(steps):
        if kinds[index] != "try_catch":
            continue
        path = _join("/body", "steps", index)
        if index != len(steps) - 1:
            raise raise_compile_error(
                PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED,
                _SEMANTIC_PHASE,
                path,
                message=(
                    "no step may follow a try_catch — both its paths terminate "
                    "independently and ProcessIR v1 emits no join"
                ),
            )
        scope = getattr(step, "scope", None)
        if scope == "process":
            # A process scope owns the whole flow, so it must BE the whole flow.
            if len(steps) != 1:
                raise raise_compile_error(
                    PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED,
                    _SEMANTIC_PHASE,
                    path,
                    message=(
                        "a process-scoped try_catch must be the sole root step"
                    ),
                )
        elif scope == "connector":
            # A connector scope protects a DOWNSTREAM call, so something must
            # produce the documents ahead of it. This is also what keeps the
            # source outside the retried region.
            if len(steps) < 2 or "connector_call" not in kinds[:index]:
                raise raise_compile_error(
                    PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED,
                    _SEMANTIC_PHASE,
                    path,
                    message=(
                        "a connector-scoped try_catch must follow the connector_call "
                        "that produces the flow's documents"
                    ),
                )


def _check_process_call_placement(ir: ProcessIRV1) -> None:
    """Re-check WHERE a root ``process_call`` sits, independently of the model (#175).

    A call ends its path, so the only supported root shape is the exact
    singleton. Same mutable-model defence as the body rules: ``ProcessIRV1`` is
    exported, so a caller can validate a legal singleton, append a stop, and hand
    the model straight to ``compile_process_ir_v1``.

    Without this the appended step would not be caught HERE but much later, by
    ``check_cfg_invariants`` reporting that flow continues past a terminal node —
    fail-closed, but blaming the CFG for an authoring defect and pointing at a
    node id rather than the authored pointer. ``validate_body_capabilities`` runs
    before lowering, so this claims the diagnosis first.
    """
    verdict = process_call_root_verdict(
        [getattr(step, "kind", None) for step in ir.body.steps]
    )
    if verdict is None:
        return
    reason, at, message = verdict
    if reason == PLACEMENT_ROOT_CONNECTOR_MIXING:
        # Mixing keeps its own code at the root, exactly as it does on the parser
        # path: the two must not serve different CODES for one document, which is
        # what live QA measured before this was unified.
        raise raise_compile_error(
            PROCESS_IR_CAPABILITY_UNSUPPORTED,
            _SEMANTIC_PHASE,
            _join("/body", *at),
            message=message,
        )
    raise raise_compile_error(
        PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED,
        _SEMANTIC_PHASE,
        _join("/body", *at),
        message=message,
    )


def validate_body_capabilities(ir: ProcessIRV1) -> None:
    """Check every control-body slot and the control-depth bound.

    Runs BEFORE lowering (see ``pipeline.compile_process_ir_v1``), so a body
    defect is rejected while nothing but the authored document exists.

    A document with no control node walks zero bodies and returns immediately,
    so no pre-#141 dialect changes behaviour.
    """
    _check_try_catch_placement(ir)
    _check_process_call_placement(ir)
    root_kinds = [getattr(step, "kind", None) for step in ir.body.steps]
    connector_at_root = any(k in _CONNECTOR_KINDS for k in root_kinds)
    for index, step in enumerate(ir.body.steps):
        _walk_control(
            step,
            _join("/body", "steps", index),
            0,
            connector_at_root,
            "process_call" in root_kinds,
        )


def registry_kinds() -> Dict[Tuple[str, str], FrozenSet[str]]:
    """A plain-dict copy, for the coverage test that pins registry vs unions."""
    return {key: set(value) for key, value in BODY_CAPABILITIES_V1.items()}


#: Internal body context -> the PUBLIC name the authoring contract publishes.
#:
#: Only ``branch_leg`` actually renames, and it is not cosmetic: ``branch_leg``
#: is a compiler-internal identifier that the served surface is forbidden to
#: carry (``tests/test_process_ir_compiler_surface.py::FORBIDDEN_NAMES``), and
#: "path" is the platform's own word for a Branch outlet. The other four pass
#: through unchanged rather than being renamed for symmetry — a rename with no
#: reason is a second vocabulary to keep in step.
#:
#: The mapping is TOTAL and INJECTIVE over the contexts this registry uses, and a
#: parity test pins both directions, so projecting through it loses no fact and
#: cannot collapse two contexts into one.
PUBLIC_BODY_CONTEXTS: Mapping[str, str] = MappingProxyType(
    {
        BRANCH_LEG: "branch_path",
        DECISION_TRUE_ARM: DECISION_TRUE_ARM,
        DECISION_FALSE_ARM: DECISION_FALSE_ARM,
        TRY_BODY: TRY_BODY,
        CATCH_BODY: CATCH_BODY,
    }
)


def body_placement_rows() -> Tuple[Tuple[str, str, Tuple[str, ...]], ...]:
    """The closed matrix as sorted public data: (context, slot, admitted kinds).

    Contexts are the PUBLIC names (see :data:`PUBLIC_BODY_CONTEXTS`); kinds are
    the authored discriminators, which are already public vocabulary. Sorted so
    the projection — and therefore ``compiler_revision`` — is deterministic.

    What this deliberately does NOT emit is the complement: the denied triples
    are not enumerated, because the registry is an ALLOWLIST and absence is the
    rule. The contract publishes that rule once, as
    ``unlisted_placement_state``, rather than shipping a combinatorial table
    that would grow silently wrong every time a node kind is added.
    """
    return tuple(
        sorted(
            (PUBLIC_BODY_CONTEXTS[context], slot, tuple(sorted(kinds)))
            for (context, slot), kinds in BODY_CAPABILITIES_V1.items()
        )
    )


__all__ = [
    "BODY_CAPABILITIES_V1",
    "BRANCH_LEG",
    "CATCH_BODY",
    "DECISION_FALSE_ARM",
    "DECISION_TRUE_ARM",
    "PUBLIC_BODY_CONTEXTS",
    "STEP_SLOT",
    "TERMINAL_SLOT",
    "TRY_BODY",
    "body_placement_rows",
    "is_allowed",
    "registry_kinds",
    "validate_body_capabilities",
]
