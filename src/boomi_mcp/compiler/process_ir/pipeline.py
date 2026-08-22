"""Compiler entry points: parse -> lower -> check (issue #137).

Orchestration lives here rather than in ``diagnostics`` (where the architect
plan placed it) because ``diagnostics`` is imported BY ``lowering`` and
``invariants`` — hosting the entry points there would close an import cycle.

One compile is pure with respect to the same ``(ir, symbols)`` pair: it reads no
clock, no environment, and no global state, and it snapshots every authored
value it touches.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, List, Optional, Tuple

from ...errors import PROCESS_IR_COMPILE_INTERNAL
from ...models.process_ir import (
    ProcessIRV1,
    ProcessIRValidationError,
    parse_process_ir_v1,
    raw_process_ir_payload,
)
from .body_capabilities import validate_body_capabilities
from .connector_resolution import validate_connector_calls
from .contracts import EmissionPlanV1, SemanticCfgV1, SymbolTableV1
from .diagnostics import (
    CompilerDiagnostic,
    ProcessIRCompileError,
    diagnostic,
    node_identity_for,
)
from .invariants import check_cfg_invariants, check_emission_plan_invariants
from .lowering import lower_cfg_to_emission_plan, lower_process_ir_to_cfg

if TYPE_CHECKING:  # pragma: no cover - typing only, avoids an import cycle
    from .semantic_validation.contracts import ProcessIRValidationCapabilitiesV1
    from .semantic_validation.validation_policy import LegacyValidationPolicyV1


def _guarded(phase, action, *args):
    """Run one compiler stage, converting an unexpected error into a diagnostic.

    The exception's text and type are deliberately discarded: an internal
    message can carry authored values, and diagnostics are logged.
    """
    try:
        return action(*args)
    except ProcessIRCompileError:
        raise
    except Exception:  # noqa: BLE001 - deliberate: never leak internals
        raise ProcessIRCompileError(
            [diagnostic(PROCESS_IR_COMPILE_INTERNAL, phase, "")]
        ) from None


def _connector_metadata(cfg, symbols):
    """``(code, path) -> the original CompilerDiagnostic``, from #140 itself.

    The gate reaches #140's codes by DELEGATION, and the conversion to findings
    drops the compiler `phase` — which is part of the diagnostic contract. It
    cannot be re-derived from the code: `PROCESS_IR_SEMANTIC_PROFILE_MISMATCH`
    is raised by connector resolution (`reference_resolution`) AND by the map
    pass (`semantic_lowering`), so only the raising site knows.

    The MESSAGE and REMEDIATION are recovered for the same reason. The gate
    rebuilds diagnostics through `finding()`, whose static tables cover this
    issue's own codes only — a delegated #140/#142 code therefore fell back to
    generic text, losing the code-specific wording an author actually acts on.

    So the whole diagnostic is read back from the one function that produces it.
    This runs ONLY when the report already has errors — the success path pays
    nothing, and the compile is failing anyway.
    """
    try:
        validate_connector_calls(cfg, symbols)
    except ProcessIRCompileError as exc:
        return {(item.code, item.path): item for item in exc.diagnostics}
    except Exception:  # noqa: BLE001 - phase recovery must never mask the report
        pass
    return {}


def _enforce_semantic_report(ir, cfg, symbols, policy, capabilities) -> None:
    """Block the compile when the unified semantic report has ERRORS.

    Imported lazily: ``semantic_validation`` imports the compiler's own
    contracts, so a module-level import here would close a cycle.

    ``capabilities`` is the trusted-contract set. Without it the gate always ran
    with ``DEFAULT_VALIDATION_CAPABILITIES``, so a flow that is valid ONLY
    because a typed map/script/subprocess contract establishes an effect was
    rejected by the compiler even though ``validate_process_ir`` called with the
    same contracts reports it valid — which made the typed capability surface
    unusable for canonical compilation.

    ``policy`` is a legacy adapter's exemption set, or None for STRICT. Applying
    it here rather than at an outer boundary is what lets the canonical path be
    gated for everyone while a migrated dialect keeps the behaviour its goldens
    pin — the two are not in tension once the adapter passes its identity in.

    Errors block; warnings and advisories do not — the compile contract is an
    artifact or an exception, with nowhere to carry a non-blocking finding.
    """
    from .semantic_validation.pipeline import validate_lowered_process_ir
    from .semantic_validation.validation_policy import apply_policy

    from .semantic_validation.contracts import DEFAULT_VALIDATION_CAPABILITIES

    report = validate_lowered_process_ir(
        ir, cfg, symbols, capabilities or DEFAULT_VALIDATION_CAPABILITIES
    )
    report = apply_policy(report, policy)
    if not report.errors:
        return
    delegated = _connector_metadata(cfg, symbols)

    def _restore(item):
        """The delegated original where there is one, else the report finding.

        #140/#142 own their codes AND their wording; this gate only changes the
        presentation, so anything it can hand back verbatim it does.
        """
        origin = delegated.get((item.code, item.path))
        return CompilerDiagnostic(
            code=item.code,
            phase=origin.phase if origin else "semantic_lowering",
            path=item.path,
            node_identity=item.node_identity,
            message=origin.message if origin else item.message,
            remediation=origin.remediation if origin else item.remediation,
            internal_node_id=item.internal_node_id,
        )

    raise ProcessIRCompileError([_restore(item) for item in report.errors])


#: The scalar types this layer normalises, most-derived first — ``bool`` before
#: ``int`` because ``isinstance(True, int)`` is True and coercing a bool through
#: ``int`` would change the document. This is an ENUMERATION, and it is
#: deliberately not extended: `complex` and `bytearray` are builtin-derived and
#: absent, which is accurate rather than an oversight — see the accepted
#: limitation in the #178 ledger. Adding them would buy nothing, because the same
#: capability is already reachable through any non-builtin carrier.
_NORMALISED_SCALARS = (bool, int, float, str, bytes)


class _NotNormalisable(Exception):
    """A scalar that cannot be reduced to its exact builtin. Never escapes."""


def _plain_scalar(value: Any) -> Any:
    """A ``str``/``int``/``float``/``bool``/``bytes`` subclass replaced by the
    EXACT builtin; every other object passed through unchanged.

    The exact-type check is load-bearing, not belt and braces. ``builtin(value)``
    dispatches an overridable conversion — ``str(x)`` calls ``x.__str__()`` — and
    a subclass can return ANOTHER subclass from it, so the coercion alone can hand
    back an object still carrying caller dunders. Measured: a ``str`` subclass
    whose ``__str__`` returns a second subclass whose ``__ne__`` raises forged a
    real-coded diagnostic straight through the normalisation that was supposed to
    prevent it. So the result is verified, and a value that will not reduce is
    refused rather than trusted.

    Non-builtin objects are left alone BY DESIGN: a ``datetime`` in a ``str``
    field must reach the parser wrong-typed so it is refused rather than repaired,
    which is the hole this whole issue exists to close.
    """
    if value is None:
        return None
    for builtin in _NORMALISED_SCALARS:
        if isinstance(value, builtin):
            if type(value) is builtin:
                return value
            reduced = builtin(value)
            if type(reduced) is not builtin:
                raise _NotNormalisable(builtin.__name__)
            return reduced
    return value


def _inert_payload(payload: Any) -> Any:
    """Rebuild a dumped payload out of plain containers and scalars before parsing.

    WHAT THIS GUARANTEES, stated narrowly because two earlier revisions of this
    docstring claimed more than the code delivered: every mapping, sequence and
    dict key, and every ``str``/``int``/``float``/``bool``/``bytes`` scalar, is
    rebuilt as an EXACT builtin — verified, not merely coerced. None of those can
    carry a caller-defined dunder into the parser, so the hooks that exist run
    HERE, once, inside the caller's guard, and a ``ProcessIRValidationError``
    raised during the parse really was authored by the parser.

    WHAT IT DOES NOT GUARANTEE: any OTHER object — ``complex`` and ``bytearray``
    as much as ``datetime`` or a bare instance — is passed through unchanged by
    design, still carrying its own dunders, and the parser's pre-validation
    version comparison will invoke one. That residue is a demonstrated, accepted
    limitation recorded in the #178 ledger: reaching it needs in-process Python
    that subclasses an exported model, and such a caller can already monkeypatch
    this module, the guard it funnels into, and the logging handler — measured, so
    the forgery path grants nothing new. Five variants were patched individually
    before that was acknowledged instead of patched a sixth time.
    """
    if isinstance(payload, dict):
        return {
            _plain_scalar(key): _inert_payload(value)
            for key, value in payload.items()
        }
    if isinstance(payload, (list, tuple)):
        return [_inert_payload(value) for value in payload]
    return _plain_scalar(payload)


def _internal_compile_error() -> ProcessIRCompileError:
    """The value-free refusal every unexpected failure in this module serves."""
    return ProcessIRCompileError(
        [diagnostic(PROCESS_IR_COMPILE_INTERNAL, "schema", "")]
    )


def _compile_error_from_validation(
    exc: ProcessIRValidationError,
) -> ProcessIRCompileError:
    """One translation, used by BOTH parser-authority paths (#178).

    ``code``/``path``/``message``/``remediation`` are preserved VERBATIM
    (ADR-001 §7: later introducers add codes, never rename them); ``phase`` is
    ``"schema"`` and ``node_identity`` is derived from the pointer.

    EXCEPTION TYPE IS NOT PROVENANCE, and this function is where that stopped
    being assumed. A caller can subclass the exported ``ProcessIRV1`` and have
    ``model_dump`` RETURN a hostile mapping — a ``dict`` subclass whose
    ``items()`` raises — so arbitrary code runs INSIDE ``parse_process_ir_v1``
    (the secret pre-scan walks the payload) and raises a
    ``ProcessIRValidationError`` the parser never authored. Forwarding it
    verbatim served a caller-chosen code, pointer, message and remediation
    through the compiler's own channel, carrying planted secret text. Measured;
    an earlier revision closed only the variant where the DUMP raises, which is
    the same trust one boundary earlier.

    So each diagnostic is checked against the parser's OWN served code set before
    it is believed. A code outside that set was not authored by the parser, no
    matter what type carried it, and the whole error degrades to the value-free
    internal refusal rather than being partially trusted.
    """
    try:
        from ...models.process_ir import process_ir_v1_parse_diagnostic_specs

        authored = {spec["code"] for spec in process_ir_v1_parse_diagnostic_specs()}
        translated = []
        for item in exc.diagnostics:
            code = getattr(item, "code", None)
            path = getattr(item, "path", None)
            message = getattr(item, "message", None)
            remediation = getattr(item, "remediation", None)
            if not isinstance(code, str) or code not in authored:
                return _internal_compile_error()
            if not isinstance(path, str) or not isinstance(message, str):
                return _internal_compile_error()
            if remediation is not None and not isinstance(remediation, str):
                return _internal_compile_error()
            translated.append(
                CompilerDiagnostic(
                    code=code,
                    phase="schema",
                    path=path,
                    node_identity=node_identity_for(path),
                    message=message,
                    remediation=remediation,
                )
            )
        if not translated:
            return _internal_compile_error()
        return ProcessIRCompileError(translated)
    except ProcessIRCompileError:
        raise
    except Exception:  # noqa: BLE001 - a malformed diagnostic must not escape raw
        # Without this, a forged diagnostic whose attributes misbehave made the
        # TRANSLATION raise — escaping as a bare RuntimeError past every handler,
        # all of which catch only ProcessIRCompileError.
        return _internal_compile_error()


def _parse_payload_for_compile(payload: Any) -> ProcessIRV1:
    """Parse an authored payload, translating parse failures into compile ones.

    #178: extracted from ``parse_and_compile_process_ir_v1`` so BOTH entry points
    raise one error family. ``ProcessIRValidationError`` and
    ``ProcessIRCompileError`` are unrelated types — neither is a subclass of the
    other — and every production handler catches only the latter
    (``authoring/workflow.py``, ``recipes/engine.py``,
    ``legacy_adapters/emission.py``; the materialization and apply call sites
    have no local ``try`` at all). Letting a raw ``ProcessIRValidationError``
    escape the compile entry would therefore defeat all of them and serve a
    refusal with no ``error_code`` — measured, and the single decision that
    separates a safe change from a broken one.

    ``code``/``path``/``message``/``remediation`` are preserved VERBATIM
    (ADR-001 §7: later introducers add codes, never rename them);
    ``phase="schema"`` and ``node_identity`` is derived from the pointer.
    """
    try:
        return parse_process_ir_v1(payload)
    except ProcessIRValidationError as exc:
        raise _compile_error_from_validation(exc) from None
    except Exception:  # noqa: BLE001 - deliberate: never leak internals
        # An UNEXPECTED parser failure must not escape carrying its text: the
        # message can echo authored values, and diagnostics get logged. The
        # compile stages are already guarded this way; parse was not.
        #
        # There is deliberately NO `except ProcessIRCompileError: raise` arm. It
        # was dead for its stated purpose — nothing under `models/` raises that
        # type, and all eight real refusals raise `ProcessIRValidationError` — and
        # live as a hazard: a forged compile error raised from inside the parse
        # bypassed the guarded translation entirely, with no allowlist at all.
        # Same shape as the arm deleted one boundary earlier, surviving one
        # boundary over. Measured.
        raise ProcessIRCompileError(
            [diagnostic(PROCESS_IR_COMPILE_INTERNAL, "schema", "")]
        ) from None


def _reparse_process_ir_for_compile(ir: ProcessIRV1) -> ProcessIRV1:
    """Re-establish the PARSER's authority over a caller-owned model (#178).

    ``ProcessIRV1`` is exported and mutable, so a caller may parse a legal
    document, mutate the model, and hand it straight to the compiler — reaching
    the compile stages with a document the parser would have refused. The two
    paths agreed on the DECISION for most such documents but not on which
    diagnostic they served, and — measured at `cdd7a3b` — not always on the
    decision either: a Branch leg with a trailing ``cache_put``, a root ``source``
    out of position, a one-leg Branch and any mutated ``version`` were all
    refused by the parser and ACCEPTED by the compiler, which models none of
    those rules. Dumping and re-parsing makes the parser the single authority for
    grammar, so both entry points refuse the same documents with the same
    ``(code, pointer, message, remediation)``.

    The dump is guarded SEPARATELY because it happens before
    ``_parse_payload_for_compile`` and so is outside that helper's own guard.

    ``mode="python"`` is the whole point, and ``mode="json"`` is WRONG here. A
    json dump is COERCIVE: it renders a ``datetime`` in a ``str`` field as an ISO
    string and ``bytes`` as text, handing the parser an already-repaired document
    that it then accepts — which is how a mutated model slipped past this very
    re-parse. The raw state is what the authority must judge.

    ``warnings=False`` is load-bearing for the AR2-01 reason
    ``authoring/workflow.py`` already documents: dumping a model that may have
    been mutated renders the caller's authored values into a pydantic serializer
    warning before the value-free parser runs — measured to emit an authored
    secret verbatim.

    Exactly ONE dump, deliberately. An earlier revision dumped to json with
    ``warnings="error"`` as a cheap detector and fell back to a raw dump when it
    raised. That is a DESTRUCTIVE probe: a field holding a one-shot iterable
    (``ir.body.steps = (s for s in ir.body.steps)``) is CONSUMED by the first
    dump, so the fallback re-dumped an exhausted generator, saw an empty
    ``/body/steps``, and refused a document the parser accepts — with a
    cardinality diagnostic that described the probe's damage rather than the
    document. Measured at `1618f99`. Reading the state once removes the failure
    mode instead of sequencing around it.
    """
    try:
        payload = _inert_payload(raw_process_ir_payload(ir))
    except Exception:  # noqa: BLE001 - deliberate: never leak internals
        # EVERY dump exception is internal, with no `ProcessIRValidationError`
        # special case. Parsing has not started at this point, so an exception of
        # that type cannot be parser-authored no matter what it claims to be —
        # and an earlier revision forwarded it verbatim, which let a caller
        # supplying a `ProcessIRV1` SUBCLASS whose `model_dump` raises it serve an
        # arbitrary code, pointer and message through the compiler's own error
        # channel. Measured: a planted secret reached the served message intact,
        # reopening the AR2-01 value-free contract through a branch that was dead
        # for its stated purpose. Value-free, like every other unexpected failure
        # in this module.
        raise ProcessIRCompileError(
            [diagnostic(PROCESS_IR_COMPILE_INTERNAL, "schema", "")]
        ) from None
    return _parse_payload_for_compile(payload)


def compile_process_ir_v1(
    ir: ProcessIRV1,
    symbols: SymbolTableV1,
    *,
    validation_policy: Optional["LegacyValidationPolicyV1"] = None,
    capabilities: Optional["ProcessIRValidationCapabilitiesV1"] = None,
) -> Tuple[SemanticCfgV1, EmissionPlanV1]:
    """Re-validate through the parser authority, then compile (#178).

    The re-parse is UNCONDITIONAL, including when ``validation_policy`` is
    supplied. A legacy policy reclassifies POST-LOWERING semantic findings only:
    the four codes it can downgrade are all
    ``PROCESS_IR_SEMANTIC_LINEAGE_*``/``PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNKNOWN``,
    and none of them is in the parser's raisable set — so a policy has nothing to
    exempt at the grammar boundary and skipping the re-parse for policy-bearing
    callers would only preserve the mutable-model hole. That disjointness is
    pinned by a derived test, not asserted here.

    The compile stages consume the FRESH re-parsed model, never the caller-owned
    object that was merely checked — otherwise a mutation performed between the
    check and the stages would still reach them.

    PRECONDITION — the caller's model is READ EXACTLY ONCE. Re-validating means
    reading every field, so a model whose field holds a ONE-SHOT ITERABLE
    (``ir.body.steps = (s for s in ir.body.steps)``) is single-use: the first
    compile succeeds and drains it, and a second compile of the SAME OBJECT sees
    an empty sequence and refuses. That is Python's iterator semantics, not a
    compiler rule, and it cannot be sequenced away — the identical drain occurs
    for any re-parse design, measured on the pre-#178 shape too. Callers that
    compile one model more than once (the canonical apply path does, three times
    per typed apply) must therefore hold a materialised model; every production
    model does, because it comes from a parse. A caller needing the exact model
    that was compiled should take it from
    :func:`compile_process_ir_model_v1` rather than re-compiling its own.
    """
    # Bound to a local rather than nested into the call below, so the source
    # READS in execution order. Nested, the core's name appears textually first
    # even though the re-parse evaluates first — which makes an ordering check
    # over this function's source unreadable and, worse, wrong-looking to anyone
    # auditing that the parser really does run first.
    reparsed = _reparse_process_ir_for_compile(ir)
    return _compile_parsed_process_ir_v1(
        reparsed,
        symbols,
        validation_policy=validation_policy,
        capabilities=capabilities,
    )


def compile_process_ir_model_v1(
    ir: ProcessIRV1,
    symbols: SymbolTableV1,
    *,
    validation_policy: Optional["LegacyValidationPolicyV1"] = None,
    capabilities: Optional["ProcessIRValidationCapabilitiesV1"] = None,
) -> Tuple[ProcessIRV1, SemanticCfgV1, EmissionPlanV1]:
    """Re-validate a caller-owned MODEL, compile it, and hand back all three.

    #178, QA round 2. The model -> payload conversion belongs in exactly ONE
    place. A caller that dumps first and calls a payload entry point picks its own
    dump mode, and `mode="json"` REPAIRS a wrong-typed value before the parser
    sees it — so `authoring/workflow.py` accepted a mutated model that the compile
    entry refused. Two public paths, two answers, for one model.

    Callers that need the re-validated model (to canonicalize or fingerprint the
    thing that was actually compiled) use this instead of dumping by hand.
    """
    revalidated = _reparse_process_ir_for_compile(ir)
    cfg, plan = _compile_parsed_process_ir_v1(
        revalidated,
        symbols,
        validation_policy=validation_policy,
        capabilities=capabilities,
    )
    return revalidated, cfg, plan


def _compile_parsed_process_ir_v1(
    ir: ProcessIRV1,
    symbols: SymbolTableV1,
    *,
    validation_policy: Optional["LegacyValidationPolicyV1"] = None,
    capabilities: Optional["ProcessIRValidationCapabilitiesV1"] = None,
) -> Tuple[SemanticCfgV1, EmissionPlanV1]:
    """Lower an ALREADY-PARSED IR into its CFG and emission plan, invariant-checked.

    #178: private, and never exported. Its one precondition is that ``ir`` came
    straight from ``parse_process_ir_v1`` — public ``compile_process_ir_v1``
    guarantees that by re-parsing, and ``parse_and_compile_process_ir_v1`` by
    parsing. Calling it with a caller-owned model would reopen exactly the hole
    #178 closes, which is why it is not in ``__all__``.

    Any unexpected exception becomes a single static ``PROCESS_IR_COMPILE_INTERNAL``
    diagnostic. The exception's text and type are deliberately NOT interpolated:
    an internal message can carry authored values, and diagnostics are logged.

    ``validation_policy`` is how a LEGACY dialect carries its exemptions into the
    canonical gate. It is a keyword with a STRICT default, so the compiler is
    gated for every caller and a legacy adapter opts into its own documented
    leniency rather than the compiler guessing an identity it cannot see.

    An earlier attempt put this gate at ``emit_legacy_result`` instead, on the
    reasoning that the compiler cannot know which adapter produced its IR and so
    cannot look a policy up. That is true and beside the point: the adapter knows,
    and can pass it. Leaving the canonical path ungated meant a direct caller of
    this function got no semantic validation at all — which is the acceptance
    criterion this issue exists to satisfy.
    """
    # Phase is part of the diagnostic contract, so an unexpected defect is
    # attributed to the stage it actually happened in — reporting a CFG-lowering
    # crash as "emission_planning" sends a reader to the wrong half of the
    # compiler.
    # #141: control-body slots and the control-depth bound are checked FIRST, on
    # the authored document, before a CFG exists. Two reasons it leads: a body
    # defect names an authored JSON pointer that is meaningless once flattened
    # into CFG nodes, and every semantic error must precede any mutation. A
    # document with no branch/decision walks nothing and returns immediately.
    _guarded("semantic_lowering", validate_body_capabilities, ir)
    cfg = _guarded("semantic_lowering", lower_process_ir_to_cfg, ir)
    _guarded("semantic_lowering", check_cfg_invariants, cfg)
    # #140's connector resolution is NOT a separate stage here. It runs inside
    # the gate below, via `flow.collect_connector_flow_findings`, which delegates
    # to `validate_connector_calls` and preserves its codes verbatim.
    #
    # It used to run first, and that destroyed ACCUMULATION: a payload with an
    # unresolvable operation in one Branch leg and a read-before-write in another
    # reported only the connector error, because the fail-fast stage raised
    # before any other collector ran. Measured — the standalone validator
    # reported both. "Accumulate, don't fail fast" is a stated criterion of this
    # issue, so the duplicate pass had to go rather than be documented around.
    # #143: the unified semantic gate. It runs on the CFG that was just lowered
    # and BEFORE any emission plan exists, so "no plan lowering occurs with
    # report errors" holds by construction rather than by convention.
    # Through `_guarded`, like every other stage: a deliberate finding raises
    # ProcessIRCompileError and passes through untouched, while an UNEXPECTED
    # collector or policy failure becomes the promised value-free
    # PROCESS_IR_COMPILE_INTERNAL instead of escaping as a raw exception with
    # its text — production builders catch ProcessIRCompileError, not anything.
    _guarded(
        "semantic_lowering",
        _enforce_semantic_report,
        ir,
        cfg,
        symbols,
        validation_policy,
        capabilities,
    )
    plan = _guarded("emission_planning", lower_cfg_to_emission_plan, cfg, symbols)
    _guarded("emission_planning", check_emission_plan_invariants, plan, cfg, symbols)
    return cfg, plan


def parse_and_compile_process_ir_v1(
    payload: Any,
    symbols: SymbolTableV1,
    *,
    validation_policy: Optional["LegacyValidationPolicyV1"] = None,
    capabilities: Optional["ProcessIRValidationCapabilitiesV1"] = None,
) -> Tuple[ProcessIRV1, SemanticCfgV1, EmissionPlanV1]:
    """Parse an authored payload, then compile it.

    Both gate keywords are forwarded verbatim. They were added to
    ``compile_process_ir_v1`` and not here, which split the exported surface in
    two: a flow whose validity rests on a trusted contract compiled through the
    direct API and was rejected by this wrapper, for no reason a caller could
    see. Two entry points, one behaviour.

    #136's parse diagnostics are translated into compiler diagnostics with
    ``phase="schema"`` and their ``code``/``path``/``message``/``remediation``
    preserved VERBATIM — renaming a shipped code here would break every caller
    that already keys on it (ADR-001 §7: later introducers add codes, never
    rename them).
    """
    ir = _parse_payload_for_compile(payload)
    # The PRIVATE core, not public `compile_process_ir_v1`. The payload has just
    # been parsed by the authority, so routing through the public entry would dump
    # and re-parse the very model it produced — one wasted pass per call, on the
    # live authoring path. #178.
    cfg, plan = _compile_parsed_process_ir_v1(
        ir, symbols, validation_policy=validation_policy, capabilities=capabilities
    )
    return ir, cfg, plan


__all__: List[str] = [
    "compile_process_ir_model_v1",
    "compile_process_ir_v1",
    "parse_and_compile_process_ir_v1",
]
