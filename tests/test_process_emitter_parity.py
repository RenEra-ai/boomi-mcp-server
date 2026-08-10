"""Byte-parity tests for the ProcessIR process-emitter registry.

TWO independent corpora live here; they share no machinery on purpose.

1. DIRECT-IR corpus (#138 M12.3). Three frozen ProcessIRV1 documents
   (``fixtures/process_ir/process_ir_v1.json``) driven through ``lowering`` +
   ``emit_process``, with ``ir_to_legacy_flow_sequence`` over the UNCHANGED
   legacy builder as the oracle and a committed pre-extraction fixture per
   document as an independent byte anchor, so "registry equals the current
   builder" is never the ONLY oracle. The registry verifier result must also
   match ``verify_process_graph`` on the legacy XML.

   They exercise 17 of the registry's 18 emitter kinds (start, both connector
   roles, message, map, flowcontrol, dataprocess, doccache load/retrieve/remove,
   set-properties, processcall, branch, decision, exception, stop,
   returndocuments) — every kind EXCEPT ``catcherrors``, which #142 added and
   which ``tests/test_process_ir_error_handling.py`` covers. Do NOT restate this
   as "all kinds": the count moves whenever the registry gains a key, and the
   claim then goes stale silently (it already did once, between #138 and #142).

2. ADAPTER-DIALECT corpus (#139E M12.4). Every currently-canonical NON-listener
   ``sync_pipeline`` chain, from
   ``fixtures/process_ir/sync_pipeline_emitter_parity_cases.json``.

   Heeding the warning above, this corpus states no case COUNT anywhere in prose:
   its size is a measurement, and ``test_sync_corpus_covers_every_canonical_chain``
   recomputes it on every run — by READING the builder's accepted stage-kind
   sequences out of its own source and probing each through the real gate, never by
   modelling the grammar. Three successive models of it each failed open; see
   ``_sync_probe_chain_space`` for what each one missed and who caught it.

   The direct-IR oracle above is structurally unreachable for this dialect and
   #140/#146 did not change that: it enters through ``ir_to_legacy_flow_sequence``,
   whose frozen #136 codec requires a non-empty ``flow_sequence``, while the
   map-less sync chains have none. The only way in is the dialect's own
   normalizer, and ``SyncPipelineBuilder.lower_config`` is the SOLE normalizer
   used — there is no inverse. (Not "called exactly once per case": ``build()``
   lowers again internally, so a case that also builds lowers twice. The property
   that matters is that no OTHER normalization path exists, not the call count.)
   #139E deliberately does NOT invent a ProcessIR-to-``PipelineSpec`` inverse — no
   such direction exists anywhere in the tree, and building one would be the second
   semantic compiler ADR-001 §6 forbids.

   The oracle is ``ProcessFlowBuilder`` on that same lowered core, which is
   genuinely independent because ``SyncPipelineBuilder.build`` intercepts BEFORE
   it would delegate. The cases carrying an ``anchor`` are complemented by the
   committed raw-byte ``golden_xml`` components — the complement matters, because
   a uniform drift moves both sides of a differential together and the
   differential alone would never see it.

The WSS listener chains are in NEITHER corpus: their fused ``start_listen`` entry
has no registry key at all, so they stay on the legacy renderer and keep their own
goldens. They ARE generated as candidates by the coverage test, which asserts the
gate routes them to the legacy arm — so when #140 lands ``start_listen`` and they
become canonical, that test fails until this corpus grows to cover them.
"""

from __future__ import annotations

import ast
import copy
import inspect
import itertools
import json
import re
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
if str(_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_ROOT / "src"))

from boomi_mcp.categories.components.builders.connector_builder import (
    BuilderValidationError,
)
from boomi_mcp.categories.components.builders import (
    process_flow_builder as _process_flow_builder_module,
)
from boomi_mcp.categories.components.builders.process_flow_builder import (
    _SYNC_PIPELINE_STAGE_ALT_PRIMITIVE,
    _SYNC_PIPELINE_STAGE_PRIMITIVE,
    ProcessFlowBuilder,
    SyncPipelineBuilder,
    WrapperSubprocessBuilder,
    _sync_pipeline_is_canonical,
)
from boomi_mcp.categories.components.process_graph_verifier import verify_process_graph
from boomi_mcp.compiler.process_ir import lowering
from boomi_mcp.compiler.process_ir.contracts import ComponentSymbolV1, SymbolTableV1
from boomi_mcp.compiler.process_ir.emitter_registry import emit_process
from boomi_mcp.compiler.process_ir.legacy_adapters.emission import emit_legacy_result
from boomi_mcp.compiler.process_ir.legacy_adapters.sync_pipeline import (
    adapt_sync_pipeline,
)
from boomi_mcp.models._process_ir_compat import (
    ConnectorBindingV1,
    ConnectorResolutionContextV1,
    ir_to_legacy_flow_sequence,
)
from boomi_mcp.models.process_ir import parse_process_ir_v1

_FIXTURES = _ROOT / "tests" / "fixtures" / "process_ir"
_PARITY_DIR = _FIXTURES / "emitter_parity"
_GOLDEN_XML = _ROOT / "tests" / "fixtures" / "golden_xml"
GOLDEN_DOCS = json.loads((_FIXTURES / "process_ir_v1.json").read_text())
SYNC_CASES = json.loads(
    (_FIXTURES / "sync_pipeline_emitter_parity_cases.json").read_text()
)["cases"]
_SHARED = json.loads((_FIXTURES / "flow_sequence_compat_cases.json").read_text())["shared"]
_BINDINGS = _SHARED["bindings"]

# A Data Process step declares its profile KIND (json/xml); the registry requires
# the matching Boomi component type on the resolved symbol.
_DP_PROFILE_COMPONENT_TYPE = {"json": "profile.json", "xml": "profile.xml"}


def _sentinel_symbols(cfg) -> SymbolTableV1:
    """A symbol per authored ref, typed only enough for lowering (which resolves
    ref -> component_id and ignores the type). Component id == the ref token."""
    refs = set()
    for node in cfg.nodes:
        s = node.semantic
        for field in ("connection_ref", "operation_ref", "map_ref", "cache_ref", "process_ref"):
            if getattr(s, field, None):
                refs.add(getattr(s, field))
        for step in getattr(s, "steps", ()):
            if getattr(step, "profile_ref", None):
                refs.add(step.profile_ref)
        for src in getattr(s, "source_values", ()):
            if getattr(src, "profile_ref", None):
                refs.add(src.profile_ref)
    symbols = []
    for ref in sorted(refs):
        b = _BINDINGS.get(ref)
        symbols.append(
            ComponentSymbolV1(
                ref=ref,
                component_id=ref,
                component_type="sentinel",
                connector_type=b["connector_type"] if b else None,
                action_type=b["action_type"] if b else None,
            )
        )
    return SymbolTableV1(symbols=tuple(symbols))


def _symbols_from_plan(plan) -> SymbolTableV1:
    """The registry-canonical symbol table, typed from the plan's RESOLVED emitter
    inputs — exactly the component types the registry requirement check validates
    (component id == the resolved id, which equals the authored ref token)."""
    id_type = {}
    for node in plan.nodes:
        e = node.emitter_input
        k = e.emitter_kind
        if k in ("connectoraction_source", "connectoraction_target"):
            id_type[e.connection_id] = "connector-settings"
            id_type[e.operation_id] = "connector-action"
        elif k == "map":
            id_type[e.map_id] = "transform.map"
        elif k in ("doccacheload", "doccacheretrieve", "doccacheremove"):
            id_type[e.document_cache_id] = "documentcache"
        elif k == "processcall":
            id_type[e.process_id] = "process"
        elif k == "dataprocess":
            for st in e.steps:
                pid = getattr(st, "profile_id", "")
                if pid:
                    kind = str(getattr(st, "profile_type", "")).strip().lower()
                    id_type[pid] = _DP_PROFILE_COMPONENT_TYPE.get(kind, "profile.json")
        elif k == "setproperties_step":
            for src in e.source_values:
                if src.value_type == "profile":
                    id_type[src.profile_id] = src.profile_type
    symbols = []
    for cid in sorted(id_type):
        b = _BINDINGS.get(cid)
        symbols.append(
            ComponentSymbolV1(
                ref=cid,
                component_id=cid,
                component_type=id_type[cid],
                connector_type=b["connector_type"] if b else None,
                action_type=b["action_type"] if b else None,
            )
        )
    return SymbolTableV1(symbols=tuple(symbols))


def _context():
    return ConnectorResolutionContextV1(
        operation_bindings={ref: ConnectorBindingV1(**b) for ref, b in _BINDINGS.items()},
        fallback_target=_SHARED["target"],
    )


def _legacy_shapes_inner(process_xml: str) -> str:
    return re.search(r"<shapes>(.*)</shapes>", process_xml, re.DOTALL).group(1)


def _build_legacy(config, name="ParityProcess"):
    builder = (
        WrapperSubprocessBuilder
        if config.get("process_kind") == "wrapper_subprocess"
        else ProcessFlowBuilder
    )
    return builder.build(config, name=name, folder_name="ParityFolder")


def _emit(doc_name):
    ir = parse_process_ir_v1(GOLDEN_DOCS[doc_name])
    cfg = lowering.lower_process_ir_to_cfg(ir)
    plan = lowering.lower_cfg_to_emission_plan(cfg, _sentinel_symbols(cfg))
    symbols = _symbols_from_plan(plan)
    return ir, emit_process(plan, symbols)


@pytest.mark.parametrize("doc_name", sorted(GOLDEN_DOCS))
def test_registry_shapes_match_legacy_builder_bytes(doc_name):
    """Registry shape XML == the legacy builder's <shapes>, byte-for-byte."""
    ir, artifact = _emit(doc_name)
    legacy_config = ir_to_legacy_flow_sequence(ir, _context())
    legacy_inner = _legacy_shapes_inner(_build_legacy(legacy_config))
    assert "".join(artifact.shape_xml_parts) == legacy_inner


@pytest.mark.parametrize("doc_name", sorted(GOLDEN_DOCS))
def test_registry_process_xml_matches_committed_fixture(doc_name):
    """Independent byte anchor: not just "registry == current builder"."""
    _ir, artifact = _emit(doc_name)
    fixture = (_PARITY_DIR / f"{doc_name}.process.xml").read_text()
    assert artifact.process_xml == fixture


@pytest.mark.parametrize("doc_name", sorted(GOLDEN_DOCS))
def test_registry_emission_is_deterministic(doc_name):
    _ir, a = _emit(doc_name)
    _ir2, b = _emit(doc_name)
    assert a.process_xml == b.process_xml
    assert a.shape_xml_parts == b.shape_xml_parts


@pytest.mark.parametrize("doc_name", sorted(GOLDEN_DOCS))
def test_registry_verifier_matches_legacy_verifier(doc_name):
    """The registry's verifier summary equals verify_process_graph on legacy XML."""
    ir, artifact = _emit(doc_name)
    legacy_config = ir_to_legacy_flow_sequence(ir, _context())
    legacy = verify_process_graph(_build_legacy(legacy_config))
    assert artifact.verifier.errors == ()
    assert [dict(code=e["code"], shape=e["shape"]) for e in legacy["errors"]] == []
    assert artifact.verifier.shapes_checked == legacy["shapes_checked"]
    assert len(artifact.verifier.warnings) == len(legacy["warnings"])


# ---------------------------------------------------------------------------
# #139E — the sync_pipeline adapter-dialect corpus
# ---------------------------------------------------------------------------
# Everything below is the SECOND corpus described in the module docstring. It
# shares no helper with the direct-IR tests above except ``_legacy_shapes_inner``
# (a `<shapes>` extractor, dialect-agnostic by construction).

#: The stage kind each source/target primitive is declared on. Only what the
#: primitive vocabulary itself does not say; the vocabulary is read from the
#: builder's own constants below, never restated here.
_SYNC_STAGE_IDS = {
    "db_read": {"connection_id": "$ref:db_conn", "operation_id": "$ref:db_op"},
    "rest_fetch": {
        "connection_id": "$ref:rest_src_conn",
        "operation_id": "$ref:rest_src_op",
    },
    "soap_fetch": {
        "connection_id": "$ref:soap_src_conn",
        "operation_id": "$ref:soap_src_op",
    },
    "rest_send": {
        "action_type": "POST",
        "connection_id": "$ref:rest_conn",
        "operation_id": "$ref:rest_op",
    },
    "soap_send": {"connection_id": "$ref:soap_conn", "operation_id": "$ref:soap_op"},
    "db_write": {"connection_id": "$ref:db_w_conn", "operation_id": "$ref:db_w_op"},
    # Present so listener chains can be BUILT as candidates and land in the
    # legacy-arm bucket by the gate's own verdict. A lowered listener binding
    # carries no connection_id at all, which is half of why the canonical chain
    # cannot represent it.
    "wss_listen": {"operation_id": "$ref:wss_op"},
    # The transform slot. Enumerated like any other kind -- an alt map primitive
    # the builder accepts therefore produces candidates and must be covered, which
    # is what keeps the transform slot from being the one unexamined position.
    "map": {"map_ref": "$ref:field_map"},
}

#: How far the behavioural cross-check sweeps. NOT a claim about the grammar's
#: maximum chain length, and NOT a completeness bound -- no bound can be one (see
#: THE HONEST LIMIT in _sync_probe_chain_space). This sweep is the only detector
#: for an acceptance path that bypasses the grammar statement entirely, so the
#: number is load-bearing and its cost is stated rather than assumed: each extra
#: length multiplies the probe count by |kind-primitive pairs| = 8. Measured --
#: 4: ~0.05s, 5: +0.34s, 6: +3.23s, against a file that runs in well under 2s.
#: 5 is affordable; 6 is not, for one more length of a limit that never closes.
_SYNC_CROSSCHECK_MAX_LENGTH = 5

#: Golden fixtures under this prefix belong to the LEGACY listener arm, not to
#: this canonical corpus. #139F commits them before it touches the routing gate.
_SYNC_LEGACY_GOLDEN_PREFIX = "sync_pipeline_listener_"

_ANCHORED_SYNC_CASES = sorted(n for n, c in SYNC_CASES.items() if c["anchor"])


def _sync_fingerprint(config):
    """``<source_primitive>[_map]_<target_primitive>`` for a raw sync config.

    Derived from the config, never from the case NAME -- so a case whose name
    disagrees with its own stages cannot smuggle a duplicate past the coverage
    test below.
    """
    return "_".join(s["config"]["primitive"] for s in config["pipeline"]["stages"])


def _sync_stage_primitives():
    """``{stage kind: [primitive, ...]}`` straight from the builder's own tables."""
    tables = (dict(_SYNC_PIPELINE_STAGE_PRIMITIVE), dict(_SYNC_PIPELINE_STAGE_ALT_PRIMITIVE))
    kinds = sorted({k for table in tables for k in table})
    return {k: [t[k] for t in tables if k in t] for k in kinds}


#: Config enrichments the probes send in addition to the bare shape, because an
#: acceptance condition keyed on a field the probes never set is invisible. That is
#: not hypothetical: the architect review built such a bypass on ``description``,
#: and live QA then built two more on ``config.label`` and ``dependencies[].ordinal``
#: -- all at chain length 2, i.e. nowhere near the length bound.
#:
#: The selection criterion is "ACCEPTED AT INPUT", not "carried through to the
#: lowered config". Carriage was the original criterion and it was simply the wrong
#: one: a bypass conditions on a value being PRESENT IN THE INPUT, not on it
#: surviving lowering. That mistake alone hid five root keys.
#:
#: This is BREADTH, not closure -- see THE HONEST LIMIT. No finite set can close the
#: class, because acceptance can be keyed on a VALUE (``label == "magic"``) and not
#: merely on a field's presence.
_SYNC_PROBE_ENRICHMENTS = (
    ("root:description", {"root": {"description": "probe description"}}),
    ("root:process_extensions", {"root": {"process_extensions": {}}}),
    (
        "root:metadata",
        {
            "root": {
                "process_type": "general",
                "name": "probe",
                "folder_name": "probe/folder",
                "component_name": "probe",
                "component_type": "process",
            }
        },
    ),
    ("stage:label", {"stage": {"label": "probe label"}}),
    ("edge:ordinal", {"edge": {"ordinal": 1}}),
    ("edge:edge_kind", {"edge": {"edge_kind": "ordering"}}),
)


def _sync_probe_shape(seq):
    """Every primitive assignment for one stage-kind sequence, through the real gate.

    Each assignment is probed under every optional-root-key variant, so acceptance
    conditioned on carried metadata cannot hide behind a single synthetic shape.
    An assignment counts as accepted if ANY variant lowers.

    Returns ``[(identity, raw_config, lowered_or_None, error_code_or_None), ...]``.
    """
    primitives = _sync_stage_primitives()
    results = []
    variants = [("bare", {})] + list(_SYNC_PROBE_ENRICHMENTS)

    def build(extra):
        """A FRESH config each time -- built, never deep-copied. This sweep runs
        into six figures of candidates, and deepcopy dominated it."""
        stages = [
            {
                "key": f"k{i}",
                "kind": k,
                "config": {"primitive": p, **_SYNC_STAGE_IDS[p], **extra.get("stage", {})},
            }
            for i, (k, p) in enumerate(zip(seq, choice))
        ]
        keys = [s["key"] for s in stages]
        return {
            "process_kind": "sync_pipeline",
            "pipeline": {
                "stages": stages,
                "dependencies": [
                    {"from_stage": a, "to_stage": b, **extra.get("edge", {})}
                    for a, b in zip(keys, keys[1:])
                ],
            },
            **copy.deepcopy(extra.get("root", {})),
        }

    for choice in itertools.product(*[primitives[k] for k in seq]):
        identity = tuple(zip(seq, choice))
        accepted, last_code = None, None
        for _label, extra in variants:
            candidate = build(extra)
            try:
                lowered = SyncPipelineBuilder.lower_config(build(extra))
            except BuilderValidationError as exc:
                last_code = exc.error_code
                continue
            # Anything OTHER than BuilderValidationError propagates on purpose: a
            # crash in the normalizer is a finding, not a rejection. Measured clean
            # across the whole cross-checked space.
            accepted = (candidate, lowered)
            break
        if accepted is None:
            results.append((identity, build({}), None, last_code))
        else:
            results.append((identity, accepted[0], accepted[1], None))
    return results


def _sync_accepted_stage_sequences():
    """Read the builder's accepted stage-kind sequences from its OWN source.

    ``lower_config`` states the grammar as a literal tuple of kind lists compared
    against ``kinds`` (``process_flow_builder.py``, the ``if kinds not in (...)``
    guard). There is no module constant to import, so this extracts that literal by
    AST rather than restating it -- because every attempt to restate it has failed
    open (see ``_sync_probe_chain_space``).

    Extraction is fail-closed: it demands exactly one matching comparison. A
    refactor that moves or computes the grammar breaks this LOUDLY, which is
    correct -- the corpus's completeness claim genuinely depends on knowing the
    accepted sequences, so it must not be able to go on quietly guessing them.
    """
    tree = ast.parse(inspect.getsource(_process_flow_builder_module))

    # Locate the GUARD STATEMENT, not a matching expression. Searching for "some
    # comparison that looks like the grammar" recognises a shape and skips past
    # anything else, so EXTENDING the guard is invisible while the match count
    # stays 1 -- fail-open. Two mutants proved it: hoisting the tuple into a module
    # constant (`... and kinds not in _CONST`), and `... and len(kinds) != 5`,
    # which adds no membership test at all. Anchoring on the statement means an
    # unrecognised form is a KILL rather than a miss.
    guards = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.If)
        and any(isinstance(x, ast.Name) and x.id == "kinds" for x in ast.walk(node.test))
        and any(isinstance(x, ast.Raise) for x in node.body)
    ]
    assert len(guards) == 1, (
        f"expected exactly ONE `if <kinds ...>: raise` stage-sequence guard in "
        f"process_flow_builder, found {len(guards)}. The grammar moved or split; "
        f"update _sync_accepted_stage_sequences to read it where it now lives."
    )
    test = guards[0].test
    assert isinstance(test, ast.Compare) and len(test.ops) == 1 and isinstance(test.ops[0], ast.NotIn), (
        f"the stage-sequence guard's test is {type(test).__name__}, not a bare "
        f"`kinds not in (...)` comparison. Acceptance now depends on a condition "
        f"this extraction cannot read, so it can no longer claim to know the "
        f"accepted sequences -- read the grammar where it now lives, or (better) "
        f"hoist it to a module constant and import it."
    )
    # The LEFT operand must be `kinds` itself, not merely something that mentions
    # it. `sorted(kinds) not in (...)`, `kinds[1:] not in (...)` and friends all
    # satisfy the statement locator and the bare-Compare check while comparing a
    # DIFFERENT value against the literal -- at which point the sequences read here
    # are not the sequences being matched.
    assert isinstance(test.left, ast.Name) and test.left.id == "kinds", (
        f"the stage-sequence guard compares `{ast.dump(test.left)[:60]}`, not `kinds` "
        f"itself, so the literal read here is not what acceptance is matched against."
    )
    target = test.comparators[0]
    assert (
        isinstance(target, (ast.Tuple, ast.List))
        and target.elts
        and all(
            isinstance(e, ast.List)
            and e.elts
            and all(isinstance(x, ast.Constant) and isinstance(x.value, str) for x in e.elts)
            for e in target.elts
        )
    ), (
        "the stage-sequence guard compares against something other than a literal "
        "tuple of string lists; this extraction can no longer read the grammar."
    )
    return tuple(tuple(x.value for x in e.elts) for e in target.elts)


def _sync_probe_chain_space():
    """Partition every chain the builder ACCEPTS, using the builder's own grammar.

    Reads the grammar rather than modelling it -- but see the honest limit at the
    end: this is a fail-CLOSED bridge, not a proof. Four fail-open holes were closed
    here, and each time the model, not its parameters, was the defect:

    1. The first version reconstructed the grammar as ``source x target x +/-map``,
       classifying kinds via the adapter's ``_SOURCE_STAGE_KINDS`` /
       ``_TARGET_STAGE_KINDS``. A kind in BOTH sets was silently filed source-only
       by the if/elif, so giving ``fetch`` a target role and permitting
       ``read -> fetch`` made ``db_read_rest_fetch`` reachable, canonical and
       uncovered with every guard green. (Codex impl-vs-plan review.)
    2. Removing the ROLE model left an ARITY model -- lengths fixed to {2, 3} with
       the transform kind in the middle. Permitting a 4-stage
       ``fetch -> map -> map -> send`` left four canonical chains uncovered, and
       those are byte-DISTINCT (both map stages are dropped, emitting zero map
       shapes) with parity still holding, because canonical and legacy drop them
       identically -- the uniform drift a differential cannot see. (Live QA.)
    3. Replacing the fixed lengths with a sweep that walked upward until a length
       was dry was still a model: dryness is LOCAL. A grammar accepting 5-stage
       chains while 4 stays dry stops the sweep at 4 and is missed entirely --
       verified reachable and canonical by mutation.
    4. Reading the grammar by AST was, at first, a SYNTAX model: it searched for a
       comparison that looked right and skipped anything else, so EXTENDING the
       guard stayed invisible while the match count held at 1. Hoisting the tuple
       into a module constant (``... and kinds not in _CONST``) or adding
       ``... and len(kinds) != 5`` both evaded it. (Live QA.) It now anchors on the
       guard STATEMENT and refuses any test it cannot read, so an unrecognised form
       kills instead of slipping through.

    No bounded sampling of the chain space is complete against a grammar that can
    change shape, so this stops sampling and READS the grammar
    (``_sync_accepted_stage_sequences``), enumerating primitive assignments over
    exactly the accepted sequences and letting the routing gate split them. A
    cheap bounded cross-check then guards the extraction itself.

    THE HONEST LIMIT -- stated as a principle, because every attempt to state it as
    a LIST of residual classes has itself been incomplete. Two successive drafts
    enumerated "the" residual classes and both were refuted within one review round;
    enumerating them is the same mistake as modelling the grammar, one level up.

    What is PROVEN: for the chains this harness reaches, the corpus covers exactly
    those the routing gate calls canonical. What is NOT proven, and cannot be by
    anything here: that the harness reaches every chain the builder accepts. It
    learns the accepted set from a statement in ``lower_config``, and it can only
    detect acceptance happening elsewhere by trying inputs and observing -- so any
    acceptance path the trials do not happen to exercise is invisible.

    The trials are bounded in chain LENGTH by a named constant
    (``_SYNC_CROSSCHECK_MAX_LENGTH``), and merely SAMPLED in config shape
    (``_SYNC_PROBE_ENRICHMENTS``) -- not bounded, sampled, and the difference is the
    whole point. No finite variant set can close that dimension, because acceptance
    can be keyed on a VALUE (``label == "magic"``) rather than on a field's presence,
    so a bypass always exists and is constructible in minutes. Three successive
    versions of this sampling were broken in review -- chain length, then root-key
    shape, then stage/edge shape -- and a fourth would be no harder. Adding
    dimensions moves the bypass; it never removes it. Do not read the enrichment
    list as a frontier.

    Every tightening here therefore buys fail-CLOSED behaviour on a named form, not
    completeness. Two remedies are recorded in the migration ledger rather than done
    here, and neither is a proof on its own:

    * Hoisting the accepted-sequence tuple to a module-level constant in
      ``process_flow_builder`` and importing it removes the EXTRACTION half -- nothing
      left to misread -- but does not touch the class above, because under a bypass
      the extraction already returns the correct, byte-identical sequences.
    * What would actually end the sequence is a different ORACLE, not more sampling:
      run this probe corpus under a tracer and assert every branch on
      ``lower_config``'s accept/reject path was exercised, so a bypass branch the
      probes never trigger surfaces as an uncovered branch rather than as silence.
      (Precedented in this repo by #144's AST+tracer proof.) That is materially
      bigger than a test-only refactor and plausibly belongs to whichever slice owns
      the lowering path.

    **Re-run this attack when an acceptance path near the routing gate changes** --
    #139F and #140 both plausibly add one when ``start_listen`` is promoted -- because
    that is when these limits stop being documented and become load-bearing.

    Listener chains are probed like everything else rather than skipped by name: the
    day #140 promotes ``start_listen`` they move from the legacy bucket to the
    canonical one and the coverage assertion fires.

    Returns ``(canonical, legacy_arm, rejected, grammar)``. Identity is the full
    ``((kind, primitive), ...)`` sequence -- NOT the primitive-only fingerprint,
    which cannot tell two kinds apart and would collide precisely in case 1.
    """
    primitives = _sync_stage_primitives()
    kinds = sorted(primitives)

    # A primitive the tables admit but this file has no ids for would otherwise
    # blow up with a bare KeyError deep in the loop. Say what to do instead: a NEW
    # primitive should arrive here as work-to-do, not as a puzzle.
    missing = sorted(p for ps in primitives.values() for p in ps if p not in _SYNC_STAGE_IDS)
    assert not missing, (
        f"sync_pipeline primitive(s) {missing} are admitted by the builder's stage "
        f"tables but have no id vocabulary in _SYNC_STAGE_IDS. Add ids here, and add "
        f"a corpus case for every chain they make canonical."
    )

    grammar = _sync_accepted_stage_sequences()
    canonical, legacy_arm, rejected = {}, {}, {}
    for seq in grammar:
        for identity, raw, lowered, code in _sync_probe_shape(seq):
            if lowered is None:
                rejected[identity] = code
                continue
            bucket = canonical if _sync_pipeline_is_canonical(lowered) else legacy_arm
            bucket[identity] = raw

    # The BEHAVIOURAL cross-check, and the only thing standing between this test and
    # the two residual classes in the docstring. Reading the grammar is sound only
    # while the grammar statement is the sole acceptance path; if the AST picked up
    # a stale list, or if some path accepts chains without consulting that statement
    # at all, those chains are accepted and otherwise invisible. So sweep every short
    # kind sequence and assert nothing accepted lives outside the extracted grammar.
    #
    # Bounded, and the bound is a real limit rather than a formality -- a bypass is
    # caught inside it and missed outside it. See _SYNC_CROSSCHECK_MAX_LENGTH for the
    # measured cost of each extra length, and the docstring for why no bound closes
    # the class.
    outside = sorted(
        seq
        for length in range(2, _SYNC_CROSSCHECK_MAX_LENGTH + 1)
        for seq in itertools.product(kinds, repeat=length)
        if seq not in grammar
        and any(lowered is not None for _i, _r, lowered, _c in _sync_probe_shape(seq))
    )
    assert not outside, (
        f"stage sequence(s) {outside} are accepted by the builder but absent from the "
        f"accepted-sequence literal this test extracts. The extraction is stale or is "
        f"reading the wrong list -- fix _sync_accepted_stage_sequences."
    )
    return canonical, legacy_arm, rejected, grammar


def _sync_case(case_name):
    """Resolve one case. ``lower_config`` is the SOLE normalizer for this dialect
    and is called exactly once per case -- there is no inverse direction, and
    #139E does not invent one."""
    case = SYNC_CASES[case_name]
    raw = copy.deepcopy(case["config"])
    lowered = SyncPipelineBuilder.lower_config(copy.deepcopy(case["config"]))
    return case, raw, lowered


def _emit_sync(lowered):
    return emit_legacy_result(
        adapt_sync_pipeline(copy.deepcopy(lowered)), dialect="sync_pipeline"
    )


def test_sync_corpus_covers_every_canonical_chain():
    """Fail-closed on the SUPERSET direction, by derivation rather than by list.

    #139C's inline chain set was hand-written at the STAGE-KIND level (6 chains),
    which silently omitted the map-ful SOAP chain and every mixed-connector-family
    combination -- exactly the surface the #139A cross-role connector guard and
    #139C's family-conditional canonicalizer defect live on. Freezing that list
    verbatim, as this corpus first did, would have inherited the gap and added
    prose claiming it was complete.

    So the universe is recomputed here from the builder's own primitive tables and
    each candidate is put through the REAL gate; the corpus must cover exactly the
    ones that come out canonical. Add a primitive to
    ``_SYNC_PIPELINE_STAGE_ALT_PRIMITIVE`` and this fails until the corpus grows --
    which a hand-maintained literal could never do.
    """
    canonical, legacy_arm, rejected, grammar = _sync_probe_chain_space()

    # The grammar READ from the builder, pinned by code. Coverage is derived from
    # it, so a sequence added or removed there must be a conscious edit here too --
    # and this is the assertion that says so, in the reviewer's own terms.
    assert grammar == (
        ("read", "send"),
        ("read", "map", "send"),
        ("fetch", "send"),
        ("fetch", "map", "send"),
        ("fetch", "write"),
        ("fetch", "map", "write"),
        ("listener", "send"),
        ("listener", "map", "send"),
        ("listener", "write"),
        ("listener", "map", "write"),
    )

    # Identity carries the stage KIND; the corpus is keyed on the primitive-only
    # fingerprint. That projection is only a valid identity while it stays
    # injective -- a dual-role kind is exactly what would collapse two distinct
    # chains onto one name and hide the second. Asserted on BOTH partitions, since
    # the legacy arm is compared as a fingerprint set below.
    for label, bucket in (("canonical", canonical), ("legacy_arm", legacy_arm)):
        fps = [_sync_fingerprint(raw) for raw in bucket.values()]
        assert len(set(fps)) == len(fps), (
            f"two {label} chains share a primitive-only fingerprint; the corpus key "
            f"can no longer identify a chain. Enrich _sync_fingerprint (and the case "
            f"names) to carry the stage kind."
        )
    fingerprints = [_sync_fingerprint(raw) for raw in canonical.values()]

    # The legacy arm is exactly the WSS listener chains (#140 owns the fused
    # start_listen entry). When that lands, these move into `canonical` and the
    # coverage assertion below fails until the corpus grows -- which is the whole
    # reason listener candidates are probed rather than skipped by name.
    assert {_sync_fingerprint(raw) for raw in legacy_arm.values()} == {
        "wss_listen_rest_send",
        "wss_listen_map_rest_send",
        "wss_listen_soap_send",
        "wss_listen_map_soap_send",
        "wss_listen_db_write",
        "wss_listen_map_db_write",
    }
    # The DB-to-DB chain is the one endpoint-shaped pairing the builder refuses on
    # purpose (no archetype). It is not in the grammar, so it is never probed above
    # -- assert the refusal directly, so "excluded by design" stays a tested claim
    # rather than an absence anyone could read as an oversight.
    for shape in (("read", "write"), ("read", "map", "write")):
        assert shape not in grammar
        assert all(
            code == "SYNC_PIPELINE_CONTROL_FLOW_UNSUPPORTED"
            for _i, _r, _low, code in _sync_probe_shape(shape)
        )

    # Within the accepted grammar, every primitive assignment must lower. A
    # rejection here means a sequence the builder advertises cannot actually be
    # spelled -- worth knowing, and not silently tolerated.
    assert rejected == {}

    # KEEP this literal. It looks like the redundant total that was removed from
    # test_sync_corpus_has_one_case_per_fingerprint..., but it is a different shape:
    # that one was IMPLIED by its neighbours, whereas this is an independent floor
    # against UNIFORM drift. If the corpus and the derivation grew together,
    # `corpus == set(canonical)` below would still hold and only this literal would
    # fire -- the same reason the golden anchors sit beside the differential oracle.
    assert len(canonical) == 16

    corpus = {_sync_fingerprint(c["config"]) for c in SYNC_CASES.values()}
    assert corpus == set(fingerprints)


def test_sync_corpus_has_one_case_per_fingerprint_plus_the_verb_duplicate():
    """The deletion direction, and the only intentional duplicate.

    ``soap_lowercase_execute`` deliberately repeats the ``soap_fetch_soap_send``
    fingerprint with the non-uppercase verb the pre-#139C canonicalizer corrupted.
    Any OTHER duplicate is a case that silently tests nothing new.

    No total is asserted here on purpose: the coverage test pins the fingerprint
    set and this one pins the duplicates, which together determine the count
    exactly. A third assertion spelling it out would be a hard-coded number that
    constrains nothing and goes stale the day the corpus legitimately grows.
    """
    seen = {}
    for name, case in SYNC_CASES.items():
        seen.setdefault(_sync_fingerprint(case["config"]), []).append(name)
    dupes = {f: sorted(n) for f, n in seen.items() if len(n) > 1}
    assert dupes == {
        "soap_fetch_soap_send": ["soap_fetch_soap_send", "soap_lowercase_execute"]
    }


def test_sync_corpus_anchors_every_committed_sync_golden():
    """Fail-closed on the ADDITION direction: a new CANONICAL ``sync_pipeline_*``
    golden cannot arrive without a parity case claiming it, and a case cannot claim
    an anchor that does not exist.

    The inventory is split by prefix rather than globbing everything, because the
    two arms are governed differently. #139F must commit legacy-rendered
    ``sync_pipeline_listener_*.xml`` fixtures BEFORE it touches the routing gate --
    that is the plan's own pre-cutover discipline, and how #139C avoided a
    self-confirming anchor. A single glob would deadlock that step: the new fixture
    fails this equality, and adding it as a corpus case instead fails
    ``test_sync_case_routes_to_the_canonical_chain``, since a listener chain is by
    definition not canonical yet. Raised by the Codex impl-vs-plan review.

    The listener arm is asserted EMPTY rather than ignored, so those fixtures
    landing is a deliberate edit here (moving the name into the legacy inventory)
    rather than a silently widened glob.
    """
    committed = {p.name for p in _GOLDEN_XML.glob("sync_pipeline_*.xml")}
    legacy = {n for n in committed if n.startswith(_SYNC_LEGACY_GOLDEN_PREFIX)}
    canonical = committed - legacy

    claimed = {c["anchor"] for c in SYNC_CASES.values() if c["anchor"]}
    assert claimed == canonical
    for name in claimed:
        assert (_GOLDEN_XML / name).is_file()

    # Empty until #139F. When it is not, the listener chains are still on the
    # legacy arm and belong in a legacy inventory -- never in this canonical corpus.
    assert legacy == set()


@pytest.mark.parametrize("case_name", sorted(SYNC_CASES))
def test_sync_case_routes_to_the_canonical_chain(case_name):
    """Pins that every case in the corpus really is canonical (the MEMBER
    direction; ``test_sync_corpus_covers_every_canonical_chain`` owns the superset
    direction). A listener case added here would take the legacy arm, and every
    parity assertion below would then be comparing the legacy renderer with
    itself."""
    _case, _raw, lowered = _sync_case(case_name)
    assert _sync_pipeline_is_canonical(lowered) is True


@pytest.mark.parametrize("case_name", sorted(SYNC_CASES))
def test_sync_canonical_shape_bytes_match_the_legacy_renderer(case_name):
    """Registry shape XML == the legacy renderer's <shapes>, byte-for-byte."""
    case, _raw, lowered = _sync_case(case_name)
    artifact = _emit_sync(lowered)
    legacy = ProcessFlowBuilder.build(
        copy.deepcopy(lowered), name=case["name"], folder_name=case["folder_name"]
    )
    assert "".join(artifact.shape_xml_parts) == _legacy_shapes_inner(legacy)


@pytest.mark.parametrize("case_name", sorted(SYNC_CASES))
def test_sync_canonical_component_matches_the_legacy_component(case_name):
    """The full component envelope, not just the shapes: the differential across
    the whole emitted document."""
    case, raw, lowered = _sync_case(case_name)
    emitted = SyncPipelineBuilder.build(
        raw, name=case["name"], folder_name=case["folder_name"]
    )
    assert emitted == ProcessFlowBuilder.build(
        copy.deepcopy(lowered), name=case["name"], folder_name=case["folder_name"]
    )


@pytest.mark.parametrize("case_name", sorted(SYNC_CASES))
def test_sync_verifier_results_are_clean_and_equivalent(case_name):
    case, _raw, lowered = _sync_case(case_name)
    artifact = _emit_sync(lowered)
    legacy = verify_process_graph(
        ProcessFlowBuilder.build(
            copy.deepcopy(lowered), name=case["name"], folder_name=case["folder_name"]
        )
    )
    assert artifact.verifier.errors == ()
    assert [dict(code=e["code"], shape=e["shape"]) for e in legacy["errors"]] == []
    assert artifact.verifier.shapes_checked == legacy["shapes_checked"]
    assert len(artifact.verifier.warnings) == len(legacy["warnings"])


@pytest.mark.parametrize("case_name", sorted(SYNC_CASES))
def test_sync_canonical_emission_is_deterministic(case_name):
    """Emitting twice from the SAME lowered core isolates emission determinism
    from lowering determinism (the latter is pinned separately, by
    ``test_sync_pipeline_builder.py::test_all_stage_and_dependency_permutations_lower_identically``)."""
    _case, _raw, lowered = _sync_case(case_name)
    a = _emit_sync(lowered)
    b = _emit_sync(lowered)
    assert a.process_xml == b.process_xml
    assert a.shape_xml_parts == b.shape_xml_parts


@pytest.mark.parametrize("case_name", _ANCHORED_SYNC_CASES)
def test_sync_anchored_case_matches_the_committed_component_bytes(case_name):
    """The independent byte anchor, asserted on BOTH renderers.

    The canonical half duplicates test_sync_pipeline_builder.py on purpose (it is
    what makes this corpus self-contained). The LEGACY half is the non-redundant
    one: nothing else in the tree pins ``ProcessFlowBuilder``'s own output against
    these bytes, which is exactly the uniform-drift hole the differential oracle
    is blind to.
    """
    case, raw, lowered = _sync_case(case_name)
    golden = (_GOLDEN_XML / case["anchor"]).read_text()
    assert (
        SyncPipelineBuilder.build(
            raw, name=case["name"], folder_name=case["folder_name"]
        )
        == golden
    )
    assert (
        ProcessFlowBuilder.build(
            copy.deepcopy(lowered), name=case["name"], folder_name=case["folder_name"]
        )
        == golden
    )
